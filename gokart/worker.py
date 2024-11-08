# -*- coding: utf-8 -*-
#
# Copyright 2012-2015 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
The worker communicates with the scheduler and does two things:

1. Sends all tasks that has to be run
2. Gets tasks from the scheduler that should be run

When running in local mode, the worker talks directly to a :py:class:`~luigi.scheduler.Scheduler` instance.
When you run a central server, the worker will talk to the scheduler using a :py:class:`~luigi.rpc.RemoteScheduler` instance.

Everything in this module is private to luigi and may change in incompatible
ways between versions. The exception is the exception types and the
:py:class:`worker` config class.
"""

import collections
import collections.abc
import contextlib
import datetime
import functools
import getpass
import importlib
import json
import logging
import multiprocessing
import os
import queue as Queue
import random
import signal
import socket
import subprocess
import sys
import threading
import time
import traceback
from typing import Any, Dict, Generator, List, Literal, Optional, Set, Tuple

import luigi
import luigi.scheduler
import luigi.worker
from luigi import notifications
from luigi.event import Event
from luigi.scheduler import DISABLED, DONE, FAILED, PENDING, UNKNOWN, WORKER_STATE_ACTIVE, WORKER_STATE_DISABLED, RetryPolicy, Scheduler
from luigi.target import Target
from luigi.task import DynamicRequirements, Task, flatten
from luigi.task_register import TaskClassException, load_task
from luigi.task_status import RUNNING

from gokart.parameter import ExplicitBoolParameter

logger = logging.getLogger(__name__)

# Prevent fork() from being called during a C-level getaddrinfo() which uses a process-global mutex,
# that may not be unlocked in child process, resulting in the process being locked indefinitely.
fork_lock = threading.Lock()

# Why we assert on _WAIT_INTERVAL_EPS:
# multiprocessing.Queue.get() is undefined for timeout=0 it seems:
# https://docs.python.org/3.4/library/multiprocessing.html#multiprocessing.Queue.get.
# I also tried with really low epsilon, but then ran into the same issue where
# the test case "test_external_dependency_worker_is_patient" got stuck. So I
# unscientifically just set the final value to a floating point number that
# "worked for me".
_WAIT_INTERVAL_EPS = 0.00001


def _is_external(task: Task) -> bool:
    return task.run is None or task.run == NotImplemented


def _get_retry_policy_dict(task: Task) -> Dict[str, Any]:
    return RetryPolicy(task.retry_count, task.disable_hard_timeout, task.disable_window)._asdict()  # type: ignore


GetWorkResponse = collections.namedtuple(
    'GetWorkResponse',
    (
        'task_id',
        'running_tasks',
        'n_pending_tasks',
        'n_unique_pending',
        'n_pending_last_scheduled',
        'worker_state',
    ),
)


class TaskProcess(multiprocessing.Process):
    """Wrap all task execution in this class.

    Mainly for convenience since this is run in a separate process."""

    # mapping of status_reporter attributes to task attributes that are added to tasks
    # before they actually run, and removed afterwards
    forward_reporter_attributes = {
        'update_tracking_url': 'set_tracking_url',
        'update_status_message': 'set_status_message',
        'update_progress_percentage': 'set_progress_percentage',
        'decrease_running_resources': 'decrease_running_resources',
        'scheduler_messages': 'scheduler_messages',
    }

    def __init__(
        self,
        task: luigi.Task,
        worker_id: str,
        result_queue: multiprocessing.Queue,
        status_reporter: luigi.worker.TaskStatusReporter,
        use_multiprocessing: bool = False,
        worker_timeout: int = 0,
        check_unfulfilled_deps: bool = True,
        check_complete_on_run: bool = False,
        task_completion_cache: Optional[Dict[str, Any]] = None,
        skip_if_completed_pre_run: bool = True,
    ) -> None:
        super(TaskProcess, self).__init__()
        self.task = task
        self.worker_id = worker_id
        self.result_queue = result_queue
        self.status_reporter = status_reporter
        self.worker_timeout = task.worker_timeout if task.worker_timeout is not None else worker_timeout
        self.timeout_time = time.time() + self.worker_timeout if self.worker_timeout else None
        self.use_multiprocessing = use_multiprocessing or self.timeout_time is not None
        self.check_unfulfilled_deps = check_unfulfilled_deps
        self.check_complete_on_run = check_complete_on_run
        self.task_completion_cache = task_completion_cache
        self.skip_if_completed_pre_run = skip_if_completed_pre_run

        # completeness check using the cache
        self.check_complete = functools.partial(luigi.worker.check_complete_cached, completion_cache=task_completion_cache)

    def _run_task(self) -> Optional[collections.abc.Generator]:
        if self.skip_if_completed_pre_run and self.check_complete(self.task):
            logger.warning(f'{self.task} is skipped because the task is already completed.')
            return None
        return self.task.run()

    def _run_get_new_deps(self) -> Optional[List[Tuple[str, str, Dict[str, str]]]]:
        task_gen = self._run_task()

        if not isinstance(task_gen, collections.abc.Generator):
            return None

        next_send = None
        while True:
            try:
                if next_send is None:
                    requires = next(task_gen)
                else:
                    requires = task_gen.send(next_send)
            except StopIteration:
                return None

            # if requires is not a DynamicRequirements, create one to use its default behavior
            if not isinstance(requires, DynamicRequirements):
                requires = DynamicRequirements(requires)

            if not requires.complete(self.check_complete):
                # not all requirements are complete, return them which adds them to the tree
                new_deps = [(t.task_module, t.task_family, t.to_str_params()) for t in requires.flat_requirements]
                return new_deps

            # get the next generator result
            next_send = requires.paths

    def run(self) -> None:
        logger.info('[pid %s] Worker %s running   %s', os.getpid(), self.worker_id, self.task)

        if self.use_multiprocessing:
            # Need to have different random seeds if running in separate processes
            processID = os.getpid()
            currentTime = time.time()
            random.seed(processID * currentTime)

        status: Optional[str] = FAILED
        expl = ''
        missing: List[str] = []
        new_deps: Optional[List[Tuple[str, str, Dict[str, str]]]] = []
        try:
            # Verify that all the tasks are fulfilled! For external tasks we
            # don't care about unfulfilled dependencies, because we are just
            # checking completeness of self.task so outputs of dependencies are
            # irrelevant.
            if self.check_unfulfilled_deps and not _is_external(self.task):
                missing = []
                for dep in self.task.deps():
                    if not self.check_complete(dep):
                        nonexistent_outputs = [output for output in flatten(dep.output()) if not output.exists()]
                        if nonexistent_outputs:
                            missing.append(f'{dep.task_id} ({", ".join(map(str, nonexistent_outputs))})')
                        else:
                            missing.append(dep.task_id)
                if missing:
                    deps = 'dependency' if len(missing) == 1 else 'dependencies'
                    raise RuntimeError('Unfulfilled %s at run time: %s' % (deps, ', '.join(missing)))
            self.task.trigger_event(Event.START, self.task)
            t0 = time.time()
            status = None

            if _is_external(self.task):
                # External task
                if self.check_complete(self.task):
                    status = DONE
                else:
                    status = FAILED
                    expl = 'Task is an external data dependency ' 'and data does not exist (yet?).'
            else:
                with self._forward_attributes():
                    new_deps = self._run_get_new_deps()
                if not new_deps:
                    if not self.check_complete_on_run:
                        # update the cache
                        if self.task_completion_cache is not None:
                            self.task_completion_cache[self.task.task_id] = True
                        status = DONE
                    elif self.check_complete(self.task):
                        status = DONE
                    else:
                        raise luigi.worker.TaskException('Task finished running, but complete() is still returning false.')
                else:
                    status = PENDING

            if new_deps:
                logger.info('[pid %s] Worker %s new requirements      %s', os.getpid(), self.worker_id, self.task)
            elif status == DONE:
                self.task.trigger_event(Event.PROCESSING_TIME, self.task, time.time() - t0)
                expl = self.task.on_success()
                logger.info('[pid %s] Worker %s done      %s', os.getpid(), self.worker_id, self.task)
                self.task.trigger_event(Event.SUCCESS, self.task)

        except KeyboardInterrupt:
            raise
        except BaseException as ex:
            status = FAILED
            expl = self._handle_run_exception(ex)

        finally:
            self.result_queue.put((self.task.task_id, status, expl, missing, new_deps))

    def _handle_run_exception(self, ex: BaseException) -> str:
        logger.exception('[pid %s] Worker %s failed    %s', os.getpid(), self.worker_id, self.task)
        self.task.trigger_event(Event.FAILURE, self.task, ex)
        return self.task.on_failure(ex)

    def _recursive_terminate(self) -> None:
        import psutil

        try:
            parent = psutil.Process(self.pid)
            children = parent.children(recursive=True)

            # terminate parent. Give it a chance to clean up
            super(TaskProcess, self).terminate()
            parent.wait()

            # terminate children
            for child in children:
                try:
                    child.terminate()
                except psutil.NoSuchProcess:
                    continue
        except psutil.NoSuchProcess:
            return

    def terminate(self) -> None:
        """Terminate this process and its subprocesses."""
        # default terminate() doesn't cleanup child processes, it orphans them.
        try:
            return self._recursive_terminate()
        except ImportError:
            return super(TaskProcess, self).terminate()

    @contextlib.contextmanager
    def _forward_attributes(self):
        # forward configured attributes to the task
        for reporter_attr, task_attr in self.forward_reporter_attributes.items():
            setattr(self.task, task_attr, getattr(self.status_reporter, reporter_attr))
        try:
            yield self
        finally:
            # reset attributes again
            for _, task_attr in self.forward_reporter_attributes.items():
                setattr(self.task, task_attr, None)


# This code and the task_process_context config key currently feels a bit ad-hoc.
# Discussion on generalizing it into a plugin system: https://github.com/spotify/luigi/issues/1897
class ContextManagedTaskProcess(TaskProcess):
    def __init__(self, context, *args, **kwargs) -> None:
        super(ContextManagedTaskProcess, self).__init__(*args, **kwargs)
        self.context = context

    def run(self) -> None:
        if self.context:
            logger.debug('Importing module and instantiating ' + self.context)
            module_path, class_name = self.context.rsplit('.', 1)
            module = importlib.import_module(module_path)
            cls = getattr(module, class_name)

            with cls(self):
                super(ContextManagedTaskProcess, self).run()
        else:
            super(ContextManagedTaskProcess, self).run()


class gokart_worker(luigi.Config):
    """Configuration for the gokart worker.

    You can set these options of section [gokart_worker] in your luigi.cfg file.

    NOTE: use snake_case for this class to match the luigi.Config convention.
    """

    id = luigi.Parameter(default='', description='Override the auto-generated worker_id')
    ping_interval = luigi.FloatParameter(default=1.0, config_path=dict(section='core', name='worker-ping-interval'))
    keep_alive = luigi.BoolParameter(default=False, config_path=dict(section='core', name='worker-keep-alive'))
    count_uniques = luigi.BoolParameter(
        default=False,
        config_path=dict(section='core', name='worker-count-uniques'),
        description='worker-count-uniques means that we will keep a ' 'worker alive only if it has a unique pending task, as ' 'well as having keep-alive true',
    )
    count_last_scheduled = luigi.BoolParameter(
        default=False, description='Keep a worker alive only if there are ' 'pending tasks which it was the last to ' 'schedule.'
    )
    wait_interval = luigi.FloatParameter(default=1.0, config_path=dict(section='core', name='worker-wait-interval'))
    wait_jitter = luigi.FloatParameter(default=5.0)

    max_keep_alive_idle_duration = luigi.TimeDeltaParameter(default=datetime.timedelta(0))

    max_reschedules = luigi.IntParameter(default=1, config_path=dict(section='core', name='worker-max-reschedules'))
    timeout = luigi.IntParameter(default=0, config_path=dict(section='core', name='worker-timeout'))
    task_limit = luigi.IntParameter(default=None, config_path=dict(section='core', name='worker-task-limit'))
    retry_external_tasks = luigi.BoolParameter(
        default=False,
        config_path=dict(section='core', name='retry-external-tasks'),
        description='If true, incomplete external tasks will be ' 'retested for completion while Luigi is running.',
    )
    send_failure_email = luigi.BoolParameter(default=True, description='If true, send e-mails directly from the worker' 'on failure')
    no_install_shutdown_handler = luigi.BoolParameter(default=False, description='If true, the SIGUSR1 shutdown handler will' 'NOT be install on the worker')
    check_unfulfilled_deps = luigi.BoolParameter(default=True, description='If true, check for completeness of ' 'dependencies before running a task')
    check_complete_on_run = luigi.BoolParameter(
        default=False,
        description='If true, only mark tasks as done after running if they are complete. '
        'Regardless of this setting, the worker will always check if external '
        'tasks are complete before marking them as done.',
    )
    force_multiprocessing = luigi.BoolParameter(default=False, description='If true, use multiprocessing also when ' 'running with 1 worker')
    task_process_context = luigi.OptionalParameter(
        default=None,
        description='If set to a fully qualified class name, the class will '
        'be instantiated with a TaskProcess as its constructor parameter and '
        'applied as a context manager around its run() call, so this can be '
        'used for obtaining high level customizable monitoring or logging of '
        'each individual Task run.',
    )
    cache_task_completion = luigi.BoolParameter(
        default=False,
        description='If true, cache the response of successful completion checks '
        'of tasks assigned to a worker. This can especially speed up tasks with '
        'dynamic dependencies but assumes that the completion status does not change '
        'after it was true the first time.',
    )
    skip_if_completed_pre_run: bool = ExplicitBoolParameter(
        default=True, description='If true, skip running tasks that are already completed just before the Task is run.'
    )


class Worker:
    """
    Worker object communicates with a scheduler.

    Simple class that talks to a scheduler and:

    * tells the scheduler what it has to do + its dependencies
    * asks for stuff to do (pulls it in a loop and runs it)
    """

    def __init__(
        self,
        scheduler: Optional[Scheduler] = None,
        worker_id: Optional[str] = None,
        worker_processes: int = 1,
        assistant: bool = False,
        config: Optional[gokart_worker] = None,
    ) -> None:
        if scheduler is None:
            scheduler = Scheduler()

        self.worker_processes = int(worker_processes)
        self._worker_info = self._generate_worker_info()
        if config is None:
            self._config = gokart_worker()
        else:
            self._config = config

        worker_id = worker_id or self._config.id or self._generate_worker_id(self._worker_info)

        assert self._config.wait_interval >= _WAIT_INTERVAL_EPS, '[worker] wait_interval must be positive'
        assert self._config.wait_jitter >= 0.0, '[worker] wait_jitter must be equal or greater than zero'

        self._id = worker_id
        self._scheduler = scheduler
        self._assistant = assistant
        self._stop_requesting_work = False

        self.host = socket.gethostname()
        self._scheduled_tasks: Dict[str, Task] = {}
        self._suspended_tasks: Dict[str, Task] = {}
        self._batch_running_tasks: Dict[str, Any] = {}
        self._batch_families_sent: Set[str] = set()

        self._first_task = None

        self.add_succeeded = True
        self.run_succeeded = True

        self.unfulfilled_counts: Dict[str, int] = collections.defaultdict(int)

        # note that ``signal.signal(signal.SIGUSR1, fn)`` only works inside the main execution thread, which is why we
        # provide the ability to conditionally install the hook.
        if not self._config.no_install_shutdown_handler:
            try:
                signal.signal(signal.SIGUSR1, self.handle_interrupt)
                signal.siginterrupt(signal.SIGUSR1, False)
            except AttributeError:
                pass

        # Keep info about what tasks are running (could be in other processes)
        self._task_result_queue: multiprocessing.Queue = multiprocessing.Queue()
        self._running_tasks: Dict[str, TaskProcess] = {}
        self._idle_since: Optional[datetime.datetime] = None

        # mp-safe dictionary for caching completation checks across task processes
        self._task_completion_cache = None
        if self._config.cache_task_completion:
            self._task_completion_cache = multiprocessing.Manager().dict()

        # Stuff for execution_summary
        self._add_task_history: List[Any] = []
        self._get_work_response_history: List[Any] = []

    def _add_task(self, *args, **kwargs):
        """
        Call ``self._scheduler.add_task``, but store the values too so we can
        implement :py:func:`luigi.execution_summary.summary`.
        """
        task_id = kwargs['task_id']
        status = kwargs['status']
        runnable = kwargs['runnable']
        task = self._scheduled_tasks.get(task_id)
        if task:
            self._add_task_history.append((task, status, runnable))
            kwargs['owners'] = task._owner_list()

        if task_id in self._batch_running_tasks:
            for batch_task in self._batch_running_tasks.pop(task_id):
                self._add_task_history.append((batch_task, status, True))

        if task and kwargs.get('params'):
            kwargs['param_visibilities'] = task._get_param_visibilities()

        self._scheduler.add_task(*args, **kwargs)

        logger.info('Informed scheduler that task   %s   has status   %s', task_id, status)

    def __enter__(self) -> 'Worker':
        """
        Start the KeepAliveThread.
        """
        self._keep_alive_thread = luigi.worker.KeepAliveThread(self._scheduler, self._id, self._config.ping_interval, self._handle_rpc_message)
        self._keep_alive_thread.daemon = True
        self._keep_alive_thread.start()
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> Literal[False]:
        """
        Stop the KeepAliveThread and kill still running tasks.
        """
        self._keep_alive_thread.stop()
        self._keep_alive_thread.join()
        for task in self._running_tasks.values():
            if task.is_alive():
                task.terminate()
        self._task_result_queue.close()
        return False  # Don't suppress exception

    def _generate_worker_info(self) -> List[Tuple[str, Any]]:
        # Generate as much info as possible about the worker
        # Some of these calls might not be available on all OS's
        args = [('salt', '%09d' % random.randrange(0, 10_000_000_000)), ('workers', self.worker_processes)]
        try:
            args += [('host', socket.gethostname())]
        except BaseException:
            pass
        try:
            args += [('username', getpass.getuser())]
        except BaseException:
            pass
        try:
            args += [('pid', os.getpid())]
        except BaseException:
            pass
        try:
            sudo_user = os.getenv('SUDO_USER')
            if sudo_user:
                args.append(('sudo_user', sudo_user))
        except BaseException:
            pass
        return args

    def _generate_worker_id(self, worker_info: List[Any]) -> str:
        worker_info_str = ', '.join(['{}={}'.format(k, v) for k, v in worker_info])
        return 'Worker({})'.format(worker_info_str)

    def _validate_task(self, task: Task) -> None:
        if not isinstance(task, Task):
            raise luigi.worker.TaskException('Can not schedule non-task %s' % task)

        if not task.initialized():
            # we can't get the repr of it since it's not initialized...
            raise luigi.worker.TaskException(
                'Task of class %s not initialized. Did you override __init__ and forget to call super(...).__init__?' % task.__class__.__name__
            )

    def _log_complete_error(self, task: Task, tb: str) -> None:
        log_msg = 'Will not run {task} or any dependencies due to error in complete() method:\n{tb}'.format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_dependency_error(self, task: Task, tb: str) -> None:
        log_msg = 'Will not run {task} or any dependencies due to error in deps() method:\n{tb}'.format(task=task, tb=tb)
        logger.warning(log_msg)

    def _log_unexpected_error(self, task: Task) -> None:
        logger.exception('Luigi unexpected framework error while scheduling %s', task)  # needs to be called from within except clause

    def _announce_scheduling_failure(self, task: Task, expl: Any) -> None:
        try:
            self._scheduler.announce_scheduling_failure(
                worker=self._id,
                task_name=str(task),
                family=task.task_family,
                params=task.to_str_params(only_significant=True),
                expl=expl,
                owners=task._owner_list(),
            )
        except Exception:
            formatted_traceback = traceback.format_exc()
            self._email_unexpected_error(task, formatted_traceback)
            raise

    def _email_complete_error(self, task: Task, formatted_traceback: str) -> None:
        self._announce_scheduling_failure(task, formatted_traceback)
        if self._config.send_failure_email:
            self._email_error(
                task,
                formatted_traceback,
                subject='Luigi: {task} failed scheduling. Host: {host}',
                headline='Will not run {task} or any dependencies due to error in complete() method',
            )

    def _email_dependency_error(self, task: Task, formatted_traceback: str) -> None:
        self._announce_scheduling_failure(task, formatted_traceback)
        if self._config.send_failure_email:
            self._email_error(
                task,
                formatted_traceback,
                subject='Luigi: {task} failed scheduling. Host: {host}',
                headline='Will not run {task} or any dependencies due to error in deps() method',
            )

    def _email_unexpected_error(self, task: Task, formatted_traceback: str) -> None:
        # this sends even if failure e-mails are disabled, as they may indicate
        # a more severe failure that may not reach other alerting methods such
        # as scheduler batch notification
        self._email_error(
            task,
            formatted_traceback,
            subject='Luigi: Framework error while scheduling {task}. Host: {host}',
            headline='Luigi framework error',
        )

    def _email_task_failure(self, task: Task, formatted_traceback: str) -> None:
        if self._config.send_failure_email:
            self._email_error(
                task,
                formatted_traceback,
                subject='Luigi: {task} FAILED. Host: {host}',
                headline='A task failed when running. Most likely run() raised an exception.',
            )

    def _email_error(self, task: Task, formatted_traceback: str, subject: str, headline: str) -> None:
        formatted_subject = subject.format(task=task, host=self.host)
        formatted_headline = headline.format(task=task, host=self.host)
        command = subprocess.list2cmdline(sys.argv)
        message = notifications.format_task_error(formatted_headline, task, command, formatted_traceback)
        notifications.send_error_email(formatted_subject, message, task.owner_email)

    def _handle_task_load_error(self, exception: Exception, task_ids: List[str]) -> None:
        msg = 'Cannot find task(s) sent by scheduler: {}'.format(','.join(task_ids))
        logger.exception(msg)
        subject = 'Luigi: {}'.format(msg)
        error_message = notifications.wrap_traceback(exception)
        for task_id in task_ids:
            self._add_task(
                worker=self._id,
                task_id=task_id,
                status=FAILED,
                runnable=False,
                expl=error_message,
            )
        notifications.send_error_email(subject, error_message)

    def add(self, task: Task, multiprocess: bool = False, processes: int = 0) -> bool:
        """
        Add a Task for the worker to check and possibly schedule and run.

        Returns True if task and its dependencies were successfully scheduled or completed before.
        """
        if self._first_task is None and hasattr(task, 'task_id'):
            self._first_task = task.task_id
        self.add_succeeded = True
        if multiprocess:
            queue: Any = multiprocessing.Manager().Queue()
            pool: Any = multiprocessing.Pool(processes=processes if processes > 0 else None)
        else:
            queue = luigi.worker.DequeQueue()
            pool = luigi.worker.SingleProcessPool()
        self._validate_task(task)
        pool.apply_async(luigi.worker.check_complete, [task, queue, self._task_completion_cache])

        # we track queue size ourselves because len(queue) won't work for multiprocessing
        queue_size = 1
        try:
            seen = {task.task_id}
            while queue_size:
                current = queue.get()
                queue_size -= 1
                item, is_complete = current
                for next in self._add(item, is_complete):
                    if next.task_id not in seen:
                        self._validate_task(next)
                        seen.add(next.task_id)
                        pool.apply_async(luigi.worker.check_complete, [next, queue, self._task_completion_cache])
                        queue_size += 1
        except (KeyboardInterrupt, luigi.worker.TaskException):
            raise
        except Exception as ex:
            self.add_succeeded = False
            formatted_traceback = traceback.format_exc()
            self._log_unexpected_error(task)
            task.trigger_event(Event.BROKEN_TASK, task, ex)
            self._email_unexpected_error(task, formatted_traceback)
            raise
        finally:
            pool.close()
            pool.join()
        return self.add_succeeded

    def _add_task_batcher(self, task: Task) -> None:
        family = task.task_family
        if family not in self._batch_families_sent:
            task_class = type(task)
            batch_param_names = task_class.batch_param_names()
            if batch_param_names:
                self._scheduler.add_task_batcher(
                    worker=self._id,
                    task_family=family,
                    batched_args=batch_param_names,
                    max_batch_size=task.max_batch_size,
                )
            self._batch_families_sent.add(family)

    def _add(self, task: Task, is_complete: bool) -> Generator[Task, None, None]:
        if self._config.task_limit is not None and len(self._scheduled_tasks) >= self._config.task_limit:
            logger.warning('Will not run %s or any dependencies due to exceeded task-limit of %d', task, self._config.task_limit)
            deps = None
            status = UNKNOWN
            runnable = False

        else:
            formatted_traceback = None
            try:
                self._check_complete_value(is_complete)
            except KeyboardInterrupt:
                raise
            except luigi.worker.AsyncCompletionException as ex:
                formatted_traceback = ex.trace
            except BaseException:
                formatted_traceback = traceback.format_exc()

            if formatted_traceback is not None:
                self.add_succeeded = False
                self._log_complete_error(task, formatted_traceback)
                task.trigger_event(Event.DEPENDENCY_MISSING, task)
                self._email_complete_error(task, formatted_traceback)
                deps = None
                status = UNKNOWN
                runnable = False

            elif is_complete:
                deps = None
                status = DONE
                runnable = False
                task.trigger_event(Event.DEPENDENCY_PRESENT, task)

            elif _is_external(task):
                deps = None
                status = PENDING
                runnable = self._config.retry_external_tasks
                task.trigger_event(Event.DEPENDENCY_MISSING, task)
                logger.warning(
                    'Data for %s does not exist (yet?). The task is an ' 'external data dependency, so it cannot be run from' ' this luigi process.', task
                )

            else:
                try:
                    deps = task.deps()
                    self._add_task_batcher(task)
                except Exception as ex:
                    formatted_traceback = traceback.format_exc()
                    self.add_succeeded = False
                    self._log_dependency_error(task, formatted_traceback)
                    task.trigger_event(Event.BROKEN_TASK, task, ex)
                    self._email_dependency_error(task, formatted_traceback)
                    deps = None
                    status = UNKNOWN
                    runnable = False
                else:
                    status = PENDING
                    runnable = True

            if task.disabled:
                status = DISABLED

            if deps:
                for d in deps:
                    self._validate_dependency(d)
                    task.trigger_event(Event.DEPENDENCY_DISCOVERED, task, d)
                    yield d  # return additional tasks to add

                deps = [d.task_id for d in deps]

        self._scheduled_tasks[task.task_id] = task
        self._add_task(
            worker=self._id,
            task_id=task.task_id,
            status=status,
            deps=deps,
            runnable=runnable,
            priority=task.priority,
            resources=task.process_resources(),
            params=task.to_str_params(),
            family=task.task_family,
            module=task.task_module,
            batchable=task.batchable,
            retry_policy_dict=_get_retry_policy_dict(task),
            accepts_messages=task.accepts_messages,
        )

    def _validate_dependency(self, dependency: Task) -> None:
        if isinstance(dependency, Target):
            raise Exception('requires() can not return Target objects. Wrap it in an ExternalTask class')
        elif not isinstance(dependency, Task):
            raise Exception('requires() must return Task objects but {} is a {}'.format(dependency, type(dependency)))

    def _check_complete_value(self, is_complete: bool) -> None:
        if is_complete not in (True, False):
            if isinstance(is_complete, luigi.worker.TracebackWrapper):
                raise luigi.workerAsyncCompletionException(is_complete.trace)
            raise Exception('Return value of Task.complete() must be boolean (was %r)' % is_complete)

    def _add_worker(self) -> None:
        self._worker_info.append(('first_task', self._first_task))
        self._scheduler.add_worker(self._id, self._worker_info)

    def _log_remote_tasks(self, get_work_response: GetWorkResponse) -> None:
        logger.debug('Done')
        logger.debug('There are no more tasks to run at this time')
        if get_work_response.running_tasks:
            for r in get_work_response.running_tasks:
                logger.debug('%s is currently run by worker %s', r['task_id'], r['worker'])
        elif get_work_response.n_pending_tasks:
            logger.debug('There are %s pending tasks possibly being run by other workers', get_work_response.n_pending_tasks)
            if get_work_response.n_unique_pending:
                logger.debug('There are %i pending tasks unique to this worker', get_work_response.n_unique_pending)
            if get_work_response.n_pending_last_scheduled:
                logger.debug('There are %i pending tasks last scheduled by this worker', get_work_response.n_pending_last_scheduled)

    def _get_work_task_id(self, get_work_response: Dict[str, Any]) -> Optional[str]:
        if get_work_response.get('task_id') is not None:
            return get_work_response['task_id']
        elif 'batch_id' in get_work_response:
            try:
                task = load_task(
                    module=get_work_response.get('task_module'),
                    task_name=get_work_response['task_family'],
                    params_str=get_work_response['task_params'],
                )
            except Exception as ex:
                self._handle_task_load_error(ex, get_work_response['batch_task_ids'])
                self.run_succeeded = False
                return None

            self._scheduler.add_task(
                worker=self._id,
                task_id=task.task_id,
                module=get_work_response.get('task_module'),
                family=get_work_response['task_family'],
                params=task.to_str_params(),
                status=RUNNING,
                batch_id=get_work_response['batch_id'],
            )
            return task.task_id
        else:
            return None

    def _get_work(self) -> GetWorkResponse:
        if self._stop_requesting_work:
            return GetWorkResponse(None, 0, 0, 0, 0, WORKER_STATE_DISABLED)

        if self.worker_processes > 0:
            logger.debug('Asking scheduler for work...')
            r = self._scheduler.get_work(
                worker=self._id,
                host=self.host,
                assistant=self._assistant,
                current_tasks=list(self._running_tasks.keys()),
            )
        else:
            logger.debug('Checking if tasks are still pending')
            r = self._scheduler.count_pending(worker=self._id)

        running_tasks = r['running_tasks']
        task_id = self._get_work_task_id(r)

        self._get_work_response_history.append(
            {
                'task_id': task_id,
                'running_tasks': running_tasks,
            }
        )

        if task_id is not None and task_id not in self._scheduled_tasks:
            logger.info('Did not schedule %s, will load it dynamically', task_id)

            try:
                # TODO: we should obtain the module name from the server!
                self._scheduled_tasks[task_id] = load_task(module=r.get('task_module'), task_name=r['task_family'], params_str=r['task_params'])
            except TaskClassException as ex:
                self._handle_task_load_error(ex, [task_id])
                task_id = None
                self.run_succeeded = False

        if task_id is not None and 'batch_task_ids' in r:
            batch_tasks = filter(None, [self._scheduled_tasks.get(batch_id) for batch_id in r['batch_task_ids']])
            self._batch_running_tasks[task_id] = batch_tasks

        return GetWorkResponse(
            task_id=task_id,
            running_tasks=running_tasks,
            n_pending_tasks=r['n_pending_tasks'],
            n_unique_pending=r['n_unique_pending'],
            # TODO: For a tiny amount of time (a month?) we'll keep forwards compatibility
            #  That is you can user a newer client than server (Sep 2016)
            n_pending_last_scheduled=r.get('n_pending_last_scheduled', 0),
            worker_state=r.get('worker_state', WORKER_STATE_ACTIVE),
        )

    def _run_task(self, task_id: str) -> None:
        if task_id in self._running_tasks:
            logger.debug('Got already running task id {} from scheduler, taking a break'.format(task_id))
            next(self._sleeper())
            return

        task = self._scheduled_tasks[task_id]

        task_process = self._create_task_process(task)

        self._running_tasks[task_id] = task_process

        if task_process.use_multiprocessing:
            with fork_lock:
                task_process.start()
        else:
            # Run in the same process
            task_process.run()

    def _create_task_process(self, task):
        message_queue: Any = multiprocessing.Queue() if task.accepts_messages else None
        reporter = luigi.worker.TaskStatusReporter(self._scheduler, task.task_id, self._id, message_queue)
        use_multiprocessing = self._config.force_multiprocessing or bool(self.worker_processes > 1)
        return ContextManagedTaskProcess(
            self._config.task_process_context,
            task,
            self._id,
            self._task_result_queue,
            reporter,
            use_multiprocessing=use_multiprocessing,
            worker_timeout=self._config.timeout,
            check_unfulfilled_deps=self._config.check_unfulfilled_deps,
            check_complete_on_run=self._config.check_complete_on_run,
            task_completion_cache=self._task_completion_cache,
            skip_if_completed_pre_run=self._config.skip_if_completed_pre_run,
        )

    def _purge_children(self) -> None:
        """
        Find dead children and put a response on the result queue.

        :return:
        """
        for task_id, p in self._running_tasks.items():
            if not p.is_alive() and p.exitcode:
                error_msg = 'Task {} died unexpectedly with exit code {}'.format(task_id, p.exitcode)
                p.task.trigger_event(Event.PROCESS_FAILURE, p.task, error_msg)
            elif p.timeout_time is not None and time.time() > float(p.timeout_time) and p.is_alive():
                p.terminate()
                error_msg = 'Task {} timed out after {} seconds and was terminated.'.format(task_id, p.worker_timeout)
                p.task.trigger_event(Event.TIMEOUT, p.task, error_msg)
            else:
                continue

            logger.info(error_msg)
            self._task_result_queue.put((task_id, FAILED, error_msg, [], []))

    def _handle_next_task(self) -> None:
        """
        We have to catch three ways a task can be "done":

        1. normal execution: the task runs/fails and puts a result back on the queue,
        2. new dependencies: the task yielded new deps that were not complete and
           will be rescheduled and dependencies added,
        3. child process dies: we need to catch this separately.
        """
        self._idle_since = None
        while True:
            self._purge_children()  # Deal with subprocess failures

            try:
                task_id, status, expl, missing, new_requirements = self._task_result_queue.get(timeout=self._config.wait_interval)
            except Queue.Empty:
                return

            task = self._scheduled_tasks[task_id]
            if not task or task_id not in self._running_tasks:
                continue
                # Not a running task. Probably already removed.
                # Maybe it yielded something?

            # external task if run not implemented, retry-able if config option is enabled.
            external_task_retryable = _is_external(task) and self._config.retry_external_tasks
            if status == FAILED and not external_task_retryable:
                self._email_task_failure(task, expl)

            new_deps = []
            if new_requirements:
                new_req = [load_task(module, name, params) for module, name, params in new_requirements]
                for t in new_req:
                    self.add(t)
                new_deps = [t.task_id for t in new_req]

            self._add_task(
                worker=self._id,
                task_id=task_id,
                status=status,
                expl=json.dumps(expl),
                resources=task.process_resources(),
                runnable=None,
                params=task.to_str_params(),
                family=task.task_family,
                module=task.task_module,
                new_deps=new_deps,
                assistant=self._assistant,
                retry_policy_dict=_get_retry_policy_dict(task),
            )

            self._running_tasks.pop(task_id)

            # re-add task to reschedule missing dependencies
            if missing:
                reschedule = True

                # keep out of infinite loops by not rescheduling too many times
                for task_id in missing:
                    self.unfulfilled_counts[task_id] += 1
                    if self.unfulfilled_counts[task_id] > self._config.max_reschedules:
                        reschedule = False
                if reschedule:
                    self.add(task)

            self.run_succeeded &= (status == DONE) or (len(new_deps) > 0)
            return

    def _sleeper(self) -> Generator[None, None, None]:
        # TODO is exponential backoff necessary?
        while True:
            jitter = self._config.wait_jitter
            wait_interval = self._config.wait_interval + random.uniform(0, jitter)
            logger.debug('Sleeping for %f seconds', wait_interval)
            time.sleep(wait_interval)
            yield

    def _keep_alive(self, get_work_response) -> bool:
        """
        Returns true if a worker should stay alive given.

        If worker-keep-alive is not set, this will always return false.
        For an assistant, it will always return the value of worker-keep-alive.
        Otherwise, it will return true for nonzero n_pending_tasks.

        If worker-count-uniques is true, it will also
        require that one of the tasks is unique to this worker.
        """
        if not self._config.keep_alive:
            return False
        elif self._assistant:
            return True
        elif self._config.count_last_scheduled:
            return get_work_response.n_pending_last_scheduled > 0
        elif self._config.count_uniques:
            return get_work_response.n_unique_pending > 0
        elif get_work_response.n_pending_tasks == 0:
            return False
        elif not self._config.max_keep_alive_idle_duration:
            return True
        elif not self._idle_since:
            return True
        else:
            time_to_shutdown = self._idle_since + self._config.max_keep_alive_idle_duration - datetime.datetime.now()
            logger.debug('[%s] %s until shutdown', self._id, time_to_shutdown)
            return time_to_shutdown > datetime.timedelta(0)

    def handle_interrupt(self, signum, _) -> None:
        """
        Stops the assistant from asking for more work on SIGUSR1
        """
        if signum == signal.SIGUSR1:
            self._start_phasing_out()

    def _start_phasing_out(self) -> None:
        """
        Go into a mode where we dont ask for more work and quit once existing
        tasks are done.
        """
        self._config.keep_alive = False
        self._stop_requesting_work = True

    def run(self) -> bool:
        """
        Returns True if all scheduled tasks were executed successfully.
        """
        logger.info('Running Worker with %d processes', self.worker_processes)

        sleeper = self._sleeper()
        self.run_succeeded = True

        self._add_worker()

        while True:
            while len(self._running_tasks) >= self.worker_processes > 0:
                logger.debug('%d running tasks, waiting for next task to finish', len(self._running_tasks))
                self._handle_next_task()

            get_work_response = self._get_work()

            if get_work_response.worker_state == WORKER_STATE_DISABLED:
                self._start_phasing_out()

            if get_work_response.task_id is None:
                if not self._stop_requesting_work:
                    self._log_remote_tasks(get_work_response)
                if len(self._running_tasks) == 0:
                    self._idle_since = self._idle_since or datetime.datetime.now()
                    if self._keep_alive(get_work_response):
                        next(sleeper)
                        continue
                    else:
                        break
                else:
                    self._handle_next_task()
                    continue

            # task_id is not None:
            logger.debug('Pending tasks: %s', get_work_response.n_pending_tasks)
            self._run_task(get_work_response.task_id)

        while len(self._running_tasks):
            logger.debug('Shut down Worker, %d more tasks to go', len(self._running_tasks))
            self._handle_next_task()

        return self.run_succeeded

    def _handle_rpc_message(self, message: Dict[str, Any]) -> None:
        logger.info('Worker %s got message %s' % (self._id, message))

        # the message is a dict {'name': <function_name>, 'kwargs': <function_kwargs>}
        name = message['name']
        kwargs = message['kwargs']

        # find the function and check if it's callable and configured to work
        # as a message callback
        func = getattr(self, name, None)
        tpl = (self._id, name)
        if not callable(func):
            logger.error("Worker %s has no function '%s'" % tpl)
        elif not getattr(func, 'is_rpc_message_callback', False):
            logger.error("Worker %s function '%s' is not available as rpc message callback" % tpl)
        else:
            logger.info("Worker %s successfully dispatched rpc message to function '%s'" % tpl)
            func(**kwargs)

    @luigi.worker.rpc_message_callback
    def set_worker_processes(self, n: int) -> None:
        # set the new value
        self.worker_processes = max(1, n)

        # tell the scheduler
        self._scheduler.add_worker(self._id, {'workers': self.worker_processes})

    @luigi.worker.rpc_message_callback
    def dispatch_scheduler_message(self, task_id: str, message_id: str, content: str, **kwargs: Any) -> None:
        task_id = str(task_id)
        if task_id in self._running_tasks:
            task_process = self._running_tasks[task_id]
            if task_process.status_reporter.scheduler_messages:
                message = luigi.worker.SchedulerMessage(self._scheduler, task_id, message_id, content, **kwargs)
                task_process.status_reporter.scheduler_messages.put(message)
