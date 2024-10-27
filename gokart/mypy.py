"""Plugin that provides support for gokart.TaskOnKart.

This Code reuses the code from mypy.plugins.dataclasses
https://github.com/python/mypy/blob/0753e2a82dad35034e000609b6e8daa37238bfaa/mypy/plugins/dataclasses.py
"""

from __future__ import annotations

import re
from typing import Callable, Final, Iterator, Literal, Optional

import luigi
from mypy.expandtype import expand_type
from mypy.nodes import (
    ARG_NAMED_OPT,
    Argument,
    AssignmentStmt,
    Block,
    CallExpr,
    ClassDef,
    EllipsisExpr,
    Expression,
    IfStmt,
    JsonDict,
    MemberExpr,
    NameExpr,
    PlaceholderNode,
    RefExpr,
    Statement,
    TempNode,
    TypeInfo,
    Var,
)
from mypy.plugin import ClassDefContext, FunctionContext, Plugin, SemanticAnalyzerPluginInterface
from mypy.plugins.common import (
    add_method_to_class,
    deserialize_and_fixup_type,
)
from mypy.server.trigger import make_wildcard_trigger
from mypy.state import state
from mypy.typeops import map_type_from_supertype
from mypy.types import (
    AnyType,
    Instance,
    NoneType,
    Type,
    TypeOfAny,
    UnionType,
)
from mypy.typevars import fill_typevars

METADATA_TAG: Final[str] = 'task_on_kart'

PARAMETER_FULLNAME_MATCHER: Final = re.compile(r'^(gokart|luigi)(\.parameter)?\.\w*Parameter$')
PARAMETER_TMP_MATCHER: Final = re.compile(r'^\w*Parameter$')


class TaskOnKartPlugin(Plugin):
    def get_base_class_hook(self, fullname: str) -> Callable[[ClassDefContext], None] | None:
        # The following gathers attributes from gokart.TaskOnKart such as `workspace_directory`
        # the transformation does not affect because the class has `__init__` method of `gokart.TaskOnKart`.
        #
        # NOTE: `gokart.task.luigi.Task` condition is required for the release of luigi versions without py.typed
        if fullname in {'gokart.task.luigi.Task', 'luigi.task.Task'}:
            return self._task_on_kart_class_maker_callback

        sym = self.lookup_fully_qualified(fullname)
        if sym and isinstance(sym.node, TypeInfo):
            if any(base.fullname == 'gokart.task.TaskOnKart' for base in sym.node.mro):
                return self._task_on_kart_class_maker_callback
        return None

    def get_function_hook(self, fullname: str) -> Callable[[FunctionContext], Type] | None:
        """Adjust the return type of the `Parameters` function."""
        if PARAMETER_FULLNAME_MATCHER.match(fullname):
            return self._task_on_kart_parameter_field_callback
        return None

    def _task_on_kart_class_maker_callback(self, ctx: ClassDefContext) -> None:
        transformer = TaskOnKartTransformer(ctx.cls, ctx.reason, ctx.api)
        transformer.transform()

    def _task_on_kart_parameter_field_callback(self, ctx: FunctionContext) -> Type:
        """Extract the type of the `default` argument from the Field function, and use it as the return type.

        In particular:
        * Retrieve the type of the argument which is specified, and use it as return type for the function.
        * If no default argument is specified, return AnyType with unannotated type instead of parameter types like `luigi.Parameter()`
          This makes mypy avoid conflict between the type annotation and the parameter type.
          e.g.
          ```python
          foo: int = luigi.IntParameter()
          ```
        """
        try:
            default_idx = ctx.callee_arg_names.index('default')
        # if no `default` argument is found, return AnyType with unannotated type.
        except ValueError:
            return AnyType(TypeOfAny.unannotated)

        default_args = ctx.args[default_idx]

        if default_args:
            default_type = ctx.arg_types[0][0]
            default_arg = default_args[0]

            # Fallback to default Any type if the field is required
            if not isinstance(default_arg, EllipsisExpr):
                return default_type
        # NOTE: This is a workaround to avoid the error between type annotation and parameter type.
        #       As the following code snippet, the type of `foo` is `int` but the assigned value is `luigi.IntParameter()`.
        #       foo: int = luigi.IntParameter()
        # TODO: infer mypy type from the parameter type.
        return AnyType(TypeOfAny.unannotated)


class TaskOnKartAttribute:
    def __init__(
        self,
        name: str,
        has_default: bool,
        line: int,
        column: int,
        type: Type | None,
        info: TypeInfo,
        api: SemanticAnalyzerPluginInterface,
    ) -> None:
        self.name = name
        self.has_default = has_default
        self.line = line
        self.column = column
        self.type = type  # Type as __init__ argument
        self.info = info
        self._api = api

    def to_argument(self, current_info: TypeInfo, *, of: Literal['__init__',]) -> Argument:
        if of == '__init__':
            # All arguments to __init__ are keyword-only and optional
            # This is because gokart can set parameters by configuration'
            arg_kind = ARG_NAMED_OPT
        return Argument(
            variable=self.to_var(current_info),
            type_annotation=self.expand_type(current_info),
            initializer=EllipsisExpr() if self.has_default else None,  # Only used by stubgen
            kind=arg_kind,
        )

    def expand_type(self, current_info: TypeInfo) -> Type | None:
        if self.type is not None and self.info.self_type is not None:
            # In general, it is not safe to call `expand_type()` during semantic analysis,
            # however this plugin is called very late, so all types should be fully ready.
            # Also, it is tricky to avoid eager expansion of Self types here (e.g. because
            # we serialize attributes).
            with state.strict_optional_set(self._api.options.strict_optional):
                return expand_type(self.type, {self.info.self_type.id: fill_typevars(current_info)})
        return self.type

    def to_var(self, current_info: TypeInfo) -> Var:
        return Var(self.name, self.expand_type(current_info))

    def serialize(self) -> JsonDict:
        assert self.type
        return {
            'name': self.name,
            'has_default': self.has_default,
            'line': self.line,
            'column': self.column,
            'type': self.type.serialize(),
        }

    @classmethod
    def deserialize(cls, info: TypeInfo, data: JsonDict, api: SemanticAnalyzerPluginInterface) -> TaskOnKartAttribute:
        data = data.copy()
        typ = deserialize_and_fixup_type(data.pop('type'), api)
        return cls(type=typ, info=info, **data, api=api)

    def expand_typevar_from_subtype(self, sub_type: TypeInfo) -> None:
        """Expands type vars in the context of a subtype when an attribute is inherited
        from a generic super type."""
        if self.type is not None:
            with state.strict_optional_set(self._api.options.strict_optional):
                self.type = map_type_from_supertype(self.type, sub_type, self.info)


class TaskOnKartTransformer:
    """Implement the behavior of gokart.TaskOnKart."""

    def __init__(
        self,
        cls: ClassDef,
        reason: Expression | Statement,
        api: SemanticAnalyzerPluginInterface,
    ) -> None:
        self._cls = cls
        self._reason = reason
        self._api = api

    def transform(self) -> bool:
        """Apply all the necessary transformations to the underlying gokart.TaskOnKart"""
        info = self._cls.info
        attributes = self.collect_attributes()

        if attributes is None:
            # Some definitions are not ready. We need another pass.
            return False
        for attr in attributes:
            if attr.type is None:
                return False
        # If there are no attributes, it may be that the semantic analyzer has not
        # processed them yet. In order to work around this, we can simply skip generating
        # __init__ if there are no attributes, because if the user truly did not define any,
        # then the object default __init__ with an empty signature will be present anyway.
        if ('__init__' not in info.names or info.names['__init__'].plugin_generated) and attributes:
            args = [attr.to_argument(info, of='__init__') for attr in attributes]
            add_method_to_class(self._api, self._cls, '__init__', args=args, return_type=NoneType())
        info.metadata[METADATA_TAG] = {
            'attributes': [attr.serialize() for attr in attributes],
        }

        return True

    def _get_assignment_statements_from_if_statement(self, stmt: IfStmt) -> Iterator[AssignmentStmt]:
        for body in stmt.body:
            if not body.is_unreachable:
                yield from self._get_assignment_statements_from_block(body)
        if stmt.else_body is not None and not stmt.else_body.is_unreachable:
            yield from self._get_assignment_statements_from_block(stmt.else_body)

    def _get_assignment_statements_from_block(self, block: Block) -> Iterator[AssignmentStmt]:
        for stmt in block.body:
            if isinstance(stmt, AssignmentStmt):
                yield stmt
            elif isinstance(stmt, IfStmt):
                yield from self._get_assignment_statements_from_if_statement(stmt)

    def collect_attributes(self) -> Optional[list[TaskOnKartAttribute]]:
        """Collect all attributes declared in the task and its parents.

        All assignments of the form

          a: SomeType
          b: SomeOtherType = ...

        are collected.

        Return None if some base class hasn't been processed
        yet and thus we'll need to ask for another pass.
        """
        cls = self._cls

        # First, collect attributes belonging to any class in the MRO, ignoring duplicates.
        #
        # We iterate through the MRO in reverse because attrs defined in the parent must appear
        # earlier in the attributes list than attrs defined in the child.
        #
        # However, we also want attributes defined in the subtype to override ones defined
        # in the parent. We can implement this via a dict without disrupting the attr order
        # because dicts preserve insertion order in Python 3.7+.
        found_attrs: dict[str, TaskOnKartAttribute] = {}
        for info in reversed(cls.info.mro[1:-1]):
            if METADATA_TAG not in info.metadata:
                continue
            # Each class depends on the set of attributes in its task_on_kart ancestors.
            self._api.add_plugin_dependency(make_wildcard_trigger(info.fullname))

            for data in info.metadata[METADATA_TAG]['attributes']:
                name: str = data['name']

                attr = TaskOnKartAttribute.deserialize(info, data, self._api)
                # TODO: We shouldn't be performing type operations during the main
                #       semantic analysis pass, since some TypeInfo attributes might
                #       still be in flux. This should be performed in a later phase.
                attr.expand_typevar_from_subtype(cls.info)
                found_attrs[name] = attr

                sym_node = cls.info.names.get(name)
                if sym_node and sym_node.node and not isinstance(sym_node.node, Var):
                    self._api.fail(
                        'TaskOnKart attribute may only be overridden by another attribute',
                        sym_node.node,
                    )

        # Second, collect attributes belonging to the current class.
        current_attr_names: set[str] = set()
        for stmt in self._get_assignment_statements_from_block(cls.defs):
            if not is_parameter_call(stmt.rvalue):
                continue

            # a: int, b: str = 1, 'foo' is not supported syntax so we
            # don't have to worry about it.
            lhs = stmt.lvalues[0]
            if not isinstance(lhs, NameExpr):
                continue
            sym = cls.info.names.get(lhs.name)
            if sym is None:
                # There was probably a semantic analysis error.
                continue

            node = sym.node
            assert not isinstance(node, PlaceholderNode)

            assert isinstance(node, Var)

            has_parameter_call, parameter_args = self._collect_parameter_args(stmt.rvalue)
            has_default = False
            # Ensure that something like x: int = field() is rejected
            # after an attribute with a default.
            if has_parameter_call:
                has_default = 'default' in parameter_args

            # All other assignments are already type checked.
            elif not isinstance(stmt.rvalue, TempNode):
                has_default = True

            if not has_default:
                # Make all non-default task_on_kart attributes implicit because they are de-facto
                # set on self in the generated __init__(), not in the class body. On the other
                # hand, we don't know how custom task_on_kart transforms initialize attributes,
                # so we don't treat them as implicit. This is required to support descriptors
                # (https://github.com/python/mypy/issues/14868).
                sym.implicit = True

            current_attr_names.add(lhs.name)
            with state.strict_optional_set(self._api.options.strict_optional):
                init_type = sym.type

            # infer Parameter type
            if init_type is None:
                init_type = self._infer_type_from_parameters(stmt.rvalue)

            found_attrs[lhs.name] = TaskOnKartAttribute(
                name=lhs.name,
                has_default=has_default,
                line=stmt.line,
                column=stmt.column,
                type=init_type,
                info=cls.info,
                api=self._api,
            )

        return list(found_attrs.values())

    def _collect_parameter_args(self, expr: Expression) -> tuple[bool, dict[str, Expression]]:
        """Returns a tuple where the first value represents whether or not
        the expression is a call to luigi.Parameter() or gokart.TaskInstanceParameter()
        and the second value is a dictionary of the keyword arguments that luigi.Parameter() or gokart.TaskInstanceParameter() was called with.
        """
        if isinstance(expr, CallExpr) and isinstance(expr.callee, RefExpr):
            args = {}
            for name, arg in zip(expr.arg_names, expr.args):
                if name is None:
                    # NOTE: this is a workaround to get default value from a parameter
                    self._api.fail(
                        'Positional arguments are not allowed for parameters when using the mypy plugin. '
                        "Update your code to use named arguments, like luigi.Parameter(default='foo') instead of luigi.Parameter('foo')",
                        expr,
                    )
                    continue
                args[name] = arg
            return True, args
        return False, {}

    def _infer_type_from_parameters(self, parameter: Expression) -> Optional[Type]:
        """
        Generate default type from Parameter.
        For example, when parameter is `luigi.parameter.Parameter`, this method should return `str` type.
        """
        parameter_name = _extract_parameter_name(parameter)
        if parameter_name is None:
            return None

        underlying_type: Optional[Type] = None
        if parameter_name in ['luigi.parameter.Parameter', 'luigi.parameter.OptionalParameter']:
            underlying_type = self._api.named_type('builtins.str', [])
        elif parameter_name in ['luigi.parameter.IntParameter', 'luigi.parameter.OptionalIntParameter']:
            underlying_type = self._api.named_type('builtins.int', [])
        elif parameter_name in ['luigi.parameter.FloatParameter', 'luigi.parameter.OptionalFloatParameter']:
            underlying_type = self._api.named_type('builtins.float', [])
        elif parameter_name in ['luigi.parameter.BoolParameter', 'luigi.parameter.OptionalBoolParameter']:
            underlying_type = self._api.named_type('builtins.bool', [])
        elif parameter_name in ['luigi.parameter.DateParameter', 'luigi.parameter.MonthParameter', 'luigi.parameter.YearParameter']:
            underlying_type = self._api.named_type('datetime.date', [])
        elif parameter_name in ['luigi.parameter.DateHourParameter', 'luigi.parameter.DateMinuteParameter', 'luigi.parameter.DateSecondParameter']:
            underlying_type = self._api.named_type('datetime.datetime', [])
        elif parameter_name in ['luigi.parameter.TimeDeltaParameter']:
            underlying_type = self._api.named_type('datetime.timedelta', [])
        elif parameter_name in ['luigi.parameter.DictParameter', 'luigi.parameter.OptionalDictParameter']:
            underlying_type = self._api.named_type('builtins.dict', [AnyType(TypeOfAny.unannotated), AnyType(TypeOfAny.unannotated)])
        elif parameter_name in ['luigi.parameter.ListParameter', 'luigi.parameter.OptionalListParameter']:
            underlying_type = self._api.named_type('builtins.tuple', [AnyType(TypeOfAny.unannotated)])
        elif parameter_name in ['luigi.parameter.TupleParameter', 'luigi.parameter.OptionalTupleParameter']:
            underlying_type = self._api.named_type('builtins.tuple', [AnyType(TypeOfAny.unannotated)])
        elif parameter_name in ['luigi.parameter.PathParameter', 'luigi.parameter.OptionalPathParameter']:
            underlying_type = self._api.named_type('pathlib.Path', [])
        elif parameter_name in ['gokart.parameter.TaskInstanceParameter']:
            underlying_type = self._api.named_type('gokart.task.TaskOnKart', [AnyType(TypeOfAny.unannotated)])
        elif parameter_name in ['gokart.parameter.ListTaskInstanceParameter']:
            underlying_type = self._api.named_type('builtins.list', [self._api.named_type('gokart.task.TaskOnKart', [AnyType(TypeOfAny.unannotated)])])
        elif parameter_name in ['gokart.parameter.ExplicitBoolParameter']:
            underlying_type = self._api.named_type('builtins.bool', [])
        elif parameter_name in ['luigi.parameter.NumericalParameter']:
            underlying_type = self._get_type_from_args(parameter, 'var_type')
        elif parameter_name in ['luigi.parameter.ChoiceParameter']:
            underlying_type = self._get_type_from_args(parameter, 'var_type')
        elif parameter_name in ['luigi.parameter.ChoiceListPareameter']:
            base_type = self._get_type_from_args(parameter, 'var_type')
            if base_type is not None:
                underlying_type = self._api.named_type('builtins.tuple', [base_type])
        elif parameter_name in ['luigi.parameter.EnumParameter']:
            underlying_type = self._get_type_from_args(parameter, 'enum')
        elif parameter_name in ['luigi.parameter.EnumListParameter']:
            base_type = self._get_type_from_args(parameter, 'enum')
            if base_type is not None:
                underlying_type = self._api.named_type('builtins.tuple', [base_type])

        if underlying_type is None:
            return None

        # When parameter has Optional, it can be none value.
        if 'Optional' in parameter_name:
            return UnionType([underlying_type, NoneType()])

        return underlying_type

    def _get_type_from_args(self, parameter: Expression, arg_key: str) -> Optional[Type]:
        """
        get type from parameter arguments.

        e.x)
        When parameter is `luigi.ChoiceParameter(var_type=int)`, this method should return `int` type.
        """
        ok, args = self._collect_parameter_args(parameter)
        if not ok:
            return None

        if arg_key not in args:
            return None

        arg = args[arg_key]
        if not isinstance(arg, NameExpr):
            return None
        if not isinstance(arg.node, TypeInfo):
            return None
        return Instance(arg.node, [])


def is_parameter_call(expr: Expression) -> bool:
    """Checks if the expression is a call to luigi.Parameter()"""
    parameter_name = _extract_parameter_name(expr)
    if parameter_name is None:
        return False
    return PARAMETER_FULLNAME_MATCHER.match(parameter_name) is not None


def _extract_parameter_name(expr: Expression) -> Optional[str]:
    """Extract name if the expression is a call to luigi.Parameter()"""
    if not isinstance(expr, CallExpr):
        return None

    callee = expr.callee
    if isinstance(callee, MemberExpr):
        type_info = callee.node
        if type_info is None and isinstance(callee.expr, NameExpr):
            return f'{callee.expr.name}.{callee.name}'
    elif isinstance(callee, NameExpr):
        type_info = callee.node
    else:
        return None

    if isinstance(type_info, TypeInfo):
        return type_info.fullname

    # Currently, luigi doesn't provide py.typed. it will be released next to 3.5.1.
    # https://github.com/spotify/luigi/pull/3297
    # With the following code, we can't assume correctly.
    #
    # from luigi import Parameter
    # class MyTask(gokart.TaskOnKart):
    #     param = Parameter()
    if isinstance(type_info, Var) and luigi.__version__ <= '3.5.1':
        return type_info.name

    return None


def plugin(version: str) -> type[Plugin]:
    return TaskOnKartPlugin
