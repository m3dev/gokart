from __future__ import annotations

import copy
import functools
import json
import re
from collections.abc import Iterable
from logging import getLogger
from typing import Any, Final
from urllib.parse import urlsplit

from googleapiclient.model import makepatch

from gokart.gcs_config import GCSConfig
from gokart.required_task_output import RequiredTaskOutput
from gokart.utils import FlattenableItems

logger = getLogger(__name__)


class GCSObjectMetadataClient:
    """
    This class is Utility-Class, so should not be initialized.
    This class used for adding metadata as labels.
    """

    # Maximum metadata size for GCS objects (8 KiB)
    MAX_GCS_METADATA_SIZE: Final[int] = 8 * 1024

    @staticmethod
    def _is_log_related_path(path: str) -> bool:
        return re.match(r'^gs://.+?/log/(processing_time/|task_info/|task_log/|module_versions/|random_seed/|task_params/).+', path) is not None

    # This is the copied method of luigi.gcs._path_to_bucket_and_key(path).
    @staticmethod
    def _path_to_bucket_and_key(path: str) -> tuple[str, str]:
        (scheme, netloc, path, _, _) = urlsplit(path)
        assert scheme == 'gs'
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    @staticmethod
    def add_task_state_labels(
        path: str,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> None:
        if GCSObjectMetadataClient._is_log_related_path(path):
            return
        # In gokart/object_storage.get_time_stamp, could find same call.
        # _path_to_bucket_and_key is a private method, so, this might not be acceptable.
        bucket, obj = GCSObjectMetadataClient._path_to_bucket_and_key(path)
        _response = GCSConfig().get_gcs_client().client.objects().get(bucket=bucket, object=obj).execute()
        if _response is None:
            logger.error(f'failed to get object from GCS bucket {bucket} and object {obj}.')
            return
        response: dict[str, Any] = dict(_response)
        original_metadata: dict[Any, Any] = {}
        if 'metadata' in response.keys():
            _metadata = response.get('metadata')
            if _metadata is not None:
                original_metadata = dict(_metadata)
        patched_metadata = GCSObjectMetadataClient._get_patched_obj_metadata(
            copy.deepcopy(original_metadata),
            task_params,
            custom_labels,
            required_task_outputs,
        )
        if original_metadata != patched_metadata:
            # If we use update api, existing object metadata are removed, so should use patch api.
            # See the official document descriptions.
            # [Link] https://cloud.google.com/storage/docs/viewing-editing-metadata?hl=ja#rest-set-object-metadata
            update_response = (
                GCSConfig()
                .get_gcs_client()
                .client.objects()
                .patch(
                    bucket=bucket,
                    object=obj,
                    body=makepatch({'metadata': original_metadata}, {'metadata': patched_metadata}),
                )
                .execute()
            )
            if update_response is None:
                logger.error(f'failed to patch object {obj} in bucket {bucket} and object {obj}.')

    @staticmethod
    def _normalize_labels(labels: dict[str, Any] | None) -> dict[str, str]:
        return {str(key): str(value) for key, value in labels.items()} if labels else {}

    @staticmethod
    def _get_patched_obj_metadata(
        metadata: Any,
        task_params: dict[str, str] | None = None,
        custom_labels: dict[str, str] | None = None,
        required_task_outputs: FlattenableItems[RequiredTaskOutput] | None = None,
    ) -> dict | Any:
        # If metadata from response when getting bucket and object information is not dictionary,
        # something wrong might be happened, so return original metadata, no patched.
        if not isinstance(metadata, dict):
            logger.warning(f'metadata is not a dict: {metadata}, something wrong was happened when getting response when get bucket and object information.')
            return metadata
        # Maximum size of metadata for each object is 8 KiB.
        # [Link]: https://cloud.google.com/storage/quotas#objects
        normalized_task_params_labels = GCSObjectMetadataClient._normalize_labels(task_params)
        normalized_custom_labels = GCSObjectMetadataClient._normalize_labels(custom_labels)
        # There is a possibility that the keys of user-provided labels(custom_labels) may conflict with those generated from task parameters (task_params_labels).
        # However, users who utilize custom_labels are no longer expected to search using the labels generated from task parameters.
        # Instead, users are expected to search using the labels they provided.
        # Therefore, in the event of a key conflict, the value registered by the user-provided labels will take precedence.
        normalized_labels = [normalized_custom_labels, normalized_task_params_labels]
        if required_task_outputs:
            normalized_labels.append({'__required_task_outputs': json.dumps(GCSObjectMetadataClient._get_serialized_string(required_task_outputs))})

        _merged_labels = GCSObjectMetadataClient._merge_custom_labels_and_task_params_labels(normalized_labels)
        return GCSObjectMetadataClient._adjust_gcs_metadata_limit_size(dict(metadata) | _merged_labels)

    @staticmethod
    def _get_serialized_string(required_task_outputs: FlattenableItems[RequiredTaskOutput]) -> FlattenableItems[str]:
        if isinstance(required_task_outputs, RequiredTaskOutput):
            return required_task_outputs.serialize()
        elif isinstance(required_task_outputs, dict):
            return {k: GCSObjectMetadataClient._get_serialized_string(v) for k, v in required_task_outputs.items()}
        elif isinstance(required_task_outputs, Iterable):
            return [GCSObjectMetadataClient._get_serialized_string(ro) for ro in required_task_outputs]
        else:
            raise TypeError(
                f'Unsupported type for required_task_outputs: {type(required_task_outputs)}. '
                'It should be RequiredTaskOutput, dict, or iterable of RequiredTaskOutput.'
            )

    @staticmethod
    def _merge_custom_labels_and_task_params_labels(
        normalized_labels_list: list[dict[str, str]],
    ) -> dict[str, str]:
        def __merge_two_dicts_helper(merged: dict[str, str], current_labels: dict[str, str]) -> dict[str, str]:
            next_merged = copy.deepcopy(merged)
            for label_name, label_value in current_labels.items():
                if len(label_value) == 0:
                    logger.warning(f'value of label_name={label_name} is empty. So skip to add as a metadata.')
                    continue
                if label_name in next_merged:
                    logger.warning(f'label_name={label_name} is already seen. So skip to add as metadata.')
                    continue
                next_merged[label_name] = label_value
            return next_merged

        return functools.reduce(__merge_two_dicts_helper, normalized_labels_list, {})

    # Google Cloud Storage(GCS) has a limitation of metadata size, 8 KiB.
    # So, we need to adjust the size of metadata.
    @staticmethod
    def _adjust_gcs_metadata_limit_size(_labels: dict[str, str]) -> dict[str, str]:
        def _get_label_size(label_name: str, label_value: str) -> int:
            return len(label_name.encode('utf-8')) + len(label_value.encode('utf-8'))

        labels = copy.deepcopy(_labels)
        max_gcs_metadata_size, current_total_metadata_size = (
            GCSObjectMetadataClient.MAX_GCS_METADATA_SIZE,
            sum(_get_label_size(label_name, label_value) for label_name, label_value in labels.items()),
        )
        if current_total_metadata_size <= max_gcs_metadata_size:
            return labels
        # NOTE: remove labels to stay within max metadata size.
        to_remove = []
        for label_name, label_value in reversed(tuple(labels.items())):
            size = _get_label_size(label_name, label_value)
            to_remove.append(label_name)
            current_total_metadata_size -= size
            if current_total_metadata_size <= max_gcs_metadata_size:
                break

        for key in to_remove:
            del labels[key]
        return labels
