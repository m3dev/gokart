from __future__ import annotations

import copy
import re
from logging import getLogger
from typing import Any, Optional, Union
from urllib.parse import urlsplit

from googleapiclient.model import makepatch

from gokart.gcs_config import GCSConfig

logger = getLogger(__name__)


class GCSObjectMetadataClient:
    """
    This class is Utility-Class, so should not be initialized.
    This class used for adding metadata as labels.
    """

    @staticmethod
    def _is_log_related_path(path: str) -> bool:
        return re.match(r'^log/(processing_time/|task_info/|task_log/|module_versions/|random_seed/|task_params/).+', path) is not None

    # This is the copied method of luigi.gcs._path_to_bucket_and_key(path).
    @staticmethod
    def _path_to_bucket_and_key(path: str) -> tuple[str, str]:
        (scheme, netloc, path, _, _) = urlsplit(path)
        assert scheme == 'gs'
        path_without_initial_slash = path[1:]
        return netloc, path_without_initial_slash

    @staticmethod
    def add_task_state_labels(path: str, task_params: Optional[dict[str, str]] = None, custom_labels: Optional[dict[str, Any]] = None) -> None:
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
    def _normalize_labels(labels: Optional[dict[str, Any]]) -> dict[str, str]:
        return {str(key): str(value) for key, value in labels.items()} if labels else {}

    @staticmethod
    def _get_patched_obj_metadata(
        metadata: Any,
        task_params: Optional[dict[str, str]] = None,
        custom_labels: Optional[dict[str, Any]] = None,
    ) -> Union[dict, Any]:
        # If metadata from response when getting bucket and object information is not dictionary,
        # something wrong might be happened, so return original metadata, no patched.
        if not isinstance(metadata, dict):
            logger.warning(f'metadata is not a dict: {metadata}, something wrong was happened when getting response when get bucket and object information.')
            return metadata

        if not task_params and not custom_labels:
            return metadata
        # Maximum size of metadata for each object is 8 KiB.
        # [Link]: https://cloud.google.com/storage/quotas#objects
        normalized_task_params_labels = GCSObjectMetadataClient._normalize_labels(task_params)
        normalized_custom_labels = GCSObjectMetadataClient._normalize_labels(custom_labels)
        # There is a possibility that the keys of user-provided labels(custom_labels) may conflict with those generated from task parameters (task_params_labels).
        # However, users who utilize custom_labels are no longer expected to search using the labels generated from task parameters.
        # Instead, users are expected to search using the labels they provided.
        # Therefore, in the event of a key conflict, the value registered by the user-provided labels will take precedence.
        _merged_labels = GCSObjectMetadataClient._merge_custom_labels_and_task_params_labels(normalized_task_params_labels, normalized_custom_labels)
        return dict(metadata) | dict(GCSObjectMetadataClient._adjust_gcs_metadata_limit_size(_merged_labels))

    @staticmethod
    def _merge_custom_labels_and_task_params_labels(
        normalized_task_params: dict[str, str],
        normalized_custom_labels: dict[str, Any],
    ) -> dict[str, str]:
        merged_labels = copy.deepcopy(normalized_custom_labels)
        for label_name, label_value in normalized_task_params.items():
            if len(label_value) == 0:
                logger.warning(f'value of label_name={label_name} is empty. So skip to add as a metadata.')
                continue
            if label_name in merged_labels.keys():
                logger.warning(f'label_name={label_name} is already seen. So skip to add as a metadata.')
                continue
            merged_labels[label_name] = label_value
        return merged_labels

    # Google Cloud Storage(GCS) has a limitation of metadata size, 8 KiB.
    # So, we need to adjust the size of metadata.
    @staticmethod
    def _adjust_gcs_metadata_limit_size(_labels: dict[str, str]) -> dict[str, str]:
        def _get_label_size(label_name: str, label_value: str) -> int:
            return len(label_name.encode('utf-8')) + len(label_value.encode('utf-8'))

        labels = copy.deepcopy(_labels)
        max_gcs_metadata_size, current_total_metadata_size = (
            8 * 1024,
            sum(_get_label_size(label_name, label_value) for label_name, label_value in labels.items()),
        )

        if current_total_metadata_size <= max_gcs_metadata_size:
            return labels

        for label_name, label_value in reversed(labels.items()):
            size = _get_label_size(label_name, label_value)
            del labels[label_name]
            current_total_metadata_size -= size
            if current_total_metadata_size <= max_gcs_metadata_size:
                break
        return labels
