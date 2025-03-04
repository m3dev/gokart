from __future__ import annotations

import copy
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
        return (
            ('log/random_seed' in path)
            or ('log/processing_time' in path)
            or ('log/task_params' in path)
            or ('log/task_log' in path)
            or ('log/module_versions' in path)
            or ('log/task_info' in path)
        )

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
        max_gcs_metadata_size, total_metadata_size = 8 * 1024, 0
        # There is a possibility that the keys of user-provided labels(custom_labels) may conflict with those generated from task parameters (task_params_labels).
        # However, users who utilize custom_labels are no longer expected to search using the labels generated from task parameters.
        # Instead, users are expected to search using the labels they provided.
        # Therefore, in the event of a key conflict, the value registered by the user-provided labels will take precedence.
        total_metadata_size, labels = GCSObjectMetadataClient._add_labels_to_metadata(normalized_custom_labels, total_metadata_size, max_gcs_metadata_size)
        _, labels = GCSObjectMetadataClient._add_labels_to_metadata(normalized_task_params_labels, total_metadata_size, max_gcs_metadata_size)
        return dict(metadata) | dict(labels)

    @staticmethod
    def _add_labels_to_metadata(
        labels_dict: dict[str, str],
        total_metadata_size: int,
        max_gcs_metadata_size: int,
        labels: Optional[list[tuple[str, str]]]=None,
        has_seen_keys: Optional[set[str]]=None
    ) -> tuple[int, list[tuple[str, str]]]:
        labels = copy.copy(labels) if labels else []
        has_seen_keys = copy.copy(has_seen_keys) if has_seen_keys else set({})
        for label_name, label_value in labels_dict.items():
            if len(label_value) == 0:
                logger.warning(f'value of label_name={label_name} is empty. So skip to add as a metadata.')
                continue
            size = len(str(label_name).encode('utf-8')) + len(str(label_value).encode('utf-8'))
            if total_metadata_size + size > max_gcs_metadata_size:
                logger.warning(f'current metadata total size is {total_metadata_size} byte, and no more labels would be added.')
                break
            if label_name in has_seen_keys:
                logger.warning(f'label_name={label_name} is seen. So skip to add as a metadata.')
                continue
            total_metadata_size += size
            labels.append((label_name, label_value))
            has_seen_keys.add(label_name)
        return total_metadata_size, labels
