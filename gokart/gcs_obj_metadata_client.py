from __future__ import annotations

import copy
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, Union

import luigi
from googleapiclient.model import makepatch

from gokart.gcs_config import GCSConfig

logger = getLogger(__name__)


class GCSObjectMetadataClient(object):
    """
    This class is Utility-Class, so should not be initialized.
    """

    @staticmethod
    def add_task_state_labels(
        path: str,
        params: Optional[List[Tuple[str, Any, luigi.Parameter]]] = None,
    ) -> None:
        # In gokart/object_storage.get_time_stamp, could find same call.
        # _path_to_bucket_and_key is a private method, so, this might not be acceptable.
        bucket, obj = GCSConfig().get_gcs_client()._path_to_bucket_and_key(path)

        _response = GCSConfig().get_gcs_client().client.objects().get(bucket=bucket, object=obj).execute()
        if _response is None:
            logger.error(f'failed to get object from GCS bucket {bucket} and object {obj}.')
            return

        response: Dict[str, Any] = dict(_response)
        original_metadata: Dict[Any, Any] = {}
        if 'metadata' in response.keys():
            _metadata = response.get('metadata')
            if _metadata is not None:
                original_metadata = dict(_metadata)

        patched_metadata = GCSObjectMetadataClient._get_patched_obj_metadata(
            copy.deepcopy(original_metadata),
            params,
        )

        if not original_metadata == patched_metadata:
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
    def _get_patched_obj_metadata(
        metadata: Any,
        params: Optional[List[Tuple[str, Any, luigi.Parameter]]] = None,
    ) -> Union[Dict, Any]:
        # If metadata from response when getting bucket and object information is not dictionary,
        # something wrong might be happened, so return original metadata, no patched.
        if not isinstance(metadata, dict):
            logger.warning(f'metadata is not a dict: {metadata}, something wrong was happened when getting response when get bucket and object information.')
            return metadata

        # Maximum size of metadata for each object is 8KiB.
        # [Link]: https://cloud.google.com/storage/quotas?hl=ja#objects
        # And also, user_provided_labels should be prioritized rather than auto generated labels.
        # So, at first, attach user_provided_labels, then add auto generated labels.
        max_gcs_metadata_size, total_metadata_size, labels = 8 * 1024, 0, []
        if params:
            all_labels = GCSObjectMetadataClient._merge_with_user_provided_labels(params)
            for label_name, label_value in all_labels:
                size = len(str(label_name).encode('utf-8')) + len(str(label_value).encode('utf-8'))
                if total_metadata_size + size > max_gcs_metadata_size:
                    logger.warning(f'current metadata total size is {total_metadata_size} byte, and no more labels would be added.')
                    break
                total_metadata_size += size
                labels.append((label_name, str(label_value)))

        patched_metadata = dict(metadata)
        for label_key, label_value in labels:
            patched_metadata[label_key] = label_value
        return patched_metadata

    @staticmethod
    def _merge_with_user_provided_labels(
        params: List[Tuple[str, Any, luigi.Parameter]],
    ) -> List[Tuple[Any, Any]]:
        # luigi.Parameter.get_param_names() returns significant parameters only.
        # [Link]: https://luigi.readthedocs.io/en/latest/_modules/luigi/task.html#Task.get_param_names
        parameter_labels, parameter_key_set = [], set()

        for param_name, param_value, param_obj in params:
            if param_name == 'user_provided_gcs_labels':
                continue
            if param_value is None:
                logger.warning(f'param_name={param_name} param_obj={param_obj} param_value is None. So skipped.')
                continue
            parameter_labels.append((param_name, param_value)) if param_obj.significant else None
            parameter_key_set.add(param_name)
        # Make user_provided override parameter when their key conflict.
        # Log a warning message when this happens.
        for param_name, param_value, _ in params:
            if param_name == 'user_provided_gcs_labels':
                user_provided_labels = dict(param_value)
                for up_key, up_value in user_provided_labels.items():
                    if up_key in parameter_key_set:
                        logger.warning(f"{up_key} is already in parameter's name set. Override it's value with {up_value} from user provided labels.")
                    parameter_labels.append((up_key, up_value))
        return parameter_labels
