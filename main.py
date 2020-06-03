import base64
import json
import logging
import os
import tempfile
from urllib.parse import urlparse

import requests
from google.cloud import error_reporting
from google.cloud import storage
from mega import Mega

_LOGGER = None
_ERROR_REPORTING_CLIENT = None
_MEGA_CLIENT = None


def _get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER:
        return _LOGGER
    log_level: str = os.getenv("LOG_LEVEL") or "INFO"
    logging.basicConfig(level=logging._nameToLevel[log_level])
    _LOGGER = logging.getLogger()
    return _LOGGER


_get_logger().info('Loading module.')


def _get_mega_client():
    global _MEGA_CLIENT
    if _MEGA_CLIENT is not None:
        return _MEGA_CLIENT
    _MEGA_CLIENT = Mega()
    try:
        vars = _get_vars_dict()
    except Exception as ex:
        _get_logger().error(f"Failed to get function's info from GCP Storage due to: {ex}")
        raise ex

    try:
        _MEGA_CLIENT.login(vars['USERNAME'], vars['PASSWORD'])
        _get_logger().info('Logged into Mega.')
    except Exception as ex:
        _get_logger().error(f"Failed to log into Mega with username {vars['USERNAME']} due to: {ex}")
        raise ex

    return _MEGA_CLIENT


def _get_error_reporting_client() -> error_reporting.Client:
    global _ERROR_REPORTING_CLIENT
    if _ERROR_REPORTING_CLIENT:
        return _ERROR_REPORTING_CLIENT
    _ERROR_REPORTING_CLIENT = error_reporting.Client()
    return _ERROR_REPORTING_CLIENT


def _get_vars_dict() -> dict:
    """
    Get sensitive data dict.
    """
    storage_client = storage.Client()
    blob = storage_client.get_bucket(os.getenv('VARS_BUCKET')).get_blob(os.getenv('VARS_BLOB'))

    if blob is None:
        msg = f"Failed to get info from blob '{os.getenv('VARS_BLOB')} on the '{os.getenv('VARS_BUCKET')}' bucket."
        raise Exception(msg)

    return json.loads(blob.download_as_string())


def _get_user(mega_client: Mega) -> float:
    try:
        return mega_client.get_user()
    except Exception as ex:
        _get_logger().warning(f"Unable to get Mega account username due to: {ex}", exc_info=True)
        raise ex


def upload_file_by_url_to_mega(event, context) -> int:
    """
    >>> from main import upload_file_by_url_to_mega
    >>> json_payload = json.dumps({
    ...     "url": 'https://i.redd.it/akkzmel1xpf41.jpg',
    ...     "folder": "mtga-promo-cards"
    ... })
    >>> upload_file_by_url_to_mega({"data": base64.b64encode(bytes(json_payload, 'utf-8'))}, {})
    0
    """
    # event must exist and contain a data property.
    try:
        if event['data'] is None:
            msg = f"Cannot find the data object in event. Got: {event['data']}"
            _get_logger().error(msg)
            _get_error_reporting_client().report(msg)
            return 1
        _get_logger().info(f"Event {event}")
    except Exception:
        _get_logger().error(f"Event has no property 'data'. Gotten event: {event}", exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    try:
        data_str = event['data']
    except Exception as ex:
        _get_logger().error(f"Failed to decode b64 data to utf-8 due to: {ex} ", exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    # event data comes in b64 and is converted to an utf-8 string.
    try:
        data = json.loads(base64.b64decode(data_str).decode('utf-8'))
    except Exception as ex:
        _get_logger().error(f"Unable to load data json string to dict due to: {ex}", exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    # data object must have an url and a folder properties.
    if "url" not in data:
        msg = "No file URL in event payload to upload."
        _get_logger().error(msg, exc_info=True)
        _get_error_reporting_client().report(msg)
        return 1
    if "folder" not in data:
        msg = "No folder in event payload to upload file too, uploading to root folder."
        _get_logger().warning(msg)

    # file needs fetching
    name = os.path.basename(urlparse(data['url']).path)
    response = requests.get(data['url'])
    path = ''
    if response.status_code == 200:
        fh = tempfile.NamedTemporaryFile('wb')
        fh.write(response.content)
        path = fh.name
        fh.close()
        _get_logger().info(f"Success in downloading file at url {data['url']}.")
    else:
        msg = f"Failed to download file at url {data['url']}. Status code: {response.status_code}."
        _get_logger().error(msg, exc_info=True)
        _get_error_reporting_client().report(msg)
        return 1

    _get_logger().info(path)
    # the folder passed is created if it does not exists.
    try:
        folder = None
        if 'folder' in data:
            folder_str = _get_mega_client().find_path_descriptor(data['folder'])
            if folder_str is None:
                _get_mega_client().create_folder(data['folder'])
                folder = _get_mega_client().find(data['folder'])
                _get_logger().warning(
                    f"Folder {data['folder']} not found in Mega account to upload the file to and was created."
                )
            else:
                _get_logger().debug(f"Folder name: {data['folder']}")
                folder = _get_mega_client().find(data['folder'])
    except Exception as ex:
        _get_logger().error(f"Failed to find or create folder '{data['folder']}' due to: {ex}", exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    # Upload to Mega.
    try:
        if folder:
            _get_mega_client().upload(path, folder[0])
        else:
            _get_mega_client().upload(path)
    except Exception as ex:
        _get_logger().error(f"Failed to upload file named '{name}' to Mega account.", exc_info=True)
        _get_error_reporting_client().report_exception()
        return 1

    # Rename to original name.
    file_on_mega = _get_mega_client().find(os.path.basename(path))
    _get_mega_client().rename(file_on_mega, name)
    _get_logger().info(f"Finished uploading file to Mega account.")
    return 0
