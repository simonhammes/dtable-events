from seaserv import seafile_api
from dtable_events.utils.dtable_storage_server_api import storage_api

try:
    from seahub.settings import ENABLE_DTABLE_STORAGE_SERVER
except ImportError as err:
    ENABLE_DTABLE_STORAGE_SERVER = False


class StorageBackend(object):
    """Storage Backend
    """

    def __init__(self):
        pass

    def create_empty_dtable(self, dtable_uuid, username, in_storage, repo_id, dtable_file_name):
        if ENABLE_DTABLE_STORAGE_SERVER and in_storage:
            return storage_api.create_empty_dtable(dtable_uuid)
        else:
            return seafile_api.post_empty_file(repo_id, '/', dtable_file_name, username)

    def save_dtable(self, dtable_uuid, json_string, username, in_storage, repo_id, dtable_path, dtable_file_name):
        if ENABLE_DTABLE_STORAGE_SERVER and in_storage:
            return storage_api.save_dtable(dtable_uuid, json_string)
        else:
            return seafile_api.post_file(repo_id, dtable_path, '/', dtable_file_name, username)


storage_backend = StorageBackend()
