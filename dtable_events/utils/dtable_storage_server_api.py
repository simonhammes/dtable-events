import json
import uuid
import requests

TIMEOUT = 60


def uuid_str_to_36_chars(dtable_uuid):
    if len(dtable_uuid) == 32:
        return str(uuid.UUID(dtable_uuid))
    else:
        return dtable_uuid


def parse_response(response):
    if response.status_code >= 400:
        raise ConnectionError(response.status_code, response.text)
    else:
        return response.text


class DTableStorageServerAPI(object):
    """DTable Storage Server API
    """

    def __init__(self, dtable_storage_server_url):
        """
        :param server_url: str
        """
        self.server_url = dtable_storage_server_url.rstrip('/')

    def __str__(self):
        return '<DTable Storage Server API [ %s ]>' % self.server_url

    def get_dtable(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.get(url, timeout=TIMEOUT)
        data = parse_response(response)

        return data

    def empty_dtable(self, dtable_uuid):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.put(url, timeout=TIMEOUT)
        data = parse_response(response)
        return data

    def save_dtable(self, dtable_uuid, json_string):
        dtable_uuid = uuid_str_to_36_chars(dtable_uuid)
        url = self.server_url + '/dtables/' + dtable_uuid
        response = requests.put(url, json=json_string, timeout=TIMEOUT)
        data = parse_response(response)

        return data
