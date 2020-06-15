import json
import requests
import os
import time
import logging
import io
import operator
from queue import Queue, Empty
from threading import Thread, Event, Lock
from zipfile import ZipFile, is_zipfile

from django.utils.http import urlquote
from seaserv import seafile_api

from dtable_events.dtable_io.task_manager import task_manager

# this two prefix used in exported zip file
FILE_URL_PREFIX = 'file://dtable-bundle/asset/files/'
IMG_URL_PREFIX = 'file://dtable-bundle/asset/images/'


def setup_logger(name):
    """
    setup logger for dtable io
    """
    logdir = os.path.join(os.environ.get('DTABLE_EVENTS_LOG_DIR', ''))
    log_file = os.path.join(logdir, 'dtable_events_io.log')
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s, %(module)s.%(funcName)s](%(name)s)[Line:%(lineno)d] %(message)s')
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.addHandler(handler)

    return logger


def gen_inner_file_get_url(token, filename):
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    return '%s/files/%s/%s' % (INNER_FILE_SERVER_ROOT, token,
                               urlquote(filename))


def gen_dir_zip_download_url(token):
    """
    Generate fileserver file url.
    Format: http://<domain:port>/files/<token>/<filename>
    """
    FILE_SERVER_PORT = task_manager.conf['file_server_port']
    INNER_FILE_SERVER_ROOT = 'http://127.0.0.1:' + str(FILE_SERVER_PORT)
    return '%s/zip/%s' % (INNER_FILE_SERVER_ROOT, token)


def convert_dtable_export_file_and_image_url(dtable_content):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded
    """

    tables = dtable_content.get('tables', [])
    for table in tables:
        rows = table.get('rows', [])
        for row in rows:
            for k, v in row.items():
                if isinstance(v, list):
                    for idx, item in enumerate(v):
                        # case1: image
                        if isinstance(item, str) and 'http' in item and 'images' in item:
                            img_name = '/'.join(item.split('/')[-2:])  # e.g. "2020-01/WeWork%20gg.png"
                            v[idx] = IMG_URL_PREFIX + img_name
                        # case2: file
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if k == 'url':
                                    file_name = '/'.join(v.split('/')[-2:]) # e.g. 2020-01/README.md
                                    item[k] = FILE_URL_PREFIX + file_name
    return dtable_content


def prepare_dtable_json(repo_id, dtable_uuid, table_name, dtable_file_dir_id):
    """
    used in export dtable
    create dtable json file at /tmp/dtable-io/<dtable_uuid>/dtable_asset/content.json,
    so that we can zip /tmp/dtable-io/<dtable_uuid>/dtable_asset

    :param repo_id:            repo of this dtable
    :param table_name:         name of dtable
    :param dtable_file_dir_id: xxx.dtable's file dir id
    :return:                   file stream
    """
    try:
        token = seafile_api.get_fileserver_access_token(
            repo_id, dtable_file_dir_id, 'download', '', use_onetime=False
        )
    except Exception as e:
        raise e

    json_url = gen_inner_file_get_url(token, table_name + '.dtable')
    content_json = requests.get(json_url).content
    if content_json:
        dtable_content = convert_dtable_export_file_and_image_url(json.loads(content_json))
    else:
        dtable_content = ''
    content_json = json.dumps(dtable_content).encode('utf-8')
    path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_asset', 'content.json')

    with open(path, 'wb') as f:
       f.write(content_json)


def prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id):
    """
    used in export dtable
    create asset folder at /tmp/dtable-io/<dtable_uuid>/dtable_asset
    notice that create_dtable_json and this function create file at same directory

    1. get asset zip from file_server
    2. unzip it at /tmp/dtable-io/<dtable_uuid>/dtable_asset/

    :param username:
    :param repo_id:
    :param asset_dir_id:
    :return:
    """

    # get file server access token
    fake_obj_id = {
        'obj_id': asset_dir_id,
        'dir_name': 'asset',        # after download and zip, folder root name is asset
        'is_windows': 0
    }
    try:
        token = seafile_api.get_fileserver_access_token(
            repo_id, json.dumps(fake_obj_id), 'download-dir', username, use_onetime=False
    )
    except Exception as e:
        raise e

    progress = {'zipped': 0, 'total': 1}
    while progress['zipped'] != progress['total']:
        time.sleep(0.5)   # sleep 0.5 second
        try:
            progress = json.loads(seafile_api.query_zip_progress(token))
        except Exception:
            raise Exception

    asset_url = gen_dir_zip_download_url(token)
    resp = requests.get(asset_url)
    file_obj = io.BytesIO(resp.content)
    if is_zipfile(file_obj):
        with ZipFile(file_obj) as zp:
            zp.extractall(os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_asset'))


def convert_dtable_import_file_and_image_url(dtable_content, workspace_id, dtable_uuid):
    """ notice that this function receive a python dict and return a python dict
        json related operations are excluded

    :param dtable_content: python dict
    :param workspace_id:
    :param dtable_uuid:
    :return:  python dict
    """
    tables = dtable_content.get('tables', [])

    # handle different url in settings.py
    dtable_web_service_url = task_manager.conf['dtable_web_service_url'].rstrip('/')

    for table in tables:
        rows = table.get('rows', [])
        for idx, row in enumerate(rows):
            for k, v in row.items():
                if isinstance(v, list):
                    for idx, item in enumerate(v):
                        # case1: image
                        if isinstance(item, str) and item.startswith(IMG_URL_PREFIX):
                            img_name = '/'.join(item.split('/')[-2:])  # e.g. "2020-01/WeWork%20gg.png"
                            new_url = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                               dtable_uuid, 'images', img_name])
                            v[idx] = new_url
                        # case2: file
                        if isinstance(item, dict):
                            for k, v in item.items():
                                if k == 'url' and v.startswith(FILE_URL_PREFIX):
                                    file_name = '/'.join(v.split('/')[-2:]) # e.g. 2020-01/README.md
                                    new_url = '/'.join([dtable_web_service_url, 'workspace', workspace_id, 'asset',
                                                       dtable_uuid, 'files', file_name])

                                    item[k] = new_url
    return dtable_content


def post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name):
    """
    used to import dtable
    prepare dtable json file and post it at file server

    :param repo_id:
    :param workspace_id:
    :param dtable_uuid:         str
    :param dtable_file_name:    xxx.dtable, the name of zip we imported
    :return:
    """
    # change url in content json, then save it at file server
    content_json_file_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/', 'content.json')
    with open(content_json_file_path, 'r') as f:
        content_json = f.read()
    content_json = convert_dtable_import_file_and_image_url(json.loads(content_json), workspace_id, dtable_uuid)
    with open(content_json_file_path, 'w') as f:
        f.write(json.dumps(content_json))

    try:
        seafile_api.post_file(repo_id, content_json_file_path, '/', dtable_file_name, username)
    except Exception as e:
        raise e


def post_asset_files(repo_id, dtable_uuid, username):
    """
    used to import dtable
    post asset files in  /tmp/dtable-io/<dtable_uuid>/dtable_zip_extracted/ to file server

    :return:
    """
    asset_root_path = os.path.join('/asset', dtable_uuid)

    tmp_extracted_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/')
    for root, dirs, files in os.walk(tmp_extracted_path):
        for file_name in files:
            if file_name == 'content.json':
                continue
            inner_path = root[len(tmp_extracted_path)+6:]  # path inside zip
            tmp_file_path = os.path.join(root, file_name)
            cur_file_parent_path = os.path.join(asset_root_path, inner_path)
            # check current file's parent path before post file
            path_id = seafile_api.get_dir_id_by_path(repo_id, cur_file_parent_path)
            if not path_id:
                seafile_api.mkdir_with_parents(repo_id, '/', cur_file_parent_path[1:], username)

            seafile_api.post_file(repo_id, tmp_file_path, cur_file_parent_path, file_name, username)


def download_files_to_path(username, repo_id, dtable_uuid, files, path):
    """
    download dtable's asset files to path
    """
    # download files by groups in which files have common path in multi threads
    # use groups and threads for high performance
    # grouping algorithm perhaps not perfect, gradually improve it
    files = [f.split('/') for f in files]
    base_path = os.path.join('/asset', dtable_uuid)
    new_files = []
    # remove invalid file
    for file in files:
        full_dirent_path = os.path.join(base_path, os.path.join(*file))
        if seafile_api.get_dirent_by_path(repo_id, full_dirent_path):
            new_files.append(file)
    files = new_files
    files = sorted(files)
    groups = [[files[0]]]
    for f in files[1:]:
        g = groups[-1]
        # if such file path == last group file path append it or new group
        if operator.eq(g[0][:-1], f[:-1]):
            groups[-1].append(f)
        else:
            groups.append([f])

    group_queue, query_queue = Queue(), Queue()
    task_event, error_lock, error = Event(), Lock(), None
    count, count_lock = 0, Lock()
    # put group to queue
    [group_queue.put(group) for group in groups]

    # gen zip token func
    def _zip_token():
        nonlocal error
        while not task_event.is_set():
            try:
                group = group_queue.get(timeout=0.5)
            except Empty:
                continue
            parent_dir = os.path.join(base_path, os.path.join(*group[0][:-1]))
            fake_obj_id = {
                'parent_dir': parent_dir,
                'file_list': [f[-1] for f in group],
                'is_windows': False
            }
            try:
                zip_token = seafile_api.get_fileserver_access_token(
                    repo_id, json.dumps(fake_obj_id), 'download-multi', username,
                    use_onetime=False
                )
                query_queue.put(zip_token)
            except Exception as e:
                # record error and stop loop to finish thread
                with error_lock:
                    if not error and not task_event.is_set():
                        error = e
                        task_event.set()

    # query token download and extract func
    def _query_token():
        nonlocal count, error
        while not task_event.is_set():
            try:
                zip_token = query_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                progress = json.loads(seafile_api.query_zip_progress(zip_token))
            except Exception as e:
                # record error and stop loop to finish thread
                with error_lock:
                    if not error and not task_event.is_set():
                        error = e
                        task_event.set()
                        break
            finally:
                # avoid unexpected error
                time.sleep(0.2)
            if progress['zipped'] != progress['total']:
                query_queue.put(zip_token)
                continue

            # download zip
            asset_url = gen_dir_zip_download_url(zip_token)
            try:
                resp = requests.get(asset_url)
            except Exception as e:
                # record error and stop loop to finish thread
                with error_lock:
                    if not error and not task_event.is_set():
                        error = e
                        task_event.set()
                        break

            # extract zip to path and count success num if all groups down set event to finish thread
            file_obj = io.BytesIO(resp.content)
            if is_zipfile(file_obj):
                with ZipFile(file_obj) as zp:
                    zp.extractall(path)
                with count_lock:
                    count += 1
                    if count == len(groups):
                        task_event.set()

    # threads to gen zip-token
    tds = [Thread(target=_zip_token) for _ in range(2)]
    # threads to query zip-token download and extract
    tds.extend([Thread(target=_query_token) for _ in range(2)])
    [t.start() for t in tds]
    # main process loops till event done
    while not task_event.is_set():
        time.sleep(0.5)
    # raise error in threads
    if error:
        raise error
