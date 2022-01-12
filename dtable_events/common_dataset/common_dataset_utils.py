# -*- coding: utf-8 -*-
import logging
import time
import os
from datetime import datetime, timedelta
import requests
import jwt
import sys
from dtable_events.utils import uuid_str_to_36_chars
from dtable_events.dtable_io.utils import convert_db_rows, generate_synced_columns, generate_synced_rows

logger = logging.getLogger(__name__)

# DTABLE_WEB_DIR
dtable_web_dir = os.environ.get('DTABLE_WEB_DIR', '')
if not dtable_web_dir:
    logging.critical('dtable_web_dir is not set')
    raise RuntimeError('dtable_web_dir is not set')
if not os.path.exists(dtable_web_dir):
    logging.critical('dtable_web_dir %s does not exist' % dtable_web_dir)
    raise RuntimeError('dtable_web_dir does not exist.')

sys.path.insert(0, dtable_web_dir)

try:
    import seahub.settings as seahub_settings
    DTABLE_PRIVATE_KEY = getattr(seahub_settings, 'DTABLE_PRIVATE_KEY')
    DTABLE_SERVER_URL = getattr(seahub_settings, 'DTABLE_SERVER_URL')
    ENABLE_DTABLE_SERVER_CLUSTER = getattr(seahub_settings, 'ENABLE_DTABLE_SERVER_CLUSTER', False)
    DTABLE_PROXY_SERVER_URL = getattr(seahub_settings, 'DTABLE_PROXY_SERVER_URL', '')
    SESSION_COOKIE_NAME = getattr(seahub_settings, 'SESSION_COOKIE_NAME', 'sessionid')
except ImportError as e:
    logger.critical("Can not import dtable_web settings: %s." % e)
    raise RuntimeError("Can not import dtable_web settings: %s" % e)


def list_synchronizing_common_dataset(db_session):
    sql = '''
            SELECT b.dst_dtable_uuid,b.dst_table_id,a.table_id as src_table_id,a.view_id as src_view_id,
                a.dtable_uuid as src_dtable_uuid, b.id as sync_id, b.src_version
            FROM dtable_common_dataset a 
            INNER JOIN dtable_common_dataset_sync b ON b.dataset_id=a.id
            INNER JOIN dtables c ON a.dtable_uuid=c.uuid and c.deleted=0
            INNER JOIN dtables d ON b.dst_dtable_uuid=d.uuid AND d.deleted=0
            WHERE last_sync_time<:per_day_check_time
        '''

    per_day_check_time = datetime.utcnow() - timedelta(hours=23)
    dataset_list = db_session.execute(sql, {
        'per_day_check_time': per_day_check_time,
    })
    return dataset_list


def update_sync_time_and_version(db_session, update_map):
    """
    :param update_map: {dataset_id:src_version,dataset_id1:src_version1}
    """
    src_version_sql = ""
    for dataset_id, src_version in update_map.items():
        sql_str = " WHEN " + str(dataset_id) + " THEN " + str(src_version)
        src_version_sql += sql_str

    sql = "UPDATE dtable_common_dataset_sync SET last_sync_time=NOW(), src_version=CASE id" \
          + src_version_sql + " END WHERE id IN :dataset_id_list"

    dataset_id_list = [dataset for dataset in update_map]
    db_session.execute(sql, {'dataset_id_list': dataset_id_list})
    db_session.commit()


def check_common_dataset(db_session):
    sync_dataset_list = list_synchronizing_common_dataset(db_session)
    dataset_update_map = {}
    sync_count = 0
    for dataset in sync_dataset_list:
        dst_dtable_uuid = uuid_str_to_36_chars(dataset[0])
        dst_table_id = dataset[1]
        src_table_id = dataset[2]
        src_view_id = dataset[3]
        src_dtable_uuid = uuid_str_to_36_chars(dataset[4])
        dataset_id = dataset[5]
        src_version = dataset[6]

        assets = gen_src_dst_assets(dst_dtable_uuid, src_dtable_uuid, src_table_id, src_view_id, dst_table_id)

        if not assets:
            continue

        dst_headers = assets.get('dst_headers')
        src_table = assets.get('src_table')
        src_view = assets.get('src_view')
        src_columns = assets.get('src_columns')
        src_headers = assets.get('src_headers')
        dst_columns = assets.get('dst_columns')
        dst_rows = assets.get('dst_rows')
        dst_table_name = assets.get('dst_table_name')
        dtable_src_version = assets.get('src_version')

        if dtable_src_version == src_version:
            continue

        try:
            dst_table_id, error_msg = sync_common_dataset({
                'dst_dtable_uuid': dst_dtable_uuid,
                'src_dtable_uuid': src_dtable_uuid,
                'src_rows': src_table.get('rows', []),
                'src_columns': src_columns,
                'src_table_name': src_table.get('name'),
                'src_view_name': src_view.get('name'),
                'src_headers': src_headers,
                'dst_table_id': dst_table_id,
                'dst_table_name': dst_table_name,
                'dst_headers': dst_headers,
                'dst_columns': dst_columns,
                'dst_rows': dst_rows
            })
            if error_msg:
                logging.error(error_msg)
                continue
        except Exception as e:
            logging.error('sync common dataset error: %s', e)
            return

        dataset_update_map[dataset_id] = dtable_src_version
        sync_count += 1

        if sync_count == 1000:
            try:
                update_sync_time_and_version(db_session, dataset_update_map)
            except Exception as e:
                logging.error(f'update sync time and src_version failed, error: {e}')
            dataset_update_map = {}
            sync_count = 0

    if dataset_update_map:
        try:
            update_sync_time_and_version(db_session, dataset_update_map)
        except Exception as e:
            logging.error(f'update sync time and src_version failed, error: {e}')


def get_dtable_server_header(dtable_uuid):
    try:
        access_token = jwt.encode({
            'dtable_uuid': dtable_uuid,
            'username': 'dtable-events',
            'permission': 'rw',
            'exp': int(time.time()) + 60
        },
            DTABLE_PRIVATE_KEY,
            algorithm='HS256'
        )
    except Exception as e:
        logger.error(e)
        return
    return {'Authorization': 'Token ' + access_token}


def gen_src_dst_assets(dst_dtable_uuid, src_dtable_uuid, src_table_id, src_view_id, dst_table_id):
    """
    return assets -> dict or None, error -> api_error or None
    """
    dst_headers = get_dtable_server_header(dst_dtable_uuid)
    src_headers = get_dtable_server_header(src_dtable_uuid)

    # request src_dtable
    dtable_server_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    url = dtable_server_url.strip('/') + '/dtables/' + src_dtable_uuid + '?from=dtable_events'

    try:
        resp = requests.get(url, headers=src_headers)
        src_dtable_json = resp.json()
    except Exception as e:
        logger.error('request src dtable: %s error: %s', src_dtable_uuid, e)
        return

    # check src_table src_view
    src_table = None
    for table in src_dtable_json.get('tables', []):
        if table.get('_id') == src_table_id:
            src_table = table
            break
    if not src_table:
        logging.error('Source table not found.')
        return

    src_view = None
    if src_view_id:
        for view in src_table.get('views', []):
            if view.get('_id') == src_view_id:
                src_view = view
                break
        if not src_view:
            logging.error('Source view not found.')
            return
    else:
        views = src_table.get('views', [])
        if not views or not isinstance(views, list):
            logging.error('No views found.')
            return
        src_view = views[0]

    # get src columns
    src_view_hidden_columns = src_view.get('hidden_columns', [])
    if not src_view_hidden_columns:
        src_columns = src_table.get('columns', [])
    else:
        src_columns = [col for col in src_table.get('columns', []) if col.get('key') not in src_view_hidden_columns]

    # request dst_dtable
    url = dtable_server_url.strip('/') + '/dtables/' + dst_dtable_uuid + '?from=dtable_events'
    try:
        resp = requests.get(url, headers=dst_headers)
        dst_dtable_json = resp.json()
    except Exception as e:
        logging.error('request dst dtable: %s error: %s', dst_dtable_uuid, e)
        return

    # check dst_table
    dst_table = None
    for table in dst_dtable_json.get('tables', []):
        if table.get('_id') == dst_table_id:
            dst_table = table
            break
    if not dst_table:
        logging.error('Destination table: %s not found.' % dst_table_id)
        return

    return {
        'dst_headers': dst_headers,
        'src_headers': src_headers,
        'src_table': src_table,
        'src_view': src_view,
        'src_columns': src_columns,
        'dst_columns': dst_table.get('columns'),
        'dst_rows': dst_table.get('rows'),
        'dst_table_name': dst_table.get('name'),
        'src_version': src_dtable_json.get('version')
    }


def sync_common_dataset(context):
    """
    sync common dataset
    return: dst_table_id, error_msg -> str or None
    """

    dst_dtable_uuid = context.get('dst_dtable_uuid')
    src_dtable_uuid = context.get('src_dtable_uuid')
    src_rows = context.get('src_rows')
    src_columns = context.get('src_columns')
    src_table_name = context.get('src_table_name')
    src_view_name = context.get('src_view_name')
    src_headers = context.get('src_headers')
    dst_table_id = context.get('dst_table_id')
    dst_table_name = context.get('dst_table_name')
    dst_headers = context.get('dst_headers')
    dst_columns = context.get('dst_columns')
    dst_rows = context.get('dst_rows')

    # generate cols and rows
    ## generate cols
    to_be_updated_columns, to_be_appended_columns, error = generate_synced_columns(src_columns, dst_columns=dst_columns)
    if error:
        return None, error
    dtable_server_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL
    # get src view-rows
    result_rows = []
    start, limit = 0, 10000

    while True:
        url = dtable_server_url.rstrip('/') + '/api/v1/internal/dtables/' + str(src_dtable_uuid) + '/view-rows/?from=dtable_events'
        query_params = {
            'table_name': src_table_name,
            'view_name': src_view_name,
            'use_dtable_db': True,
            'start': start,
            'limit': limit
        }
        try:
            resp = requests.get(url, headers=src_headers, params=query_params)
            res_json = resp.json()
            archive_rows = res_json.get('rows', [])
            archive_metadata = res_json.get('metadata')
            temp_result_rows = convert_db_rows(archive_metadata, archive_rows)
        except Exception as e:
            return None, 'request src_dtable: %s params: %s view-rows error: %s' % (src_dtable_uuid, query_params, e)
        result_rows.extend(temp_result_rows)
        if not temp_result_rows or len(temp_result_rows) < limit:
            break
        start += limit

    final_columns = (to_be_updated_columns or []) + (to_be_appended_columns or [])
    to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids = generate_synced_rows(result_rows, src_rows, src_columns, final_columns, dst_rows=dst_rows)

    # batch append columns
    if to_be_appended_columns:
        url = dtable_server_url.strip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/batch-append-columns/?from=dtable_events'
        data = {
            'table_id': dst_table_id,
            'columns': [{
                'column_key': col.get('key'),
                'column_name': col.get('name'),
                'column_type': col.get('type'),
                'column_data': col.get('data')
            } for col in to_be_appended_columns]
        }
        try:
            resp = requests.post(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                logger.error('batch append columns to dst dtable: %s, table: %s error status code: %s text: %s', dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
                return None, 'batch append columns to dst dtable: %s, table: %s error status code: %s text: %s' % (dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
        except Exception as e:
            return None, 'batch append columns to dst dtable: %s, table: %s error: %s' % (dst_dtable_uuid, dst_table_id, e)
    ### batch update columns
    if to_be_updated_columns:
        url = dtable_server_url.strip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/batch-update-columns/?from=dtable_events'
        data = {
            'table_id': dst_table_id,
            'columns': [{
                'key': col.get('key'),
                'type': col.get('type'),
                'data': col.get('data')
            } for col in to_be_updated_columns]
        }
        try:
            resp = requests.put(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                logger.error('batch update columns to dst dtable: %s, table: %s error status code: %s text: %s', dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
                return None, 'batch update columns to dst dtable: %s, table: %s error status code: %s text: %s' % (dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
        except Exception as e:
            return None, 'batch update columns to dst dtable: %s, table: %s error: %s' % (dst_dtable_uuid, dst_table_id, e)

    ## update delete append rows step by step
    step = 1000
    ### update rows
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-update-rows/?from=dtable_events' % (str(dst_dtable_uuid),)
    for i in range(0, len(to_be_updated_rows), step):
        updates = []
        for row in to_be_updated_rows[i: i+step]:
            updates.append({
                'row_id': row['_id'],
                'row': row
            })
        data = {
            'table_name': dst_table_name,
            'updates': updates,
            'need_convert_back': False
        }
        try:
            resp = requests.put(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                return None, 'sync dataset update rows dst dtable: %s dst table: %s error status code: %s content: %s' % (dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
        except Exception as e:
            return None, 'sync dataset update rows dst dtable: %s dst table: %s error: %s' % (dst_dtable_uuid, dst_table_name, e)

    ### delete rows
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-delete-rows/?from=dtable_events' % (str(dst_dtable_uuid),)
    for i in range(0, len(to_be_deleted_row_ids), step):
        data = {
            'table_name': dst_table_name,
            'row_ids': to_be_deleted_row_ids[i: i+step]
        }
        try:
            resp = requests.delete(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                return None, 'sync dataset delete rows dst dtable: %s dst table: %s error status code: %s, content: %s' % (dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
        except Exception as e:
            return None, 'sync dataset delete rows dst dtable: %s dst table: %s error: %s' % (dst_dtable_uuid, dst_table_name, e)

    ### append rows
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-append-rows/' % (str(dst_dtable_uuid),)
    for i in range(0, len(to_be_appended_rows), step):
        data = {
            'table_name': dst_table_name,
            'rows': to_be_appended_rows[i: i+step],
            'need_convert_back': False
        }
        try:
            resp = requests.post(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                return None, 'sync dataset append rows dst dtable: %s dst table: %s error status code: %s' % (dst_dtable_uuid, dst_table_name, resp.status_code)
        except Exception as e:
            return None, 'sync dataset append rows dst dtable: %s dst table: %s error: %s' % (dst_dtable_uuid, dst_table_name, e)

    return dst_table_id, None
