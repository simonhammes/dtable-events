import re
import time
from copy import deepcopy
from datetime import datetime

import jwt
import requests

from dtable_events.db import init_db_session_class
from dtable_events.dtable_io.utils import get_converted_cell_value
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.utils import uuid_str_to_32_chars
from dtable_events.utils.constants import ColumnTypes
from dtable_events.dtable_io import dtable_io_logger


DTABLE_SERVER_URL = task_manager.conf['dtable_server_url']
DTABLE_PRIVATE_KEY = task_manager.conf['dtable_private_key']
DTABLE_PROXY_SERVER_URL = task_manager.conf['dtable_proxy_server_url']
ENABLE_DTABLE_SERVER_CLUSTER = task_manager.conf['dtable_proxy_server_url']
dtable_server_url = DTABLE_PROXY_SERVER_URL if ENABLE_DTABLE_SERVER_CLUSTER else DTABLE_SERVER_URL


def convert_db_rows(metadata, results):
    """ Convert dtable-db rows data to readable rows data

    :param metadata: list
    :param results: list
    :return: list
    """
    converted_results = []
    column_map = {column['key']: column for column in metadata}
    select_map = {}
    for column in metadata:
            column_type = column['type']
            if column_type in ('single-select', 'multiple-select'):
                column_data = column['data']
                if not column_data:
                    continue
                column_key = column['key']
                column_options = column['data']['options']
                select_map[column_key] = {
                    select['id']: select['name'] for select in column_options}

    for result in results:
        item = {}
        for column_key, value in result.items():
            if column_key in column_map:
                column = column_map[column_key]
                column_name = column['name']
                column_type = column['type']
                s_map = select_map.get(column_key)
                if column_type == 'single-select' and value and s_map:
                    item[column_name] = s_map.get(value, value)
                elif column_type == 'multiple-select' and value and s_map:
                    item[column_name] = [s_map.get(s, s) for s in value]
                elif column_type == 'date' and value:
                    try:
                        date_value = datetime.fromisoformat(value)
                        date_format = column['data']['format']
                        if date_format == 'YYYY-MM-DD':
                            value = date_value.strftime('%Y-%m-%d')
                        else:
                            value = date_value.strftime('%Y-%m-%d %H:%M')
                    except Exception as e:
                        pass
                        # dtable_io_logger.error(e)
                    item[column_name] = value
                else:
                    item[column_name] = value
            else:
                item[column_key] = value
        converted_results.append(item)

    return converted_results


def transfer_column(src_column):
    """
    transfer origin column to new target column
    """
    if src_column.get('type') == ColumnTypes.BUTTON:
        return None
    column = deepcopy(src_column)
    if src_column.get('type') == ColumnTypes.FORMULA:
        data = src_column.get('data', {})
        result_type = data.get('result_type', 'string')
        if result_type == 'date':
            column['type'] = ColumnTypes.DATE
            column['data'] = {
                'format': data.get('format', 'YYYY-MM-DD')
            }
        elif result_type == 'number':
            column['type'] = ColumnTypes.NUMBER
            column['data'] = {
                'format': data.get('format', 'number'),
                'precision': data.get('precision', 2),
                'enable_precision': data.get('enable_precision', False),
                'enable_fill_default_value': data.get('enable_fill_default_value', False),
                'decimal': data.get('decimal', 'dot'),
                'thousands': data.get('thousands', 'no'),
                'currency_symbol': data.get('currency_symbol')
            }
        elif result_type == 'bool':
            column['type'] = ColumnTypes.CHECKBOX
            column['data'] = None
        else:
            column['type'] = ColumnTypes.TEXT
            column['data'] = None
    elif src_column.get('type') == ColumnTypes.LINK:
        column['type'] = ColumnTypes.TEXT
        column['data'] = None
    elif src_column.get('type') == ColumnTypes.LINK_FORMULA:
        data = src_column.get('data', {})
        result_type = data.get('result_type', 'string')
        if result_type == 'number':
            column['type'] = ColumnTypes.NUMBER
            column['data'] = {
                'format': data.get('format', 'number'),
                'precision': data.get('precision', 2),
                'enable_precision': data.get('enable_precision', False),
                'enable_fill_default_value': data.get('enable_fill_default_value', False),
                'decimal': data.get('decimal', 'dot'),
                'thousands': data.get('thousands', 'no'),
                'currency_symbol': data.get('currency_symbol')
            }
        elif result_type == 'string':
            column['type'] = ColumnTypes.TEXT
            column['data'] = None
        elif result_type == 'date':
            column['type'] = ColumnTypes.DATE
            column['data'] = {
                'format': data.get('format', 'YYYY-MM-DD')
            }
        elif result_type == 'bool':
            column['type'] = ColumnTypes.CHECKBOX,
            column['data'] = None
        elif result_type == 'array':
            array_type = data.get('array_type')
            array_data = data.get('array_data')
            if not array_type:
                column['type'] = ColumnTypes.TEXT
                column['data'] = None
            elif array_type in [
                ColumnTypes.NUMBER,
                ColumnTypes.DATE,
                ColumnTypes.SINGLE_SELECT,
                ColumnTypes.MULTIPLE_SELECT,
                ColumnTypes.DURATION,
                ColumnTypes.GEOLOCATION
            ]:
                column['type'] = array_type
                column['data'] = array_data
            elif array_type in [
                ColumnTypes.TEXT,
                ColumnTypes.LONG_TEXT,
                ColumnTypes.COLLABORATOR,
                ColumnTypes.IMAGE,
                ColumnTypes.FILE,
                ColumnTypes.EMAIL,
                ColumnTypes.URL,
                ColumnTypes.CHECKBOX,
                ColumnTypes.CREATOR,
                ColumnTypes.CTIME,
                ColumnTypes.LAST_MODIFIER,
                ColumnTypes.MTIME,
                ColumnTypes.AUTO_NUMBER,
            ]:
                column['type'] = array_type
                column['data'] = None
            else:
                column['type'] = ColumnTypes.TEXT
                column['data'] = None
        else:
            column['type'] = ColumnTypes.TEXT
            column['data'] = None
    return column


def generate_synced_columns(src_columns, dst_columns=None):
    """
    generate synced columns

    return: to_be_updated_columns -> list or None, to_be_appended_columns -> list or None, error_msg -> str or None
    """
    transfered_columns = []
    for col in src_columns:
        new_col = transfer_column(col)
        if new_col:
            transfered_columns.append(new_col)
    if not dst_columns:
        return None, transfered_columns, None
    to_be_updated_columns, to_be_appended_columns = [], []
    dst_column_name_dict = {col.get('name'): True for col in dst_columns}
    dst_column_key_dict = {col.get('key'): col for col in dst_columns}

    for col in transfered_columns:
        dst_col = dst_column_key_dict.get(col.get('key'))
        if dst_col:
            dst_col['type'] = col.get('type')
            dst_col['data'] = col.get('data')
            to_be_updated_columns.append(dst_col)
        else:
            if dst_column_name_dict.get(col.get('name')):
                return None, None, 'Column %s exists' % (col.get('name'),)
            to_be_appended_columns.append(col)
    return to_be_updated_columns, to_be_appended_columns, None


def generate_synced_rows(converted_rows, src_rows, src_columns, synced_columns, dst_rows=None):
    """
    generate synced rows divided into `rows to be updated`, `rows to be appended` and `rows to be deleted`
    return to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids
    """

    converted_rows_dict = {row.get('_id'): row for row in converted_rows}
    src_rows_dict = {row.get('_id'): row for row in src_rows}
    synced_columns_dict = {col.get('key'): col for col in synced_columns}

    to_be_updated_rows, to_be_appended_rows, transfered_row_ids = [], [], {}
    if not dst_rows:
        dst_rows = []
    to_be_deleted_row_ids = []
    for row in dst_rows:
        row_id = row.get('_id')
        src_row = src_rows_dict.get(row_id)
        converted_row = converted_rows_dict.get(row_id)
        if not converted_row or not src_row:
            to_be_deleted_row_ids.append(row_id)
            continue

        update_row = generate_single_row(converted_row, src_row, src_columns, synced_columns_dict, dst_row=row)
        if update_row:
            update_row['_id'] = row_id
            to_be_updated_rows.append(update_row)
        transfered_row_ids[row_id] = True

    for converted_row in converted_rows:
        row_id = converted_row.get('_id')
        src_row = src_rows_dict.get(row_id)
        if not src_row or transfered_row_ids.get(row_id):
            continue
        append_row = generate_single_row(converted_row, src_row, src_columns, synced_columns_dict, dst_row=None)
        if append_row:
            append_row['_id'] = row_id
            to_be_appended_rows.append(append_row)
        transfered_row_ids[row_id] = True
    return to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids


def generate_single_row(converted_row, src_row, src_columns, transfered_columns_dict, dst_row=None):
    """
        generate new single row according to src column type
        :param converted_row: {'_id': '', 'column_name_1': '', 'col_name_2'; ''} from dtable-db
        :param src_columns: [{'key': 'column_key_1', 'name': 'column_name_1'}]
        :param transfered_columns_dict: {'col_key_1': {'key': 'column_key_1', 'name': 'column_name_1'}}
        :param dst_row: {'_id': '', 'column_key_1': '', 'col_key_2': ''}
    """
    dataset_row = {}
    op_type = 'update'
    if not dst_row:
        op_type = 'append'
    dst_row = deepcopy(dst_row) if dst_row else {'_id': src_row.get('_id')}
    for col in src_columns:
        col_key = col.get('key')
        col_name = col.get('name')
        col_type = col.get('type')

        converted_cell_value = converted_row.get(col_name)
        if not converted_cell_value:
            continue
        transfered_column = transfered_columns_dict.get(col_key)

        if op_type == 'update':
            src_cell_value = src_row.get(col_key)
            dst_cell_value = dst_row.get(col_key)
            if col_type == ColumnTypes.MULTIPLE_SELECT:
                src_cell_value = sorted(src_cell_value)
                dst_cell_value = sorted(dst_row.get(col_key, []))

            if src_cell_value == dst_cell_value:
                continue

        converted_value = get_converted_cell_value(converted_cell_value, src_row, transfered_column, col)
        if converted_value is not None:
            dataset_row[col_key] = converted_value
    return dataset_row


def import_or_sync(import_sync_context):
    """
    import or sync common dataset

    return: dst_table_id, error_msg -> str or None
    """
    # extract necessary assets
    dst_dtable_uuid = import_sync_context.get('dst_dtable_uuid')
    src_dtable_uuid = import_sync_context.get('src_dtable_uuid')

    src_rows = import_sync_context.get('src_rows')
    src_columns = import_sync_context.get('src_columns')
    src_table_name = import_sync_context.get('src_table_name')
    src_view_name = import_sync_context.get('src_view_name')
    src_headers = import_sync_context.get('src_headers')

    dst_table_id = import_sync_context.get('dst_table_id')
    dst_table_name = import_sync_context.get('dst_table_name')
    dst_headers = import_sync_context.get('dst_headers')
    dst_columns = import_sync_context.get('dst_columns')
    dst_rows = import_sync_context.get('dst_rows')

    lang = import_sync_context.get('lang', 'en')

    # generate cols and rows
    ## generate cols
    to_be_updated_columns, to_be_appended_columns, error = generate_synced_columns(src_columns, dst_columns=dst_columns)
    if error:
        return None, error
    ## generate rows
    ### get src view-rows
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
            dtable_io_logger.error('request src_dtable: %s params: %s view-rows error: %s', src_dtable_uuid, query_params, e)
            return None, 'request src_dtable: %s params: %s view-rows error: %s' % (src_dtable_uuid, query_params, e)
        result_rows.extend(temp_result_rows)
        if not temp_result_rows or len(temp_result_rows) < limit:
            break
        start += limit

    final_columns = (to_be_updated_columns or []) + (to_be_appended_columns or [])

    to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids = generate_synced_rows(result_rows, src_rows, src_columns, final_columns, dst_rows=dst_rows)

    # sync table
    ## maybe create table
    if not dst_table_id:
        url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/tables/' % (str(dst_dtable_uuid),)
        data = {
            'table_name': dst_table_name,
            'lang': lang,
            'columns': [{
                'column_key': col.get('key'),
                'column_name': col.get('name'),
                'column_type': col.get('type'),
                'column_data': col.get('data')
            } for col in to_be_appended_columns] if to_be_appended_columns else []
        }
        try:
            resp = requests.post(url, headers=dst_headers, json=data)
            if resp.status_code != 200:
                dtable_io_logger.error('create new table error status code: %s, resp text: %s', resp.status_code, resp.text)
                return None, 'create new table error status code: %s, resp text: %s' % (resp.status_code, resp.text)
            dst_table_id = resp.json().get('_id')
        except Exception as e:
            dtable_io_logger.error(e)
            return None, str(e)
    ## or maybe append/update columns
    else:
        ### batch append columns
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
                    dtable_io_logger.error('batch append columns to dst dtable: %s, table: %s error status code: %s text: %s', dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
                    return None, 'batch append columns to dst dtable: %s, table: %s error status code: %s text: %s' % (dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
            except Exception as e:
                dtable_io_logger.error('batch append columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
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
                    dtable_io_logger.error('batch update columns to dst dtable: %s, table: %s error status code: %s text: %s', dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
                    return None, 'batch update columns to dst dtable: %s, table: %s error status code: %s text: %s' % (dst_dtable_uuid, dst_table_id, resp.status_code, resp.text)
            except Exception as e:
                dtable_io_logger.error('batch update columns to dst dtable: %s, table: %s error: %s', dst_dtable_uuid, dst_table_id, e)
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
                dtable_io_logger.error('sync dataset update rows dst dtable: %s dst table: %s error status code: %s content: %s', dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
                return None, 'sync dataset update rows dst dtable: %s dst table: %s error status code: %s content: %s' % (dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
        except Exception as e:
            dtable_io_logger.error('sync dataset update rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
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
                dtable_io_logger.error('sync dataset delete rows dst dtable: %s dst table: %s error status code: %s, content: %s', dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
                return None, 'sync dataset delete rows dst dtable: %s dst table: %s error status code: %s, content: %s' % (dst_dtable_uuid, dst_table_name, resp.status_code, resp.text)
        except Exception as e:
            dtable_io_logger.error('sync dataset delete rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
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
                dtable_io_logger.error('sync dataset append rows dst dtable: %s dst table: %s error status code: %s', dst_dtable_uuid, dst_table_name, resp.status_code)
                return None, 'sync dataset append rows dst dtable: %s dst table: %s error status code: %s' % (dst_dtable_uuid, dst_table_name, resp.status_code)
        except Exception as e:
            dtable_io_logger.error('sync dataset append rows dst dtable: %s dst table: %s error: %s', dst_dtable_uuid, dst_table_name, e)
            return None, 'sync dataset append rows dst dtable: %s dst table: %s error: %s' % (dst_dtable_uuid, dst_table_name, e)

    return dst_table_id, None


def sync_common_dataset(context, config):
    """
    sync common dataset to destination table

    :param dst_dtable: destination dtable
    :param src_dtable: source dtable
    :param table_id: source table id
    :param view_id: source view id
    :param dst_table_id: destination table id

    :return api_error or None
    """
    dst_headers = context['dst_headers']
    src_table = context['src_table']
    src_view = context['src_view']
    src_columns = context['src_columns']
    src_headers = context['src_headers']

    dst_dtable_uuid = context['dst_dtable_uuid']
    src_dtable_uuid = context['src_dtable_uuid']
    dst_table_id = context['dst_table_id']

    dataset_id = context.get('dataset_id')
    src_version = context.get('src_version')
    dst_version = context.get('dst_version')

    # get database version
    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))
        return
    sql = '''
                SELECT id FROM dtable_common_dataset_sync 
                WHERE dst_dtable_uuid=:dst_dtable_uuid AND dataset_id=:dataset_id AND dst_table_id=:dst_table_id 
                AND src_version=:src_version AND dst_version=:dst_version
            '''
    try:
        sync_dataset = db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dataset_id': dataset_id,
            'dst_table_id': dst_table_id,
            'src_version': src_version,
            'dst_version': dst_version,
        })
    except Exception as e:
        dtable_io_logger.error('get src version error: %s', e)
        return
    finally:
        db_session.close()

    if list(sync_dataset):
        return

    # request dst_dtable
    url = dtable_server_url.strip('/') + '/dtables/' + str(dst_dtable_uuid) + '?from=dtable_events'
    try:
        resp = requests.get(url, headers=dst_headers)
        dst_dtable_json = resp.json()
    except Exception as e:
        dtable_io_logger.error('request dst dtable: %s error: %s', dst_dtable_uuid, e)
        return

    # check dst_table
    dst_table = None
    for table in dst_dtable_json.get('tables', []):
        if table.get('_id') == dst_table_id:
            dst_table = table
            break
    if not dst_table:
        dtable_io_logger.error('Destination table: %s not found.' % dst_table_id)
        return
    dst_columns = dst_table.get('columns')
    dst_rows = dst_table.get('rows')

    try:
        dst_table_id, error_msg = import_or_sync({
            'dst_dtable_uuid': dst_dtable_uuid,
            'src_dtable_uuid': src_dtable_uuid,
            'src_rows': src_table.get('rows', []),
            'src_columns': src_columns,
            'src_table_name': src_table.get('name'),
            'src_view_name': src_view.get('name'),
            'src_headers': src_headers,
            'dst_table_id': dst_table_id,
            'dst_table_name': dst_table.get('name'),
            'dst_headers': dst_headers,
            'dst_rows': dst_rows,
            'dst_columns': dst_columns
        })
        if error_msg:
            dtable_io_logger.error(error_msg)
            return
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('sync common dataset error: %s', e)
        return

    # get base's metadata
    url = dtable_server_url.rstrip('/') + '/api/v1/dtables/' + str(src_dtable_uuid) + '/metadata/?from=dtable_events'
    dst_url = dtable_server_url.rstrip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/metadata/?from=dtable_events'
    try:
        dtable_metadata = requests.get(url, headers=src_headers)
        dst_dtable_metadata = requests.get(dst_url, headers=dst_headers)
        src_metadata = dtable_metadata.json()
        dst_metadata = dst_dtable_metadata.json()
    except Exception as e:
        dtable_io_logger.error('get metadata error:  %s', e)
        return None, 'get metadata error: %s' % (e,)

    last_src_version = src_metadata.get('metadata', {}).get('version')
    last_dst_version = dst_metadata.get('metadata', {}).get('version')

    sql = '''
        UPDATE dtable_common_dataset_sync SET
        last_sync_time=:last_sync_time, src_version=:last_src_version, dst_version=:last_dst_version
        WHERE dataset_id=:dataset_id AND dst_dtable_uuid=:dst_dtable_uuid AND dst_table_id=:dst_table_id
    '''
    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dst_table_id': dst_table_id,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id,
            'last_src_version': last_src_version,
            'last_dst_version': last_dst_version
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable common dataset sync error: %s', e)
    finally:
        db_session.close()


def import_common_dataset(context, config):
    """
    import common dataset to destination table
    """
    dst_headers = context['dst_headers']
    src_table = context['src_table']
    src_columns = context['src_columns']
    src_view = context['src_view']
    src_headers = context['src_headers']

    dst_dtable_uuid = context['dst_dtable_uuid']
    src_dtable_uuid = context['src_dtable_uuid']
    dst_table_name = context['dst_table_name']
    lang = context.get('lang', 'en')

    dataset_id = context.get('dataset_id')
    creator = context.get('creator')

    try:
        dst_table_id, error_msg = import_or_sync({
            'dst_dtable_uuid': dst_dtable_uuid,
            'src_dtable_uuid': src_dtable_uuid,
            'src_rows': src_table.get('rows', []),
            'src_columns': src_columns,
            'src_table_name': src_table.get('name'),
            'src_view_name': src_view.get('name'),
            'src_headers': src_headers,
            'dst_table_name': dst_table_name,
            'dst_headers': dst_headers,
            'lang': lang
        })
        if error_msg:
            dtable_io_logger.error(error_msg)
            return
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('import common dataset error: %s', e)
        return

    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))
        return

    # get base's metadata
    url = dtable_server_url.rstrip('/') + '/api/v1/dtables/' + str(
        src_dtable_uuid) + '/metadata/?from=dtable_events'
    dst_url = dtable_server_url.rstrip('/') + '/api/v1/dtables/' + str(
        dst_dtable_uuid) + '/metadata/?from=dtable_events'
    try:
        dtable_metadata = requests.get(url, headers=src_headers)
        dst_dtable_metadata = requests.get(dst_url, headers=dst_headers)
        src_metadata = dtable_metadata.json()
        dst_metadata = dst_dtable_metadata.json()
    except Exception as e:
        dtable_io_logger.error('get metadata error:  %s', e)
        return None, 'get metadata error: %s' % (e,)

    last_src_version = src_metadata.get('metadata', {}).get('version')
    last_dst_version = dst_metadata.get('metadata', {}).get('version')

    sql = '''
        INSERT INTO dtable_common_dataset_sync (`dst_dtable_uuid`, `dst_table_id`, `created_at`, `creator`, `last_sync_time`, `dataset_id`, `src_version`, `dst_version`)
        VALUES (:dst_dtable_uuid, :dst_table_id, :created_at, :creator, :last_sync_time, :dataset_id, :src_version, :dst_version)
    '''

    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': uuid_str_to_32_chars(dst_dtable_uuid),
            'dst_table_id': dst_table_id,
            'created_at': datetime.now(),
            'creator': creator,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id,
            'src_version': last_src_version,
            'dst_version': last_dst_version
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable common dataset sync error: %s', e)
    finally:
        db_session.close()
