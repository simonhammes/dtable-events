import re
import time
from copy import deepcopy
from datetime import datetime

import jwt
import requests

from dtable_events.db import init_db_session_class
from dtable_events.dtable_io.utils import setup_logger
from dtable_events.dtable_io.task_manager import task_manager
from dtable_events.utils.constants import ColumnTypes


dtable_io_logger = setup_logger('dtable_events_io.log')

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
                        dtable_io_logger.error(e)
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
    dst_column_names = [col.get('name') for col in dst_columns]
    for col in transfered_columns:
        exists = False
        for dst_col in dst_columns:
            if dst_col.get('key') != col.get('key'):
                continue
            exists = True
            dst_col['type'] = col.get('type')
            dst_col['data'] = col.get('data')
            to_be_updated_columns.append(dst_col)
            break
        if not exists:
            if col.get('name') in dst_column_names:
                return None, None, 'Column %s exists' % (col.get('name'),)
            to_be_appended_columns.append(col)
    return to_be_updated_columns, to_be_appended_columns, None


def generate_single_row(converted_row, src_row, src_columns, transfered_columns_dict, dst_row=None):
    """
    generate new single row according to src column type

    :param converted_row: {'_id': '', 'column_name_1': '', 'col_name_2'; ''} from dtable-db
    :param src_columns: [{'key': 'column_key_1', 'name': 'column_name_1'}]
    :param transfered_columns_dict: {'col_key_1': {'key': 'column_key_1', 'name': 'column_name_1'}}
    :param dst_row: {'_id': '', 'column_key_1': '', 'col_key_2': ''}
    """
    dst_row = deepcopy(dst_row) if dst_row else {'_id': converted_row.get('_id')}
    for col in src_columns:
        col_key = col.get('key')
        col_name = col.get('name')
        col_type = col.get('type')

        if not converted_row.get(col_name):
            dst_row.pop(col_key, None)
            continue
        converted_cell_value = converted_row[col_name]
        transfered_column = transfered_columns_dict.get(col_key)

        if col_type in [
            ColumnTypes.TEXT,
            ColumnTypes.LONG_TEXT,
            ColumnTypes.IMAGE,
            ColumnTypes.FILE,
            ColumnTypes.RATE,
            ColumnTypes.NUMBER,
            ColumnTypes.COLLABORATOR,
            ColumnTypes.DURATION,
            ColumnTypes.EMAIL,
            ColumnTypes.DATE,
            ColumnTypes.CHECKBOX,
            ColumnTypes.AUTO_NUMBER,
            ColumnTypes.CREATOR,
            ColumnTypes.CTIME,
            ColumnTypes.LAST_MODIFIER,
            ColumnTypes.MTIME,
            ColumnTypes.URL,
            ColumnTypes.GEOLOCATION
        ]:
            dst_row[col_key] = deepcopy(src_row.get(col_key))

        elif col_type == ColumnTypes.SINGLE_SELECT:
            if not isinstance(converted_cell_value, str):
                continue
            options = col.get('data', {}).get('options', [])
            single_value = None
            for option in options:
                if option.get('name') == converted_cell_value:
                    single_value = option.get('id')
                    break
            if single_value:
                dst_row[col_key] = single_value

        elif col_type == ColumnTypes.MULTIPLE_SELECT:
            if not isinstance(converted_cell_value, list):
                continue
            options = col.get('data', {}).get('options', [])
            multi_value = []
            for option in options:
                if option.get('name') in converted_cell_value:
                    multi_value.append(option.get('id'))
            if multi_value:
                dst_row[col_key] = multi_value

        elif col_type == ColumnTypes.LINK:
            if not isinstance(converted_cell_value, list):
                continue
            dst_row[col_key] = ', '.join([str(v.get('display_value', '')) for v in converted_cell_value])

        elif col_type == ColumnTypes.FORMULA:
            result_type = col.get('data', {}).get('result_type')
            if result_type == 'number':
                try:
                    re_number = r'(\-|\+)?\d+(\.\d+)?'
                    match_obj = re.search(re_number, str(converted_cell_value))
                    if not match_obj:
                        continue
                    start, end = match_obj.span()
                    dst_row[col_key] = float(str(converted_cell_value)[start: end])
                except Exception as e:
                    dtable_io_logger.error('re search: %s in: %s error: %s', re_number, converted_cell_value, e)
            elif result_type == 'date':
                dst_row[col_key] = converted_cell_value
            elif result_type == 'bool':
                if isinstance(converted_cell_value, bool):
                    dst_row[col_key] = converted_cell_value
                    continue
                dst_row[col_key] = str(converted_cell_value).upper() == 'TRUE'
            elif result_type == 'string':
                options = col.get('data', {}).get('options')
                if options and isinstance(options, list):
                    options_dict = {option.get('id'): option.get('name', '') for option in options}
                    if isinstance(converted_cell_value, list):
                        values = [options_dict.get(item, item) for item in converted_cell_value]
                        dst_row[col_key] = ', '.join(values)
                    else:
                        dst_row[col_key] = options_dict.get(converted_cell_value, converted_cell_value)
                else:
                    if isinstance(converted_cell_value, list):
                        dst_row[col_key] = ', '.join(str(v) for v in converted_cell_value)
                    else:
                        dst_row[col_key] = converted_cell_value
            else:
                if isinstance(converted_cell_value, list):
                    dst_row[col_key] = ', '.join(str(v) for v in converted_cell_value)
                else:
                    dst_row[col_key] = converted_cell_value

        elif col_type == ColumnTypes.LINK_FORMULA:
            result_type = col.get('data', {}).get('result_type')
            if result_type == 'number':
                try:
                    re_number = r'(\-|\+)?\d+(\.\d+)?'
                    match_obj = re.search(re_number, str(converted_cell_value))
                    if not match_obj:
                        continue
                    start, end = match_obj.span()
                    dst_row[col_key] = int(str(converted_cell_value)[start: end])
                except Exception as e:
                    dtable_io_logger.error('re search: %s in: %s error: %s', re_number, converted_cell_value, e)
            elif result_type == 'date':
                dst_row[col_key] = converted_cell_value
            elif result_type == 'bool':
                if isinstance(converted_cell_value, bool):
                    dst_row[col_key] = converted_cell_value
                    continue
                dst_row[col_key] = str(converted_cell_value).upper() == 'TRUE'
            elif result_type == 'array':
                transfered_type = transfered_column.get('type')
                if not isinstance(converted_cell_value, list):
                    continue
                if transfered_type in [
                    ColumnTypes.TEXT,
                    ColumnTypes.LONG_TEXT,
                    ColumnTypes.IMAGE,
                    ColumnTypes.FILE,
                    ColumnTypes.RATE,
                    ColumnTypes.NUMBER,
                    ColumnTypes.COLLABORATOR,
                    ColumnTypes.DURATION,
                    ColumnTypes.EMAIL,
                    ColumnTypes.CHECKBOX,
                    ColumnTypes.AUTO_NUMBER,
                    ColumnTypes.CREATOR,
                    ColumnTypes.CTIME,
                    ColumnTypes.LAST_MODIFIER,
                    ColumnTypes.MTIME,
                    ColumnTypes.URL,
                    ColumnTypes.GEOLOCATION,
                    ColumnTypes.SINGLE_SELECT
                ]:
                    if converted_cell_value:
                        dst_row[col_key] = converted_cell_value[0]
                elif transfered_type == ColumnTypes.MULTIPLE_SELECT:
                    if converted_cell_value:
                        dst_row[col_key] = [converted_cell_value[0]]
                elif transfered_type == ColumnTypes.DATE:
                    if converted_cell_value:
                        try:
                            value = datetime.fromisoformat(converted_cell_value[0])
                        except:
                            pass
                        else:
                            data_format = transfered_column.get('data', {}).get('format')
                            if data_format == 'YYYY-MM-DD':
                                dst_row[col_key] = value.strftime('%Y-%m-%d')
                            elif data_format == 'YYYY-MM-DD HH:mm':
                                dst_row[col_key] = value.strftime('%Y-%m-%d %H:%M')
                            else:
                                dst_row[col_key] = value.strftime('%Y-%m-%d')

    return dst_row


def generate_synced_rows(converted_rows, src_rows, src_columns, synced_columns, dst_rows=None):
    """
    generate synced rows divided into `rows to be updated`, `rows to be appended` and `rows to be deleted`
    return to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids
    """
    converted_rows_dict = {row.get('_id'): row for row in converted_rows}
    src_rows_dict = {row.get('_id'): row for row in src_rows}
    synced_columns_dict = {col.get('key'): col for col in synced_columns}
    to_be_updated_rows, to_be_appended_rows, transfered_row_ids = [], [], set()
    if not dst_rows:
        dst_rows = []
    to_be_deleted_row_ids = []
    for row in dst_rows:
        row_id = row.get('_id')
        if row_id not in converted_rows_dict:
            to_be_deleted_row_ids.append(row_id)
            continue
        src_row = src_rows_dict.get(row_id)
        converted_row = converted_rows_dict.get(row_id)
        to_be_updated_rows.append(generate_single_row(converted_row, src_row, src_columns, synced_columns_dict, dst_row=row))
        transfered_row_ids.add(row_id)

    for converted_row in converted_rows:
        row_id = converted_row.get('_id')
        if row_id in transfered_row_ids:
            continue
        src_row = src_rows_dict.get(row_id)
        to_be_appended_rows.append(generate_single_row(converted_row, src_row, src_columns, synced_columns_dict, dst_row=None))
        transfered_row_ids.add(row_id)

    return to_be_updated_rows, to_be_appended_rows, to_be_deleted_row_ids


def import_or_sync(dst_dtable_uuid, src_dtable_uuid, src_rows, src_columns, src_table_name, src_view_name, src_headers, dst_table_id, dst_table_name, dst_headers, dst_columns=None, dst_rows=None, lang='en'):
    """
    import or sync common dataset

    return: dst_table_id, error_msg -> str or None
    """
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
        url = dtable_server_url.rstrip('/') + '/api/v1/internal/dtables/' + str(src_dtable_uuid) + '/view-rows/?from=dtable_web'
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
        url = dtable_server_url.strip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/batch-append-columns/?from=dtable_web'
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
        url = dtable_server_url.strip('/') + '/api/v1/dtables/' + str(dst_dtable_uuid) + '/batch-update-columns/?from=dtable_web'
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
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-update-rows/?from=dtable_web' % (str(dst_dtable_uuid),)
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
    url = dtable_server_url.strip('/') + '/api/v1/dtables/%s/batch-delete-rows/?from=dtable_web' % (str(dst_dtable_uuid),)
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

    # request dst_dtable
    url = dtable_server_url.strip('/') + '/dtables/' + str(dst_dtable_uuid) + '?from=dtable_web'
    try:
        resp = requests.get(url, headers=dst_headers)
        dst_dtable_json = resp.json()
    except Exception as e:
        dtable_io_logger.error('request dst dtable: %s error: %s', dst_dtable_uuid, e)
        return None, 'request dst dtable: %s error: %s' % (dst_dtable_uuid, e)

    # check dst_table
    dst_table = None
    for table in dst_dtable_json.get('tables', []):
        if table.get('_id') == dst_table_id:
            dst_table = table
            break
    if not dst_table:
        return None, 'Destination table: %s not found.' % dst_table_id
    dst_columns = dst_table.get('columns')
    dst_rows = dst_table.get('rows')

    try:
        dst_table_id, error_msg = import_or_sync(dst_dtable_uuid, src_dtable_uuid, src_table.get('rows', []), src_columns, src_table.get('name'), src_view.get('name'), src_headers, dst_table_id, dst_table.get('name'), dst_headers, dst_rows=dst_rows, dst_columns=dst_columns)
        if error_msg:
            dtable_io_logger.error(error_msg)
            return
    except Exception as e:
        dtable_io_logger.exception(e)
        dtable_io_logger.error('sync common dataset error: %s', e)
        return

    try:
        db_session = init_db_session_class(config)()
    except Exception as e:
        db_session = None
        dtable_io_logger.error('create db session failed. ERROR: {}'.format(e))
        return
    sql = '''
        UPDATE dtable_common_dataset_sync SET
        last_sync_time=:last_sync_time
        WHERE dataset_id=:dataset_id AND dst_dtable_uuid=:dst_dtable_uuid AND dst_table_id=:dst_table_id
    '''
    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': dst_dtable_uuid.replace('-', ''),
            'dst_table_id': dst_table_id,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable coomo dataset sync error: %s', e)
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
        dst_table_id, error_msg = import_or_sync(dst_dtable_uuid, src_dtable_uuid, src_table.get('rows', []), src_columns, src_table.get('name'), src_view.get('name'), src_headers, None, dst_table_name, dst_headers, lang=lang)
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
    sql = '''
        INSERT INTO dtable_common_dataset_sync (`dst_dtable_uuid`, `dst_table_id`, `created_at`, `creator`, `last_sync_time`, `dataset_id`)
        VALUES (:dst_dtable_uuid, :dst_table_id, :created_at, :creator, :last_sync_time, :dataset_id)
    '''
    try:
        db_session.execute(sql, {
            'dst_dtable_uuid': dst_dtable_uuid.replace('-', ''),
            'dst_table_id': dst_table_id,
            'created_at': datetime.now(),
            'creator': creator,
            'last_sync_time': datetime.now(),
            'dataset_id': dataset_id
        })
        db_session.commit()
    except Exception as e:
        dtable_io_logger.error('insert dtable coomo dataset sync error: %s', e)
    finally:
        db_session.close()
