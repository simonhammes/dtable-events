import json
import logging
from datetime import datetime

from openpyxl import load_workbook


CHECKBOX_TUPLE = (
    ('√', 'x'),
    ('checked', 'unchecked'),
    ('y', 'n'),
    ('yes', 'no'),
    ('enabled', 'disabled'),
    ('on', 'off'),
    ('是', '否'),
    ('完成', '未完成'),
)
CHECKBOX_STRING_LIST = [string for item in CHECKBOX_TUPLE for string in item]
CHECKBOX_TRUE_LISt = [item[0] for item in CHECKBOX_TUPLE]


def parse_excel_rows(sheet_rows, columns, head_index, max_column):
    if head_index < 0:
        value_rows = sheet_rows
    else:
        value_rows = sheet_rows[head_index + 1:]
    rows = []
    for row in value_rows:
        row_data = {}
        for index in range(max_column):
            try:
                cell_value = row[index].value
                column_name = columns[index]['name']
                column_type = columns[index]['type']
                if cell_value is None:
                    continue
                elif column_type == 'number':
                    row_data[column_name] = cell_value
                elif column_type == 'date':
                    row_data[column_name] = str(cell_value)
                elif column_type == 'long-text':
                    row_data[column_name] = {
                        'preview': cell_value[:10].replace('\n', ' '),
                        'text': cell_value,
                    }
                elif column_type == 'checkbox':
                    row_data[column_name] = True if cell_value in CHECKBOX_TRUE_LISt else False
                elif column_type == 'multiple-select':
                    multiple_value = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
                    row_data[column_name] = [value.strip(' ') for value in multiple_value]
                else:
                    row_data[column_name] = str(cell_value)
            except Exception as e:
                logging.exception(e)
        rows.append(row_data)

    return rows


def parse_excel_column_type(value_list):
    try:
        type_list = []
        # Check the first 200 rows of data
        for cell_value in value_list[:200]:
            if cell_value is None:
                continue
            elif isinstance(cell_value, int) or isinstance(cell_value, float):
                column_type = 'number'
            elif isinstance(cell_value, datetime):
                column_type = 'date'
            elif '\n' in cell_value:
                column_type = 'long-text'
            elif cell_value in CHECKBOX_STRING_LIST:
                column_type = 'checkbox'
            elif (',' in cell_value or '，' in cell_value) \
                    and ('{' not in cell_value):
                column_type = 'multiple-select'
                multiple_value = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
                for value in multiple_value:
                    if len(value.strip(' ')) > 20:
                        # more than 20 characters.
                        column_type = 'text'
            else:
                column_type = 'text'
            type_list.append(column_type)

        max_column_type = max(type_list, key=type_list.count) if type_list else 'text'
        column_data = None
        if max_column_type == 'multiple-select':
            multiple_list = []
            for cell_value in value_list:
                if cell_value is None:
                    continue
                multiple_value = cell_value.split('，') if '，' in cell_value else cell_value.split(',')
                for value in multiple_value:
                    value = value.strip(' ')
                    if value not in multiple_list:
                        multiple_list.append(value)
            column_data = {'options': [{'name': value} for value in multiple_list]}

        return max_column_type, column_data
    except Exception as e:
        logging.exception(e)
        return 'text', None

def parse_excel_columns(sheet_rows, head_index, max_column):
    if head_index == -1:
        head_row = [None] * head_index
        value_rows = sheet_rows
    else:
        head_row = sheet_rows[head_index]
        value_rows = sheet_rows[head_index + 1:]

    columns = []
    for index in range(max_column):
        name = head_row[index].value
        column_name = str(name) if name else 'Field' + str(index + 1)
        value_list = [row[index].value for row in value_rows]
        column_type, column_data = parse_excel_column_type(value_list)
        column = {
            'name': column_name,
            'type': column_type,
            'data': column_data,
        }
        columns.append(column)
        index = index + 1

    return columns


def parse_excel_to_json(repo_id, dtable_name, custom=False):
    from dtable_events.dtable_io.utils import get_excel_file, \
        upload_excel_json_file, get_excel_json_file

    # user custom columns
    if custom:
        json_file = get_excel_json_file(repo_id, dtable_name)
        tables = json.loads(json_file)
        head_index_map = {table['name']: table['head_index'] for table in tables}
    else:
        head_index_map = {}

    # parse
    excel_file = get_excel_file(repo_id, dtable_name)
    tables = []
    wb = load_workbook(excel_file, read_only=True)
    for sheet in wb:
        sheet_rows = list(sheet.rows)
        max_row = len(sheet_rows)
        if not max_row:
            continue

        if custom:
            head_index = head_index_map.get(sheet.title, 0)
            if head_index > max_row - 1:
                head_index = 0
        else:
            head_index = 0

        max_column = sheet.max_column
        columns = parse_excel_columns(sheet_rows, head_index, max_column)
        rows = parse_excel_rows(sheet_rows, columns, head_index, max_column)

        table = {
            'name': sheet.title,
            'rows': rows,
            'columns': columns,
            'head_index': head_index,
            'max_row': max_row,
            'max_column': max_column,
        }
        tables.append(table)

    # upload json to file server
    upload_excel_json_file(repo_id, dtable_name, json.dumps(tables))


def import_excel_by_dtable_server(username, repo_id, dtable_uuid, dtable_name):
    from dtable_events.dtable_io.utils import get_excel_json_file, \
        upload_excel_json_to_dtable_server, delete_excel_file

    # get json file
    json_file = get_excel_json_file(repo_id, dtable_name)
    # delete excel file
    delete_excel_file(username, repo_id, dtable_name)
    # upload json file to dtable-server
    upload_excel_json_to_dtable_server(username, dtable_uuid, json_file)
