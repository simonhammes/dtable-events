import json
from datetime import datetime

from openpyxl import load_workbook


def parse_excel_rows(sheet_rows, columns, max_row, max_column):
    rows = []
    for row in sheet_rows[1:]:
        row = list(row)
        row_data = {}
        for index in range(max_column):
            cell_value = row[index].value
            column_name = columns[index]['name']
            column_type = columns[index]['type']
            if cell_value and column_type != 'number':
                cell_value = str(cell_value)
            row_data[column_name] = cell_value
        rows.append(row_data)

    return rows


def get_excel_column_type(cell_value):
    if isinstance(cell_value, int):
        column_type = 'number'
    elif isinstance(cell_value, float):
        column_type = 'number'
    elif isinstance(cell_value, datetime):
        column_type = 'date'
    else:
        column_type = 'text'

    return column_type


def parse_excel_columns(sheet_rows, max_row, max_column):
    # select middle row to parse column type
    middle_row = list(sheet_rows[max_row // 2])
    first_row = list(sheet_rows[0])

    columns = []
    for index in range(max_column):
        name_value = first_row[index].value
        middle_value = middle_row[index].value
        column_name = str(name_value) if name_value else 'Field' + str(index + 1)
        column = {
            'name': column_name,
            'type': get_excel_column_type(middle_value),
        }
        columns.append(column)
        index = index + 1

    return columns


def parse_excel_sheet(sheet):
    sheet_rows = list(sheet.rows)
    if not sheet_rows:
        return [], []

    max_row = len(sheet_rows)
    max_column = sheet.max_column
    columns = parse_excel_columns(sheet_rows, max_row, max_column)
    rows = parse_excel_rows(sheet_rows, columns, max_row, max_column)

    return rows, columns


def parse_excel_to_json(repo_id, dtable_name):
    from dtable_events.dtable_io.utils import get_excel_file, upload_excel_json_file

    # parse
    excel_file = get_excel_file(repo_id, dtable_name)
    tables = []
    wb = load_workbook(excel_file, read_only=True)
    for sheet in wb:
        rows, columns = parse_excel_sheet(sheet)
        if not columns:
            continue
        table = {
            'name': sheet.title,
            'rows': rows,
            'columns': columns,
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
