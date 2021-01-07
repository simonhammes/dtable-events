import os
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


def parse_excel_to_json(dtable_name, file_dir):
    excel_file_path = os.path.join(file_dir, dtable_name + '.xlsx')

    tables = []
    wb = load_workbook(excel_file_path, read_only=True)
    for sheet in wb:
        rows, columns = parse_excel_sheet(sheet)
        table = {
            'name': sheet.title,
            'rows': rows,
            'columns': columns,
        }
        tables.append(table)

    json_file_path = os.path.join(file_dir, dtable_name + '.json')
    with open(json_file_path, 'w') as f:
        json.dump(tables, f)
