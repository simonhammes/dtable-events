import shutil
import os
from zipfile import ZipFile

from dtable_events.dtable_io.utils import prepare_dtable_json, \
    prepare_asset_file_folder, post_dtable_json, post_asset_files


def clear_tmp_files_and_dirs(TMP_FILE_PATH, TMP_ZIP_PATH):
    # delete tmp files/dirs
    if os.path.exists(TMP_FILE_PATH):
        shutil.rmtree(TMP_FILE_PATH)
    if os.path.exists(TMP_ZIP_PATH):
        os.remove(TMP_ZIP_PATH)


def get_dtable_export_content(username, table_name, repo_id, dtable_id, dtable_file_dir_id, asset_dir_id=None):
    """
    1. prepare file content at /tmp/<dtable_id>/dtable_asset/...
    2. make zip file
    3. return zip file's content
    """
    TMP_FILE_PATH = os.path.join('/tmp', str(dtable_id),
                                 'dtable_asset/')  # used to store asset files and json from file_server
    TMP_ZIP_PATH = os.path.join('/tmp', str(dtable_id), 'zip_file') + '.zip'  # zip path of zipped xxx.dtable

    clear_tmp_files_and_dirs(TMP_FILE_PATH, TMP_ZIP_PATH)
    os.makedirs(TMP_FILE_PATH, exist_ok=True)
    # import here to avoid circular dependency

    # 1. create 'content.json' from 'xxx.dtable'
    prepare_dtable_json(repo_id, dtable_id, table_name, dtable_file_dir_id)
    # 2. get asset file folder, asset could be empty
    if asset_dir_id:
        prepare_asset_file_folder(username, repo_id, dtable_id, asset_dir_id)


    """
    /tmp/<dtable_id>/dtable_asset/
                                |- asset/
                                |- content.json

    we zip /tmp/<dtable_id>/dtable_asset/ to /tmp/<dtable_id>/zip_file.zip and download it
    notice than make_archive will auto add .zip suffix to /tmp/<dtable_id>/zip_file
    """
    shutil.make_archive('/tmp/' + str(dtable_id) +  '/zip_file', "zip", root_dir=TMP_FILE_PATH)
    with open(TMP_ZIP_PATH, 'rb') as f:
        zip_stream = f.read()
    clear_tmp_files_and_dirs(TMP_FILE_PATH, TMP_ZIP_PATH)
    return zip_stream


def post_dtable_import_files(username, repo_id, workspace_id, dtable_id, dtable_uuid, dtable_file_name, uploaded_temp_path):
    """
    post files at /tmp/<dtable_id>/dtable_zip_extracted/ to file server
    """
    TMP_EXTRACTED_PATH = os.path.join('/tmp', dtable_id, 'dtable_zip_extracted/')

    with ZipFile(uploaded_temp_path, 'r') as zip_file:
        zip_file.extractall(TMP_EXTRACTED_PATH)

    post_dtable_json(username, repo_id, workspace_id, dtable_id, dtable_uuid, dtable_file_name)
    post_asset_files(repo_id, dtable_id, dtable_uuid, username)

    # remove extracted tmp file
    shutil.rmtree(TMP_EXTRACTED_PATH)
