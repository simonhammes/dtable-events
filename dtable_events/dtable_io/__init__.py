import shutil
import os
from zipfile import ZipFile

from dtable_events.dtable_io.utils import prepare_dtable_json, \
    prepare_asset_file_folder, post_dtable_json, post_asset_files


def clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path):
    # delete tmp files/dirs
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)
    if os.path.exists(tmp_zip_path):
        os.remove(tmp_zip_path)


def get_dtable_export_content(username, repo_id, table_name, dtable_uuid, dtable_file_dir_id, asset_dir_id):
    """
    1. prepare file content at /tmp/dtable-io/<dtable_id>/dtable_asset/...
    2. make zip file
    3. return zip file's content
    """
    tmp_file_path = os.path.join('/tmp/dtable-io', dtable_uuid,
                                 'dtable_asset/')  # used to store asset files and json from file_server
    tmp_zip_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'zip_file') + '.zip'  # zip path of zipped xxx.dtable

    clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path)
    os.makedirs(tmp_file_path, exist_ok=True)
    # import here to avoid circular dependency

    # 1. create 'content.json' from 'xxx.dtable'
    prepare_dtable_json(repo_id, dtable_uuid, table_name, dtable_file_dir_id)
    # 2. get asset file folder, asset could be empty
    if asset_dir_id:
        prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id)


    """
    /tmp/dtable-io/<dtable_uuid>/dtable_asset/
                                    |- asset/
                                    |- content.json

    we zip /tmp/dtable-io/<dtable_uuid>/dtable_asset/ to /tmp/dtable-io/<dtable_id>/zip_file.zip and download it
    notice than make_archive will auto add .zip suffix to /tmp/dtable-io/<dtable_id>/zip_file
    """
    shutil.make_archive('/tmp/dtable-io/' + dtable_uuid +  '/zip_file', "zip", root_dir=tmp_file_path)

    # only rm tmp_file_path here, we remove tmp_zip_path in dtable web api
    if os.path.exists(tmp_file_path):
        shutil.rmtree(tmp_file_path)


def post_dtable_import_files(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, uploaded_temp_path):
    """
    post files at /tmp/<dtable_uuid>/dtable_zip_extracted/ to file server
    """
    tmp_extracted_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/')

    with ZipFile(uploaded_temp_path, 'r') as zip_file:
        zip_file.extractall(tmp_extracted_path)

    post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name)
    post_asset_files(repo_id, dtable_uuid, username)

    # remove extracted tmp file
    shutil.rmtree(tmp_extracted_path)

