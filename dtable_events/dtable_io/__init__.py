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

def get_dtable_export_content_action(username, repo_id, table_name, dtable_uuid, dtable_file_dir_id, asset_dir_id, logger):
    """
    1. prepare file content at /tmp/dtable-io/<dtable_id>/dtable_asset/...
    2. make zip file
    3. return zip file's content
    """
    logger.info('Start prepare /tmp/dtable-io/{}/zip_file.zip for export DTable.'.format(dtable_uuid))

    tmp_file_path = os.path.join('/tmp/dtable-io', dtable_uuid,
                                 'dtable_asset/')  # used to store asset files and json from file_server
    tmp_zip_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'zip_file') + '.zip'  # zip path of zipped xxx.dtable

    logger.info('Clear tmp dirs and files before prepare.')
    clear_tmp_files_and_dirs(tmp_file_path, tmp_zip_path)
    os.makedirs(tmp_file_path, exist_ok=True)
    # import here to avoid circular dependency

    # 1. create 'content.json' from 'xxx.dtable'
    logger.info('Create content.json file.')
    try:
        prepare_dtable_json(repo_id, dtable_uuid, table_name, dtable_file_dir_id)
    except Exception as e:
        logger.error(e)


    # 2. get asset file folder, asset could be empty

    if asset_dir_id:
        logger.info('Create asset dir.')
        try:
            prepare_asset_file_folder(username, repo_id, dtable_uuid, asset_dir_id)
        except Exception as e:
            logger.error(e)


    """
    /tmp/dtable-io/<dtable_uuid>/dtable_asset/
                                    |- asset/
                                    |- content.json

    we zip /tmp/dtable-io/<dtable_uuid>/dtable_asset/ to /tmp/dtable-io/<dtable_id>/zip_file.zip and download it
    notice than make_archive will auto add .zip suffix to /tmp/dtable-io/<dtable_id>/zip_file
    """
    logger.info('Make zip file for download...')
    try:
        shutil.make_archive('/tmp/dtable-io/' + dtable_uuid +  '/zip_file', "zip", root_dir=tmp_file_path)
    except Exception as e:
        logger.error(e)

    logger.info('Create /tmp/dtable-io/{}/zip_file.zip success!'.format(dtable_uuid))
    # we remove '/tmp/dtable-io/<dtable_uuid>' in dtable web api

def get_dtable_export_content(*args):
    from dtable_events.dtable_io.utils import setup_logger
    logger = setup_logger(__name__)
    try:
        get_dtable_export_content_action(*args, logger)
    except Exception as e:
        logger.error(e)


def post_dtable_import_files_action(username, repo_id, workspace_id, dtable_uuid, dtable_file_name, uploaded_temp_path, logger):
    """
    post files at /tmp/<dtable_uuid>/dtable_zip_extracted/ to file server
    """
    logger.info('Start import DTable: {}.'.format(dtable_uuid))
    tmp_extracted_path = os.path.join('/tmp/dtable-io', dtable_uuid, 'dtable_zip_extracted/')

    logger.info('Extract all from uploaded tmp path: {}'.format(uploaded_temp_path))
    try:
        with ZipFile(uploaded_temp_path, 'r') as zip_file:
            zip_file.extractall(tmp_extracted_path)
    except Exception as e:
        logger.error(e)

    logger.info('Prepare dtable json file and post it at file server.')
    try:
        post_dtable_json(username, repo_id, workspace_id, dtable_uuid, dtable_file_name)
    except Exception as e:
        logger.error(e)

    logger.info('Post asset files in tmp path to filesercer.')
    try:
        post_asset_files(repo_id, dtable_uuid, username)
    except Exception as e:
        logger.error(e)


    # remove extracted tmp file
    logger.info('Remove extracted tmp file.')
    try:
        shutil.rmtree(os.path.join('/tmp/dtable-io', dtable_uuid))
    except Exception as e:
        logger.error(e)

    logger.info('Import DTable: {} success!'.format(dtable_uuid))


def post_dtable_import_files(*args):
    from dtable_events.dtable_io.utils import setup_logger
    logger = setup_logger(__name__)
    try:
        post_dtable_import_files_action(*args, logger)
    except Exception as e:
        logger.error(e)