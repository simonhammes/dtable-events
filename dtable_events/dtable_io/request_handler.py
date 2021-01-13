import cgi
import json
import jwt
import logging
from urllib import parse
from http.server import SimpleHTTPRequestHandler
from dtable_events.dtable_io.task_manager import task_manager

logger = logging.getLogger(__name__)


class DTableIORequestHandler(SimpleHTTPRequestHandler):

    def do_GET(self):
        auth = self.headers['Authorization'].split()
        if not auth or auth[0].lower() != 'token' or len(auth) != 2:
            self.send_error(403, 'Token invalid.')
        token = auth[1]
        if not token:
            self.send_error(403, 'Token invalid.')

        private_key = task_manager.conf['dtable_private_key']
        try:
            jwt.decode(token, private_key, algorithms=['HS256'])
        except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
            self.send_error(403, e)

        path, arguments = parse.splitquery(self.path)
        arguments = parse.parse_qs(arguments)
        if path == '/add-export-task':

            if task_manager.tasks_queue.full():
                self.send_error(400, 'dtable io server busy.')
                return

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            table_name = arguments['table_name'][0]
            dtable_uuid = arguments['dtable_uuid'][0]

            try:
                task_id = task_manager.add_export_task(
                    username,
                    repo_id,
                    dtable_uuid,
                    table_name,
                )
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {'task_id': task_id}
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/add-import-task':

            if task_manager.tasks_queue.full():
                self.send_error(400, 'dtable io server busy.')
                return

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            workspace_id = arguments['workspace_id'][0]
            dtable_uuid = arguments['dtable_uuid'][0]
            dtable_file_name = arguments['dtable_file_name'][0]

            try:
                task_id = task_manager.add_import_task(
                    username,
                    repo_id,
                    workspace_id,
                    dtable_uuid,
                    dtable_file_name,
                )
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {}
            resp['task_id'] = task_id
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/add-parse-excel-task':

            if task_manager.tasks_queue.full():
                self.send_error(400, 'dtable io server busy.')
                return

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            workspace_id = arguments['workspace_id'][0]
            dtable_name = arguments['dtable_name'][0]
            custom = arguments['custom'][0]
            custom = bool(int(custom))

            try:
                task_id = task_manager.add_parse_excel_task(
                    username,
                    repo_id,
                    workspace_id,
                    dtable_name,
                    custom,
                )
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {}
            resp['task_id'] = task_id
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/add-import-excel-task':

            if task_manager.tasks_queue.full():
                self.send_error(400, 'dtable io server busy.')
                return

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            workspace_id = arguments['workspace_id'][0]
            dtable_uuid = arguments['dtable_uuid'][0]
            dtable_name = arguments['dtable_name'][0]

            try:
                task_id = task_manager.add_import_excel_task(
                    username,
                    repo_id,
                    workspace_id,
                    dtable_uuid,
                    dtable_name,
                )
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {}
            resp['task_id'] = task_id
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/query-status':
            task_id = arguments['task_id'][0]

            if not task_manager.is_valid_task_id(task_id):
                self.send_error(400, 'task_id invalid.')
            is_finished = False
            try:
                is_finished = task_manager.query_status(task_id)
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.end_headers()
            resp = {}
            resp['is_finished'] = is_finished
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/cancel-task':
            task_id = arguments['task_id'][0]

            if not task_manager.is_valid_task_id(task_id):
                self.send_error(400, 'task_id invalid.')
            try:
                task_manager.cancel_task(task_id)
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.end_headers()
            resp = {'success': True}
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        else:
            self.send_error(400, 'path %s invalid.' % path)

    def do_POST(self):
        auth = self.headers['Authorization'].split()
        if not auth or auth[0].lower() != 'token' or len(auth) != 2:
            self.send_error(403, 'Token invalid.')
        token = auth[1]
        if not token:
            self.send_error(403, 'Token invalid.')

        private_key = task_manager.conf['dtable_private_key']
        try:
            jwt.decode(token, private_key, algorithms=['HS256'])
        except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
            self.send_error(403, e)

        path, _ = parse.splitquery(self.path)
        datasets = cgi.FieldStorage(fp = self.rfile,headers = self.headers,environ = {'REQUEST_METHOD': 'POST'})

        if path == '/dtable-asset-files':
            if task_manager.tasks_queue.full():
                self.send_error(400, 'dtable io server busy.')
                return

            username = datasets.getvalue('username')
            repo_id = datasets.getvalue('repo_id')
            dtable_uuid = datasets.getvalue('dtable_uuid')
            files = datasets.getvalue('files')
            files_map = datasets.getvalue('files_map')
            if not isinstance(files, list):
                files = [files]
            if not isinstance(files_map, dict):
                files_map = json.loads(files_map)
            try:
                task_id = task_manager.add_export_dtable_asset_files_task(
                    username,
                    repo_id,
                    dtable_uuid,
                    files,
                    files_map,
                )
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {'task_id': task_id}
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/transfer-dtable-asset-files':
            if task_manager.tasks_queue.full():
                self.send_error(400, 'dtable io server busy.')
                return

            username = datasets.getvalue('username')
            repo_id = datasets.getvalue('repo_id')
            dtable_uuid = datasets.getvalue('dtable_uuid')
            files = datasets.getvalue('files')
            files_map = datasets.getvalue('files_map')
            repo_api_token = datasets.getvalue('repo_api_token')
            seafile_server_url = datasets.getvalue('seafile_server_url')
            parent_dir = datasets.getvalue('parent_dir')
            relative_path = datasets.getvalue('relative_path')
            replace = datasets.getvalue('replace')
            if not isinstance(files, list):
                files = [files]
            if not isinstance(files_map, dict):
                files_map = json.loads(files_map)
            try:
                task_id = task_manager.add_transfer_dtable_asset_files_task(
                    username,
                    repo_id,
                    dtable_uuid,
                    files,
                    files_map,
                    parent_dir,
                    relative_path,
                    replace,
                    repo_api_token,
                    seafile_server_url,
                )
            except Exception as e:
                logger.error(e)
                self.send_error(500)
                return

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {'task_id': task_id}
            self.wfile.write(json.dumps(resp).encode('utf-8'))
        else:
            self.send_error(400, 'path %s invalid.' % path)