import json
import jwt
from urllib import parse
from http.server import SimpleHTTPRequestHandler
from dtable_events.dtable_io.task_manager import task_manager


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

            if task_manager.is_workers_maxed():
                self.send_error(400, 'dtable io server bussy.')
                return

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            table_name = arguments['table_name'][0]
            dtable_uuid = arguments['dtable_uuid'][0]

            task_id = task_manager.add_export_task(
                username,
                repo_id,
                dtable_uuid,
                table_name,
            )

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {'task_id': task_id}
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/add-import-task':

            if task_manager.is_workers_maxed():
                self.send_error(400, 'dtable io server bussy.')
                return

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            workspace_id = arguments['workspace_id'][0]
            dtable_uuid = arguments['dtable_uuid'][0]
            dtable_file_name = arguments['dtable_file_name'][0]
            uploaded_temp_path = arguments['uploaded_temp_path'][0]

            task_id = task_manager.add_import_task(
                username,
                repo_id,
                workspace_id,
                dtable_uuid,
                dtable_file_name,
                uploaded_temp_path
            )

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
            except Exception:
                self.send_error(500)

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
                self.send_error(500)

            self.send_response(200)
            self.end_headers()
            resp = {'success': True}
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        else:
            self.send_error(400, 'path %s invalid.' % path)