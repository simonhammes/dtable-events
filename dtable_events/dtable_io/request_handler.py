import json
import jwt
from urllib import parse
from http.server import SimpleHTTPRequestHandler
from dtable_events.dtable_io.TaskManager import task_manager



class DTableIORequestHandler(SimpleHTTPRequestHandler):

    def do_GET(self):

        auth = self.headers['Authorization'].split()
        if not auth or auth[0].lower() != 'token' or len(auth) != 2:
            self.send_error(403, 'Token invalid.')
        token = auth[1]
        if not token:
            self.send_error(403, 'Token invalid.')
        from dtable_events.app.config import global_dtable_server_conf
        private_key = global_dtable_server_conf.get('private_key','')
        try:
            jwt.decode(token, private_key, algorithms=['HS256'])
        except (jwt.ExpiredSignatureError, jwt.InvalidSignatureError) as e:
            self.send_error(403, e)

        path, arguments = parse.splitquery(self.path)
        arguments = parse.parse_qs(arguments)
        if path == '/add-export-task':
            username = arguments['username'][0]
            table_name = arguments['table_name'][0]
            repo_id = arguments['repo_id'][0]
            dtable_id = arguments['dtable_id'][0]
            dtable_file_dir_id = arguments['dtable_file_dir_id'][0]
            asset_dir_id = None
            if 'asset_dir_id' in arguments.keys():
                asset_dir_id = arguments['asset_dir_id'][0]

            task_id = task_manager.add_export_task(
                username,
                table_name,
                repo_id,
                dtable_id,
                dtable_file_dir_id,
                asset_dir_id
            )

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            resp = {'task_id': task_id}
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        elif path == '/add-import-task':

            username = arguments['username'][0]
            repo_id = arguments['repo_id'][0]
            workspace_id = arguments['workspace_id'][0]
            dtable_id = arguments['dtable_id'][0]
            dtable_uuid = arguments['dtable_uuid'][0]
            dtable_file_name = arguments['dtable_file_name'][0]
            uploaded_temp_path = arguments['uploaded_temp_path'][0]

            task_id = task_manager.add_import_task(
                username,
                repo_id,
                workspace_id,
                dtable_id,
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

            is_finished = task_manager.query_status(task_id)

            self.send_response(200)
            self.end_headers()
            resp = {}
            resp['is_finished'] = is_finished
            self.wfile.write(json.dumps(resp).encode('utf-8'))

        else:
            self.send_error(400, 'path %s invalid.' % path)