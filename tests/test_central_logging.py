import json
import logging
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
import requests
import yaml
from app.central_logging import configure, emit
from app.central_collect import Collector, ScanError
from app.central_config import Cluster, Flow, Images
from app.central import Monitor

class Response:
    def __init__(self,status=200):self.status_code=status
    def __enter__(self):return self
    def __exit__(self,*a):pass
    def iter_content(self,*a):yield b'{"items":[],"metadata":{},"secret_value":"DO_NOT_LOG_BODY"}'

class LoggingTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.token=Path(self.tmp.name)/'token';self.token.write_text('DO_NOT_LOG_TOKEN')
        self.cluster=Cluster(id='test-remote',token_file=str(self.token))
        configure()
    def tearDown(self):self.tmp.cleanup()
    def collector(self):return Collector(self.cluster,Flow(),Images(),'1.4.1',threading.Event(),session_id='session',scan_id='scan')
    def test_debug_records_path_and_status_without_token_or_body(self):
        http=unittest.mock.Mock();http.get.return_value=Response()
        with self.assertLogs('patch.monitor',level='DEBUG') as logs:
            list(self.collector().listed(http,'/api/v1/namespaces'))
        messages=[json.loads(r.getMessage()) for r in logs.records]
        self.assertEqual(['api_request_started','api_connected','api_page_received'],[m['event'] for m in messages])
        self.assertEqual('test-remote',messages[-1]['cluster']);self.assertEqual(200,messages[-1]['status'])
        self.assertNotIn('DO_NOT_LOG',str(messages));self.assertNotIn('Authorization',str(messages))
    def test_forbidden_resource_is_identified_and_not_connected(self):
        http=unittest.mock.Mock();http.get.return_value=Response(403)
        with self.assertLogs('patch.monitor',level='DEBUG') as logs:
            with self.assertRaisesRegex(ScanError,'API_HTTP_403'):
                list(self.collector().listed(http,'/apis/apps/v1/namespaces/test-a/deployments'))
        data=[json.loads(r.getMessage()) for r in logs.records]
        self.assertEqual('api_request_failed',data[-1]['event']);self.assertEqual('deployments',data[-1]['resource'])
        self.assertEqual('test-a',data[-1]['namespace']);self.assertFalse(any(r['event']=='api_connected' for r in data))
    def test_tls_timeout_and_file_errors_are_sanitized(self):
        for exception,code in [(requests.exceptions.SSLError('DO_NOT_LOG_TOKEN'),'TLS_ERROR'),(requests.exceptions.Timeout('DO_NOT_LOG_TOKEN'),'API_TIMEOUT'),(requests.exceptions.ConnectionError('DO_NOT_LOG_TOKEN'),'CONNECTION_ERROR'),(OSError('DO_NOT_LOG_TOKEN'),'CA_FILE_UNREADABLE')]:
            http=unittest.mock.Mock();http.get.side_effect=exception
            with self.assertRaisesRegex(ScanError,code) as raised:list(self.collector().listed(http,'/api/v1/namespaces'))
            self.assertNotIn('DO_NOT_LOG',str(raised.exception))
    def test_operational_info_and_field_allowlist(self):
        with self.assertLogs('patch.monitor',level='INFO') as logs:
            emit('DEBUG','api_request_started',cluster='a')
            emit('INFO','cluster_state',cluster='a',scanning=False,token='DO_NOT_LOG_TOKEN',headers={'Authorization':'secret'})
        data=[json.loads(r.getMessage()) for r in logs.records]
        self.assertEqual(1,len(data));self.assertFalse(data[0]['scanning']);self.assertNotIn('token',data[0]);self.assertNotIn('headers',data[0])
    def test_scan_summary_is_published_and_demo_never_claims_api_connection(self):
        config=Path(self.tmp.name)/'config.yaml'
        config.write_text(yaml.safe_dump({'clusters':[{'id':'a'}],'demo':True,'storage':{'database_path':self.tmp.name+'/db'}}))
        m=Monitor(str(config))
        try:
            m.store.create('s','1.4.1','patch-live',m.config.model_dump(),60)
            m.store.status('s','CAPTURING')
            with self.assertLogs('patch.monitor',level='INFO') as logs:
                self.assertTrue(m.scan('s',m.config.clusters[0],m.config,True,0))
            data=[json.loads(r.getMessage()) for r in logs.records]
            self.assertEqual(['scan_started','scan_completed'],[r['event'] for r in data]);self.assertEqual(4,data[-1]['rows']);self.assertTrue(data[-1]['published']);self.assertTrue(data[-1]['demo'])
        finally:m.store.db.close()
