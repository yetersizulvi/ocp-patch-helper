import copy
import json
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import yaml
from fastapi.testclient import TestClient
from app.central import create_app
from app.central_config import Cluster, Config, Flow, Images, Storage
from app.central_collect import Collector, ScanError, health, matches, demo_rows
from app.central_store import Store, Conflict


class CentralAPITests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name)/'config.yaml'
        self.path.write_text(yaml.safe_dump({'clusters':[{'id':'a'},{'id':'b'}], 'demo':True,
            'flows':{'patch-live':{'interval_seconds':1}},
            'storage':{'database_path':str(Path(self.tmp.name)/'state.db'),'max_database_mb':8}}))
        self.app = create_app(str(self.path))
        self.client = TestClient(self.app)
        self.client.__enter__()

    def tearDown(self):
        self.client.__exit__(None,None,None)
        self.tmp.cleanup()

    def session(self):
        r = self.client.post('/api/v1/sessions',json={'target_tag':'1.4.1'})
        self.assertEqual(201,r.status_code,r.text)
        self.sid = r.json()['id']
        self.base = '/api/v1/sessions/'+self.sid
        return self.sid

    def wait_state(self, status):
        until=time.time()+5
        while time.time()<until:
            s=self.client.get(self.base).json()
            if s['status']==status:return s
            time.sleep(.02)
        self.fail(s)

    def baseline(self):
        self.session()
        self.assertEqual(202,self.client.post(self.base+'/baseline').status_code)
        self.wait_state('BASELINE_READY')

    def test_live_regression_recovery_old_version_and_immutable_baseline(self):
        self.baseline()
        self.assertEqual(409,self.client.post(self.base+'/baseline').status_code)
        self.assertEqual(200,self.client.post(self.base+'/start').status_code)
        until=time.time()+5
        while time.time()<until:
            counts=self.client.get(self.base+'/summary').json()['change_counts']
            if counts.get('RECOVERED')==2: break
            time.sleep(.03)
        self.assertEqual(2,counts['REGRESSION'])
        self.assertEqual(2,counts['RECOVERED'])
        self.assertEqual(2,counts['OLD_VERSION'])
        rows=self.client.get(self.base+'/targets?health=CRASH').json()['items']
        self.assertEqual(2,len(rows))
        detail=self.client.get(self.base+'/containers/'+rows[0]['id']).json()
        self.assertEqual('READY',detail['baseline'][0]['health'])
        self.assertEqual('CRASH',detail['current']['health'])
        self.assertIsNone(detail['restart_delta'])
        old=self.client.get(self.base+'/images?target_match=false').json()['items']
        self.assertTrue(all(r['image_tag']=='1.4.0' for r in old))
        self.assertEqual(200,self.client.post(self.base+'/stop').status_code)

    def test_validation_scopes_paging_and_no_token_content(self):
        self.assertEqual(422,self.client.post('/api/v1/sessions',json={'target_tag':'1.4.1','clusters':['unknown']}).status_code)
        self.assertEqual(422,self.client.post('/api/v1/sessions',json={'target_tag':'1.4.1','flow':'missing'}).status_code)
        self.baseline()
        first=self.client.get(self.base+'/images?limit=3').json()
        second=self.client.get(self.base+'/images?limit=3&cursor='+first['next_cursor']).json()
        self.assertFalse({r['id'] for r in first['items']}&{r['id'] for r in second['items']})
        self.assertNotIn('token_file',self.client.get('/api/v1/config').text)
        self.assertEqual(422,self.client.get(self.base+'/images?limit=999').status_code)
        self.assertEqual(403,self.client.post(self.base+'/stop',headers={'Origin':'https://evil.example.com'}).status_code)

    def test_failed_cluster_cannot_start_and_retry_does_not_replace_successful_baseline(self):
        self.session()
        actual=demo_rows
        def broken(cluster,target,tick):
            if cluster.id=='b':raise ScanError('API_HTTP_403')
            yield from actual(cluster,target,tick)
        with patch('app.central.demo_rows',broken):
            self.client.post(self.base+'/baseline')
            s=self.wait_state('DRAFT')
        self.assertEqual([1,0],[c['baseline'] for c in s['clusters']])
        self.assertEqual(409,self.client.post(self.base+'/start').status_code)
        self.client.post(self.base+'/baseline')
        self.wait_state('BASELINE_READY')
        self.assertEqual(8,len(self.client.get(self.base+'/images').json()['items']))

    def test_failed_scan_keeps_last_good_data_and_marks_stale(self):
        self.baseline()
        before=self.client.get(self.base+'/images').json()['items']
        with patch('app.central.demo_rows',side_effect=ScanError('API_TIMEOUT')):
            self.client.post(self.base+'/start')
            until=time.time()+5
            while time.time()<until:
                d=self.client.get(self.base+'/images').json()
                if all(c['error'] for c in d['clusters']):break
                time.sleep(.02)
            self.assertEqual(before,d['items'])
            self.assertEqual('STALE',d['freshness'])
            self.client.post(self.base+'/stop')

    def test_restart_preserves_baseline_and_interrupts_running_session(self):
        self.baseline()
        self.app.state.monitor.store.status(self.sid,'RUNNING',time.time()+3600)
        store=Store(self.app.state.monitor.config.storage)
        try:
            self.assertEqual('INTERRUPTED',store.session(self.sid)['status'])
            self.assertTrue(all(c['baseline'] for c in store.session(self.sid)['clusters']))
        finally:store.db.close()

    def test_expiry_rejects_late_publish_and_keeps_baseline(self):
        self.baseline()
        self.client.post(self.base+'/start')
        monitor=self.app.state.monitor
        monitor.store.status(self.sid,'RUNNING',time.time()-1)
        self.wait_state('COMPLETED')
        with self.assertRaises(Conflict):
            monitor.store.publish(self.sid,'a')
        self.assertTrue(all(c['baseline'] for c in monitor.store.session(self.sid)['clusters']))

    def test_stop_during_scan_does_not_publish_partial_results(self):
        self.baseline()
        monitor=self.app.state.monitor
        entered=threading.Event()
        def slow(cluster,target,tick):
            entered.set()
            while not monitor.stop_event.wait(.01):
                pass
            yield from demo_rows(cluster,target,tick)
        with patch('app.central.demo_rows',slow):
            self.client.post(self.base+'/start')
            self.assertTrue(entered.wait(2))
            self.assertEqual(200,self.client.post(self.base+'/stop').status_code)
        rows=self.client.get(self.base+'/images').json()['items']
        self.assertTrue(all(r['image_tag']=='1.4.0' for r in rows))
        self.assertTrue(all(t.done() for t in monitor.tasks))

    def test_session_freezes_flow_and_scope(self):
        self.session()
        monitor=self.app.state.monitor
        monitor.config.flows['patch-live'].interval_seconds=30
        monitor.config.clusters=[Cluster(id='replacement')]
        frozen=monitor.store.session(self.sid)['config']
        self.assertEqual(1,frozen['flows']['patch-live']['interval_seconds'])
        self.assertEqual(['a','b'],[c['id'] for c in frozen['clusters']])

    def test_disk_quota_rollback_preserves_baseline(self):
        self.baseline()
        store=self.app.state.monitor.store
        current=store.page(self.sid)['items']
        import sqlite3
        with store.lock:
            pages=store.db.execute('PRAGMA page_count').fetchone()[0]
            store.db.execute(f'PRAGMA max_page_count={pages+2}')
        row=copy.deepcopy(current[0]);row['reason']='x'*100000
        with self.assertRaises(sqlite3.DatabaseError):
            store.stage(self.sid,'a',[row])
        self.assertEqual(current,store.page(self.sid)['items'])
        self.assertTrue(all(c['baseline'] for c in store.session(self.sid)['clusters']))

    def test_missing_session_and_start_before_baseline(self):
        self.session()
        self.assertEqual(409,self.client.post(self.base+'/start').status_code)
        self.assertEqual(404,self.client.get('/api/v1/sessions/missing/images').status_code)
        self.assertEqual(409,self.client.post('/api/v1/sessions',json={'target_tag':'1.4.2'}).status_code)


class FakeResponse:
    def __init__(self,data,code=200):self.data=data;self.status_code=code
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def iter_content(self,*args):yield json.dumps(self.data).encode()

class FakeHTTP:
    def __init__(self,pages):self.pages=iter(pages);self.calls=[]
    def get(self,url,**kwargs):self.calls.append((url,kwargs));return next(self.pages)

class CollectorTests(unittest.TestCase):
    def test_running_phase_is_not_ready_and_previous_oom_does_not_mean_current_crash(self):
        pod={'status':{'phase':'Running'}}
        self.assertEqual('CRASH',health(pod,{'state':{'waiting':{'reason':'CrashLoopBackOff'}}})[0])
        self.assertEqual('READY',health(pod,{'ready':True,'state':{'running':{}},'lastState':{'terminated':{'reason':'OOMKilled'}}})[0])

    def test_tag_policy_and_registry_port_digest(self):
        self.assertTrue(matches('host:5000/app:1.4.1','1.4.1',Images()))
        self.assertFalse(matches('host:5000/app:1.4.10','1.4.1',Images()))
        self.assertFalse(matches('host/app@sha256:abc','1.4.1',Images()))
        self.assertTrue(matches('host/app:1.4.1-hash','1.4.1',Images(tag_match_mode='release')))

    def test_pagination_token_rotation_http_errors_and_bounded_response(self):
        with tempfile.TemporaryDirectory() as d:
            token=Path(d)/'token';token.write_text('first')
            c=Collector(Cluster(id='a',token_file=str(token)),Flow(),Images(),'1.4.1',threading.Event())
            http=FakeHTTP([FakeResponse({'items':[{'id':1}],'metadata':{'continue':'next'}}),FakeResponse({'items':[{'id':2}]})])
            it=c.listed(http,'/api/v1/pods')
            self.assertEqual({'id':1},next(it));token.write_text('rotated')
            self.assertEqual({'id':2},next(it))
            self.assertEqual('next',http.calls[1][1]['params']['continue'])
            self.assertEqual('Bearer rotated',http.calls[1][1]['headers']['Authorization'])
            with self.assertRaisesRegex(ScanError,'API_HTTP_410'):
                list(c.listed(FakeHTTP([FakeResponse({},410)]),'/pods'))
            c.flow.max_page_bytes=1024
            with self.assertRaisesRegex(ScanError,'PAGE_BYTE_LIMIT'):
                list(c.listed(FakeHTTP([FakeResponse({'items':['x'*2048]})]),'/pods'))

    def test_http_is_only_explicit_proxy_configuration(self):
        with self.assertRaises(ValueError):Cluster(id='a',api_url='http://kubernetes.default.svc')
        self.assertEqual('http://proxy:8080',Cluster(id='a',api_url='http://proxy:8080',allow_http=True).api_url)
        with self.assertRaises(ValueError):Cluster(id='remote',connection='remote')

    def test_deployment_owner_resolution_and_waiting_placeholder(self):
        c=Collector(Cluster(id='a'),Flow(resources=['deployments']),Images(),'1.4.1',threading.Event())
        data={
          '/api/v1/namespaces':[{'metadata':{'name':'test-app'}}],
          '/apis/apps/v1/namespaces/test-app/deployments':[{'metadata':{'name':'app','uid':'dep','generation':2},'spec':{'replicas':1,'template':{'spec':{'containers':[{'name':'app','image':'r/app:1.4.1'}]}}},'status':{'observedGeneration':2,'updatedReplicas':0,'readyReplicas':1}}],
          '/apis/apps/v1/namespaces/test-app/replicasets':[{'metadata':{'uid':'rs','ownerReferences':[{'uid':'dep','controller':True}]}}],
          '/api/v1/namespaces/test-app/pods':[{'metadata':{'uid':'pod','name':'app-old','ownerReferences':[{'uid':'rs','controller':True}]},'spec':{'containers':[{'name':'app','image':'r/app:1.4.0'}]},'status':{'phase':'Running','containerStatuses':[{'name':'app','ready':True,'state':{'running':{}}}]}}]
        }
        with patch.object(c,'listed',side_effect=lambda _,path:iter(data[path])):
            rows=list(c.rows())
        self.assertEqual('dep',rows[0]['workload_uid'])
        self.assertEqual('1.4.0',rows[0]['image_tag'])
        self.assertEqual('r/app:1.4.1',rows[0]['desired_image'])
        self.assertEqual('WAITING_FOR_POD',rows[1]['health'])
        self.assertEqual('desired',rows[1]['row_type'])

if __name__=='__main__':unittest.main()
