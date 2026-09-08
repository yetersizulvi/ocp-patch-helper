import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch
import yaml
from fastapi.testclient import TestClient
from app.central import create_app
from app.central_collect import demo_rows, Collector
from app.central_config import Cluster, Flow, Images, Scope
import threading

class RevisionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.cfg={'clusters':[{'id':'a'},{'id':'b'}],'demo':True,'flows':{'patch-live':{'interval_seconds':1}},'storage':{'database_path':self.tmp.name+'/db','max_database_mb':8}}
        p=Path(self.tmp.name)/'config';p.write_text(yaml.safe_dump(self.cfg))
        self.app=create_app(str(p));self.client=TestClient(self.app);self.client.__enter__()
    def tearDown(self):
        self.client.__exit__(None,None,None);self.tmp.cleanup()
    def capture(self,**extra):
        r=self.client.post('/api/v1/sessions',json={'target_tag':'1.4.1',**extra});self.assertEqual(201,r.status_code,r.text)
        self.sid=r.json()['id'];self.base='/api/v1/sessions/'+self.sid
        self.client.post(self.base+'/baseline')
        for _ in range(150):
            if self.client.get(self.base).json()['status']=='BASELINE_READY':return
            time.sleep(.01)
        self.fail('baseline timeout')
    def test_filters_are_sql_backed_and_summary_matches_both_tables(self):
        self.capture()
        store=self.app.state.monitor.store
        with store.lock,store.db:
            # Create uat and mesh rows across actual persisted snapshots.
            store.db.execute("UPDATE rows SET namespace='uat-demo' WHERE workload_uid='legacy'")
            store.db.execute("UPDATE rows SET container='istio-proxy' WHERE workload_uid='checkout'")
        q='cluster=a&namespace_glob=test-*&hide_infrastructure=true'
        rows=self.client.get(self.base+'/images?'+q).json()['items']
        counts=self.client.get(self.base+'/summary?'+q).json()['counts']
        changes=self.client.get(self.base+'/changes?'+q).json()['items']
        self.assertEqual(2,len(rows));self.assertEqual(2,sum(r['count'] for r in counts));self.assertEqual(2,len(changes))
        self.assertTrue(all(r['cluster']=='a' for r in changes))
        self.assertEqual(2,len(self.client.get(self.base+'/images?namespace=uat-demo').json()['items']))
        self.assertEqual(['test-demo','uat-demo'],self.client.get(self.base+'/facets?cluster=a').json()['namespaces'])
    def test_image_search_preserves_before_after_group(self):
        self.capture()
        store=self.app.state.monitor.store
        with store.lock,store.db:
            store.db.execute("UPDATE rows SET image='r/payments:1.4.1',target_match=1 WHERE layer='current' AND workload_uid='payments'")
        result=self.client.get(self.base+'/changes?search=1.4.1').json()['items']
        self.assertEqual(2,len(result))
        self.assertTrue(all(r['classification']=='IMAGE_CHANGED' for r in result))
        self.assertTrue(all('1.4.0' in r['before_images'] for r in result))
    def test_new_session_scope_restricts_baseline_and_is_persisted(self):
        self.capture(namespace_glob='uat-*')
        self.assertEqual([],self.client.get(self.base+'/images').json()['items'])
        self.assertEqual('uat-*',self.client.get(self.base).json()['scope']['namespace_glob'])
    def test_flow_preview_save_and_limits(self):
        body={'target_tag':'1.4.1','clusters':['a'],'namespace_glob':'test-*','interval_seconds':10,'tag_match_mode':'release'}
        r=self.client.post('/api/v1/flows/preview',json=body);self.assertEqual(200,r.status_code)
        self.assertEqual('release',r.json()['tag_match_mode']);self.assertEqual(['a'],r.json()['clusters'])
        self.assertEqual([],self.client.get('/api/v1/sessions').json()['items'])
        self.assertEqual(200,self.client.post('/api/v1/flows/designs',json={'name':'test','description':'Aylık patch','settings':body}).status_code)
        saved=self.client.get('/api/v1/flows').json()['designs'];self.assertEqual(10,saved[0]['settings']['interval_seconds'])
        self.assertEqual(422,self.client.post('/api/v1/flows/preview',json={**body,'clusters':['missing']}).status_code)
        self.app.state.monitor.config.flows['patch-live'].interval_seconds=5
        self.assertEqual(422,self.client.post('/api/v1/flows/preview',json={**body,'interval_seconds':1}).status_code)
    def test_namespace_scope_cannot_expand_admin_regex(self):
        c=Collector(Cluster(id='a',namespace_pattern='^test-'),Flow(resources=['deployments']),Images(),'1.4.1',threading.Event(),Scope(namespace_glob='*'))
        paths=[]
        def listed(_,path):
            paths.append(path)
            if path=='/api/v1/namespaces':return iter([{'metadata':{'name':'test-a'}},{'metadata':{'name':'prod-a'}}])
            return iter([])
        with patch.object(c,'listed',side_effect=listed):list(c.rows())
        self.assertTrue(any('test-a' in p for p in paths));self.assertFalse(any('prod-a' in p for p in paths))
    def test_unknown_tag_count_and_frontend_served(self):
        self.capture()
        store=self.app.state.monitor.store
        with store.lock,store.db:
            store.db.execute("UPDATE rows SET payload=json_set(payload,'$.image_tag',NULL) WHERE workload_uid='legacy'")
        counts=self.client.get(self.base+'/summary').json()['counts']
        self.assertEqual(2,sum(r['count'] for r in counts if not r['tag_known']))
        changes=self.client.get(self.base+'/changes?classification=UNKNOWN_VERSION').json()['items']
        self.assertEqual(2,len(changes))
        self.assertEqual(6,len(self.client.get(self.base+'/images?known_tag_only=true').json()['items']))
        self.assertIn('Akış tasarla',self.client.get('/').text)
        self.assertIn('renderSession',self.client.get('/ui.js').text)
        self.assertIn('javascript',self.client.get('/ui.js').headers['content-type'])

    def test_multiple_patterns_and_independent_change_axes(self):
        self.capture(namespace_glob='test-*, uat-*,test-*')
        self.assertEqual('test-*,uat-*',self.client.get(self.base).json()['scope']['namespace_glob'])
        store=self.app.state.monitor.store
        with store.lock,store.db:
            store.db.execute("UPDATE rows SET namespace='uat-demo' WHERE workload_uid='legacy'")
        result=self.client.get(self.base+'/changes?cluster=a&namespace_glob=test-*,uat-*').json()['items']
        self.assertEqual(4,len(result))
        self.assertTrue(all(r['cluster']=='a' for r in result))
        errors=[r for r in result if r['health_change']=='PERSISTING_ERROR']
        self.assertTrue(errors)
        self.assertEqual('NOT_UPDATED',errors[0]['version_status'])
        selected=self.client.get(self.base+'/changes?cluster=a&version_status=NOT_UPDATED&health_change=PERSISTING_ERROR').json()['items']
        self.assertEqual(errors,selected)
        for invalid in ['test-*,,uat-*','test-[abc]','test-*;prod-*']:
            self.assertEqual(422,self.client.get(self.base+'/images',params={'namespace_glob':invalid}).status_code)
        self.assertEqual(200,self.client.post('/api/v1/flows/preview',json={'target_tag':'1.4.1','namespace_glob':'test-*,uat-*'}).status_code)

    def test_retention_uses_closed_time_preserves_active_and_designs(self):
        self.capture()
        store=self.app.state.monitor.store
        store.save_design('keep','keep',{'target_tag':'1.4.1'})
        with store.lock,store.db:
            store.db.execute('UPDATE sessions SET created=?',(time.time()-30*86400,))
        self.assertEqual(0,store.cleanup())
        store.status(self.sid,'STOPPED')
        self.assertEqual(0,store.cleanup())
        with store.lock,store.db:
            store.db.execute('UPDATE sessions SET closed_at=?',(time.time()-15*86400,))
        self.assertEqual(1,store.cleanup())
        self.assertEqual(0,store.db.execute('SELECT count(*) FROM rows').fetchone()[0])
        self.assertEqual(0,store.db.execute('SELECT count(*) FROM clusters').fetchone()[0])
        self.assertTrue(store.designs())
