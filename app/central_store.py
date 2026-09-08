"""SQLite holds staged, baseline and current rows; no snapshot history in RAM."""
from __future__ import annotations
import json
import sqlite3
import threading
import time
from pathlib import Path
from app.namespace_patterns import split_patterns

def scope_sql(cluster=None, namespace=None, namespace_glob=None, search=None, hide_infrastructure=False, known_tag_only=False):
    where, args = [], []
    for column, value in [('cluster',cluster), ('namespace',namespace)]:
        if value:
            where.append(column+'=?'); args.append(value)
    if namespace_glob:
        patterns=split_patterns(namespace_glob)
        where.append('('+ ' OR '.join('namespace GLOB ?' for _ in patterns)+')'); args.extend(patterns)
    if search:
        where.append("(instr(lower(image),lower(?))>0 OR instr(lower(json_extract(payload,'$.workload')),lower(?))>0 OR instr(lower(container),lower(?))>0)")
        args.extend([search]*3)
    if known_tag_only:
        where.append("json_extract(payload,'$.image_tag') IS NOT NULL")
    if hide_infrastructure:
        where.append("container NOT IN ('istio-proxy','linkerd-proxy')")
    return (' AND '+ ' AND '.join(where) if where else ''), args

BAD = ('CRASH', 'IMAGE_PULL_ERROR', 'ERROR')

class Conflict(Exception):
    pass

class Store:
    def __init__(self, settings):
        Path(settings.database_path).parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(settings.database_path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        # DELETE journaling keeps total disk usage predictable; no unbounded WAL file.
        self.db.executescript('''
            PRAGMA journal_mode=DELETE;
            PRAGMA cache_size=-4096;
            PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS designs (name TEXT PRIMARY KEY, description TEXT, body TEXT);
            CREATE TABLE IF NOT EXISTS sessions (
              id TEXT PRIMARY KEY, status TEXT, target TEXT, flow TEXT, config TEXT,
              created REAL, ends REAL, duration INTEGER, revision INTEGER DEFAULT 0);
            CREATE TABLE IF NOT EXISTS clusters (
              session TEXT REFERENCES sessions(id) ON DELETE CASCADE, cluster TEXT,
              baseline INTEGER DEFAULT 0, observed REAL, error TEXT, PRIMARY KEY(session,cluster));
            CREATE TABLE IF NOT EXISTS rows (
              session TEXT REFERENCES sessions(id) ON DELETE CASCADE, cluster TEXT, layer TEXT,
              id TEXT, namespace TEXT, workload_uid TEXT, container TEXT, image TEXT,
              target_match INTEGER, health TEXT, row_type TEXT, payload TEXT,
              PRIMARY KEY(session,cluster,layer,id));
            CREATE INDEX IF NOT EXISTS row_filter ON rows(session,layer,target_match,health);
            CREATE INDEX IF NOT EXISTS row_group ON rows(session,layer,cluster,namespace,workload_uid,container);
        ''')
        columns={r[1] for r in self.db.execute('PRAGMA table_info(sessions)')}
        if 'closed_at' not in columns:
            self.db.execute('ALTER TABLE sessions ADD COLUMN closed_at REAL')
        self.db.execute("UPDATE sessions SET closed_at=COALESCE(ends,created) WHERE closed_at IS NULL AND status IN ('STOPPED','COMPLETED','INTERRUPTED')")
        self.db.commit()
        page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
        self.db.execute(f'PRAGMA max_page_count={settings.max_database_mb*1024*1024//page_size}')
        self.retention = settings.retention_days * 86400
        with self.db:
            self.db.execute("UPDATE sessions SET status='INTERRUPTED',closed_at=? WHERE status IN ('RUNNING','CAPTURING','STOPPING')",(time.time(),))
            self.db.execute("DELETE FROM rows WHERE layer='stage'")

    def cleanup(self):
        with self.lock, self.db:
            result=self.db.execute("DELETE FROM sessions WHERE closed_at<? AND status IN ('STOPPED','COMPLETED','INTERRUPTED')", (time.time()-self.retention,))
            return result.rowcount

    def session(self, sid):
        with self.lock:
            r = self.db.execute('SELECT * FROM sessions WHERE id=?', (sid,)).fetchone()
            if r is None:
                raise KeyError(sid)
            value = dict(r)
            value['config'] = json.loads(value['config'])
            value['clusters'] = [dict(c) for c in self.db.execute('SELECT * FROM clusters WHERE session=?', (sid,))]
            return value

    def sessions(self):
        with self.lock:
            return [dict(r) for r in self.db.execute('SELECT id,status,target,flow,created,ends,revision FROM sessions ORDER BY created DESC LIMIT 100')]

    def create(self, sid, target, flow, config, duration):
        self.cleanup()
        with self.lock, self.db:
            if self.db.execute("SELECT 1 FROM sessions WHERE status IN ('RUNNING','CAPTURING','DRAFT','BASELINE_READY','STOPPING')").fetchone():
                raise Conflict('Another session is active; stop it first')
            self.db.execute('INSERT INTO sessions(id,status,target,flow,config,created,duration) VALUES(?,?,?,?,?,?,?)',
                            (sid,'DRAFT',target,flow,json.dumps(config),time.time(),duration))
            self.db.executemany('INSERT INTO clusters(session,cluster) VALUES(?,?)', [(sid,c['id']) for c in config['clusters']])
        return self.session(sid)

    def status(self, sid, status, ends=None):
        with self.lock, self.db:
            self.db.execute('UPDATE sessions SET status=?, ends=COALESCE(?,ends), closed_at=?, revision=revision+1 WHERE id=?', (status,ends,time.time() if status in ('STOPPED','COMPLETED','INTERRUPTED') else None,sid))

    def begin_stage(self, sid, cluster):
        with self.lock, self.db:
            self.db.execute("DELETE FROM rows WHERE session=? AND cluster=? AND layer='stage'", (sid,cluster))

    def stage(self, sid, cluster, batch):
        with self.lock, self.db:
            self.db.executemany('INSERT INTO rows VALUES(?,?,?,?,?,?,?,?,?,?,?,?)', [
                (sid,cluster,'stage',r['id'],r['namespace'],r['workload_uid'],r['container'],r['image'],
                 int(r['target_match']),r['health'],r['row_type'],json.dumps(r,separators=(',',':'))) for r in batch])

    def publish(self, sid, cluster, baseline=False):
        with self.lock, self.db:
            state = self.db.execute('SELECT status FROM sessions WHERE id=?', (sid,)).fetchone()[0]
            if state not in ('CAPTURING','RUNNING'):
                raise Conflict('Session is no longer collecting')
            if baseline and self.db.execute('SELECT baseline FROM clusters WHERE session=? AND cluster=?',(sid,cluster)).fetchone()[0]:
                raise Conflict('Baseline is immutable')
            self.db.execute("DELETE FROM rows WHERE session=? AND cluster=? AND layer='current'", (sid,cluster))
            self.db.execute("UPDATE rows SET layer='current' WHERE session=? AND cluster=? AND layer='stage'", (sid,cluster))
            if baseline:
                self.db.execute("INSERT INTO rows SELECT session,cluster,'baseline',id,namespace,workload_uid,container,image,target_match,health,row_type,payload FROM rows WHERE session=? AND cluster=? AND layer='current'", (sid,cluster))
            self.db.execute('UPDATE clusters SET baseline=MAX(baseline,?),observed=?,error=NULL WHERE session=? AND cluster=?', (int(baseline),time.time(),sid,cluster))
            self.db.execute('UPDATE sessions SET revision=revision+1 WHERE id=?', (sid,))

    def error(self, sid, cluster, message):
        with self.lock, self.db:
            self.db.execute('UPDATE clusters SET error=? WHERE session=? AND cluster=?', (message,sid,cluster))
            self.db.execute('UPDATE sessions SET revision=revision+1 WHERE id=?', (sid,))
            self.db.execute("DELETE FROM rows WHERE session=? AND cluster=? AND layer='stage'", (sid,cluster))

    def page(self, sid, target=None, health=None, cluster=None, limit=50, cursor='', inventory=False, **filters):
        with self.lock:
            where, args = ["session=?", "layer='current'", 'id>?'], [sid,cursor]
            if inventory:
                where.append("row_type='pod'")
            for col, value in [('target_match',target),('health',health),('cluster',cluster)]:
                if col=='health' and value=='ERRORS':
                    where.append("health IN ('CRASH','IMAGE_PULL_ERROR','ERROR')")
                    continue
                if value is not None:
                    where.append(col+'=?')
                    args.append(value)
            extra, values = scope_sql(**filters)
            found = self.db.execute('SELECT payload FROM rows WHERE '+' AND '.join(where)+extra+' ORDER BY id LIMIT ?', args+values+[limit+1]).fetchall()
            items = [json.loads(r[0]) for r in found[:limit]]
            return {'items':items, 'next_cursor':items[-1]['id'] if len(found)>limit else None}

    def detail(self, sid, rid):
        with self.lock:
            row = self.db.execute("SELECT * FROM rows WHERE session=? AND layer='current' AND id=?",(sid,rid)).fetchone()
            if row is None:
                raise KeyError(rid)
            baseline = self.db.execute("SELECT payload FROM rows WHERE session=? AND cluster=? AND layer='baseline' AND namespace=? AND workload_uid=? AND container=? LIMIT 100", (sid,row['cluster'],row['namespace'],row['workload_uid'],row['container'])).fetchall()
            current = json.loads(row['payload'])
            prior = [json.loads(r[0]) for r in baseline]
            same = next((r for r in prior if r['pod_uid']==current['pod_uid']),None)
            return {'current':current,'baseline':prior,'baseline_limit':100,
                    'restart_delta':max(0,current['restart_count']-same['restart_count']) if same else None}

    def counts(self, sid, **filters):
        extra, args = scope_sql(**filters)
        with self.lock:
            return [dict(r) for r in self.db.execute("SELECT target_match,health,row_type,(json_extract(payload,'$.image_tag') IS NOT NULL) AS tag_known,COUNT(*) AS count FROM rows WHERE session=? AND layer='current'"+extra+" GROUP BY target_match,health,row_type,tag_known", [sid]+args)]

    def changes(self, sid, classification=None, limit=50, offset=0, counts=False, version_status=None, health_change=None, **filters):
        # Compare aggregated replica states at stable workload UID/container granularity.
        sql = '''WITH groups AS (
          SELECT cluster,namespace,workload_uid,container,layer,
            MIN(json_extract(payload,'$.workload')) AS workload,
            GROUP_CONCAT(DISTINCT image) AS images,
            SUM(health IN ('CRASH','IMAGE_PULL_ERROR','ERROR')) AS bad,
            SUM(health='READY') AS ready, COUNT(*) AS total,
            SUM(target_match=0 AND json_extract(payload,'$.image_tag') IS NOT NULL) AS old,
            SUM(target_match=1) AS matched,
            SUM(json_extract(payload,'$.image_tag') IS NOT NULL) AS known
          FROM (SELECT * FROM rows WHERE session=? AND layer IN ('current','baseline') AND row_type='pod' ORDER BY image)
          GROUP BY cluster,namespace,workload_uid,container,layer
        ), keys AS (
          SELECT DISTINCT cluster,namespace,workload_uid,container FROM groups
        ), compared AS (
          SELECT k.*,COALESCE(c.workload,b.workload) AS workload,b.images AS before_images,c.images AS after_images,
            b.bad AS before_errors,c.bad AS after_errors,b.ready AS before_ready,c.ready AS after_ready,
            b.total AS before_total,c.total AS after_total,c.matched AS target_replicas,
            CASE WHEN c.total IS NULL THEN 'REMOVED'
                 WHEN c.known=0 THEN 'UNKNOWN_VERSION'
                 WHEN c.known<c.total THEN 'PARTLY_UNKNOWN'
                 WHEN c.matched=c.total THEN 'TARGET_REACHED'
                 WHEN c.matched>0 THEN 'MIXED_VERSION'
                 ELSE 'NOT_UPDATED' END AS version_status,
            CASE WHEN c.total IS NULL THEN 'REMOVED'
                 WHEN b.total IS NULL AND c.bad>0 THEN 'NEW_WITH_ERRORS'
                 WHEN b.total IS NULL THEN 'NEW_RESOURCE'
                 WHEN c.bad>b.bad THEN 'REGRESSION'
                 WHEN b.bad>0 AND c.bad=0 AND c.ready=c.total THEN 'RECOVERED'
                 WHEN c.bad>0 AND c.bad<b.bad THEN 'IMPROVING'
                 WHEN c.bad>0 THEN 'PERSISTING_ERROR'
                 WHEN c.ready=c.total THEN 'HEALTHY'
                 ELSE 'NOT_READY' END AS health_change,
            CASE WHEN b.total IS NULL THEN 'NEW_RESOURCE'
                 WHEN c.total IS NULL THEN 'REMOVED'
                 WHEN c.bad>COALESCE(b.bad,0) THEN 'REGRESSION'
                 WHEN b.bad>0 AND c.bad=0 AND c.ready=c.total THEN 'RECOVERED'
                 WHEN b.bad>0 AND c.bad>0 THEN 'PERSISTING_ERROR'
                 WHEN c.known=0 THEN 'UNKNOWN_VERSION'
                 WHEN c.old>0 AND c.matched>0 THEN 'MIXED_VERSION'
                 WHEN c.old>0 AND c.matched=0 THEN 'OLD_VERSION'
                 WHEN b.images<>c.images THEN 'IMAGE_CHANGED'
                 ELSE 'UNCHANGED' END AS classification
          FROM keys k LEFT JOIN groups b ON b.layer='baseline' AND b.cluster=k.cluster AND b.namespace=k.namespace AND b.workload_uid=k.workload_uid AND b.container=k.container
          LEFT JOIN groups c ON c.layer='current' AND c.cluster=k.cluster AND c.namespace=k.namespace AND c.workload_uid=k.workload_uid AND c.container=k.container
        ) SELECT * FROM compared'''
        extra, values = scope_sql(**filters)
        if extra:
            sql = sql.replace("AND row_type='pod' ORDER BY image", "AND row_type='pod' AND (cluster,namespace,workload_uid,container) IN (SELECT cluster,namespace,workload_uid,container FROM rows WHERE session=? AND layer IN ('baseline','current')"+extra+") ORDER BY image")
        args = [sid] + ([sid]+values if extra else [])
        if counts:
            axis = counts if counts in ('version_status','health_change') else 'classification'
            sql = 'SELECT '+axis+',COUNT(*) AS n FROM (' + sql + ') GROUP BY '+axis
            with self.lock:
                return {r[axis]:r['n'] for r in self.db.execute(sql,args)}
        conditions=[]
        for column,value in [('classification',classification),('version_status',version_status),('health_change',health_change)]:
            if value:
                conditions.append(column+'=?'); args.append(value)
        if conditions:
            sql += ' WHERE '+ ' AND '.join(conditions)
        sql += ' ORDER BY cluster,namespace,workload_uid,container LIMIT ? OFFSET ?'
        with self.lock:
            found = [dict(r) for r in self.db.execute(sql,args+[limit+1,offset])]
        return {'items':found[:limit], 'next_cursor':str(offset+limit) if len(found)>limit else None}

    def facets(self, sid, cluster=None):
        with self.lock:
            args=[sid]; extra=''
            if cluster: extra=' AND cluster=?'; args.append(cluster)
            namespaces=[r[0] for r in self.db.execute("SELECT DISTINCT namespace FROM rows WHERE session=? AND layer IN ('baseline','current')"+extra+" ORDER BY namespace LIMIT 2000",args)]
            return {'namespaces':namespaces, 'limit':2000}

    def designs(self):
        with self.lock:
            return [{'name':r['name'],'description':r['description'],'settings':json.loads(r['body'])} for r in self.db.execute('SELECT * FROM designs ORDER BY name')]

    def save_design(self,name,description,body):
        with self.lock,self.db:
            if self.db.execute('SELECT COUNT(*) FROM designs').fetchone()[0]>=50 and not self.db.execute('SELECT 1 FROM designs WHERE name=?',(name,)).fetchone():
                raise Conflict('En fazla 50 akış tasarımı kaydedilebilir')
            self.db.execute('INSERT OR REPLACE INTO designs VALUES(?,?,?)',(name,description,json.dumps(body)))
