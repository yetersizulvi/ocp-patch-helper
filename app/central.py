"""Single-process central patch monitor. Run with exactly one Uvicorn worker."""
from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import sqlite3
import threading
import time
import uuid
from fastapi import FastAPI, HTTPException, Query, Request, Depends
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse, Response
from pydantic import BaseModel, Field
from typing import Literal
from app.central_config import Config, load_config
from app.central_collect import Collector, ScanError, demo_rows
from app.namespace_patterns import NamespacePatterns, namespace_matches, normalize_patterns
from app.central_store import Store, Conflict
from app.central_logging import configure as configure_central_logging, emit


class NewSession(BaseModel):
    target_tag: str = Field(pattern=r'^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$')
    flow: str = 'patch-live'
    clusters: list[str] | None = None
    duration_minutes: int = Field(default=60, ge=1, le=720)
    namespace_glob: NamespacePatterns = '*'
    namespaces: list[str] = Field(default_factory=list, max_length=100)
    interval_seconds: float | None = Field(default=None, ge=1, le=3600)
    tag_match_mode: Literal['exact','release'] | None = None


class SavedDesign(BaseModel):
    name: str = Field(min_length=1,max_length=80)
    description: str = Field(default='',max_length=500)
    settings: NewSession


def view_filters(cluster: str|None=Query(None,max_length=64), namespace: str|None=Query(None,max_length=253),
                 namespace_glob: str=Query('*',max_length=1024),
                 search: str=Query('',max_length=128), hide_infrastructure: bool=False):
    try:
        namespace_glob=normalize_patterns(namespace_glob)
    except ValueError as exc:
        raise HTTPException(422,str(exc))
    return dict(cluster=cluster or None, namespace=namespace or None, namespace_glob=namespace_glob,
                search=search, hide_infrastructure=hide_infrastructure)


class Monitor:
    def __init__(self, path):
        self.log_level = configure_central_logging()
        self.state_log_interval = int(os.getenv('PATCH_LOG_STATE_INTERVAL_SECONDS','60'))
        if not 5 <= self.state_log_interval <= 3600:
            raise ValueError('PATCH_LOG_STATE_INTERVAL_SECONDS must be between 5 and 3600')
        self.state_log_task = None
        self.inflight = set()
        self.path = path
        self.config, self.version = load_config(path)
        self.store = Store(self.config.storage)
        self.action = asyncio.Lock()
        self.stop_event = threading.Event()
        self.tasks = []
        self.task_session = None
        self.maintenance_task = None
        self.config_error = None
        self.storage_error = None
        self.stream_clients = 0
        emit('INFO','monitor_started',log_level=self.log_level,config_version=self.version,demo=self.config.demo)
        for cluster in self.config.clusters:
            emit('INFO','cluster_configured',cluster=cluster.id,api_url=cluster.api_url,connection='NOT_CHECKED',scanning=False)

    async def log_state(self):
        while True:
            with self.store.lock:
                for cluster in self.config.clusters:
                    latest=self.store.db.execute("SELECT c.*,s.status FROM clusters c JOIN sessions s ON s.id=c.session WHERE c.cluster=? ORDER BY s.created DESC LIMIT 1",(cluster.id,)).fetchone()
                    state=latest['status'] if latest else 'IDLE'
                    observed=latest['observed'] if latest else None
                    error=latest['error'] if latest else None
                    emit('INFO','cluster_state',cluster=cluster.id,state=state,scanning=cluster.id in self.inflight,
                         connection='SCAN_CANCELLED' if error=='CANCELLED' else 'LAST_SCAN_FAILED' if error else 'LAST_SCAN_SUCCEEDED' if observed else 'NOT_CHECKED',
                         last_success_age_seconds=round(time.time()-observed) if observed else None,error=error,
                         reason='No active scan; start baseline or monitoring from UI' if state not in ('CAPTURING','RUNNING') else 'Periodic collection enabled')
            await asyncio.sleep(self.state_log_interval)

    def design(self, body):
        cfg = self.config.model_copy(deep=True)
        if body.flow not in cfg.flows:
            raise HTTPException(422,'Unknown flow')
        if body.duration_minutes > cfg.flows[body.flow].session_max_minutes:
            raise HTTPException(422,'Duration exceeds flow limit')
        if body.clusters is not None:
            ids = {c.id for c in cfg.clusters}
            if not body.clusters or set(body.clusters)-ids or len(body.clusters)!=len(set(body.clusters)):
                raise HTTPException(422,'Invalid cluster selection')
            cfg.clusters = [c for c in cfg.clusters if c.id in body.clusters]
        if body.interval_seconds is not None:
            if body.interval_seconds < cfg.flows[body.flow].interval_seconds:
                raise HTTPException(422,'Tarama aralığı ConfigMap minimumundan daha kısa olamaz')
            cfg.flows[body.flow].interval_seconds = body.interval_seconds
            cfg.flows[body.flow].stale_after_seconds = max(cfg.flows[body.flow].stale_after_seconds, body.interval_seconds*3)
        cfg.scope.namespace_glob = body.namespace_glob
        cfg.scope.namespaces = body.namespaces
        if body.tag_match_mode:
            cfg.images.tag_match_mode = body.tag_match_mode
        return cfg

    async def maintenance(self):
        while True:
            await asyncio.sleep(self.config.config_reload_seconds)
            try:
                config, version = await asyncio.to_thread(load_config, self.path)
                previous_version = self.version
                if config.storage.model_dump(exclude={'retention_days'}) != self.config.storage.model_dump(exclude={'retention_days'}) or config.demo != self.config.demo:
                    self.config_error = 'Storage/demo changes require a restart'
                else:
                    self.config, self.version, self.config_error = config, version, None
                    self.store.retention = config.storage.retention_days * 86400
                    if version != previous_version:
                        emit("INFO","config_reloaded",config_version=version)
                removed = await asyncio.to_thread(self.store.cleanup)
                if removed:
                    emit('INFO','retention_cleanup',removed_sessions=removed,retention_days=self.config.storage.retention_days)
            except Exception:
                self.config_error = 'Configuration reload or retention failed; last valid configuration retained'
                emit('ERROR','config_or_retention_failed')

    def public_session(self, sid):
        s = self.store.session(sid)
        cfg = s.pop('config')
        s['scope'] = cfg.get('scope',{'namespace_glob':'*','namespaces':[]})
        s['flow_settings'] = cfg['flows'][s['flow']]
        s['tag_match_mode'] = cfg['images']['tag_match_mode']
        for c in s['clusters']:
            c.pop('session', None)
        return s

    def scan(self, sid, cluster, config, baseline, tick):
        s = self.store.session(sid)
        flow = config.flows[s['flow']]
        context=dict(cluster=cluster.id,session_id=sid,scan_id=uuid.uuid4().hex[:12],mode='baseline' if baseline else 'live',demo=config.demo)
        began=time.monotonic()
        row_count=target_count=unhealthy_count=0
        collector=None
        self.inflight.add(cluster.id)
        emit('INFO','scan_started',interval_seconds=flow.interval_seconds,**context)
        try:
            self.store.begin_stage(sid,cluster.id)
            if config.demo:
                rows=demo_rows(cluster,s['target'],tick)
            else:
                collector=Collector(cluster,flow,config.images,s['target'],self.stop_event,config.scope,sid,context['scan_id'])
                rows=collector.rows()
            batch = []
            for row in rows:
                if not namespace_matches(row['namespace'],config.scope.namespace_glob) or (config.scope.namespaces and row['namespace'] not in config.scope.namespaces):
                    continue
                if self.stop_event.is_set():
                    raise ScanError('CANCELLED')
                row_count += 1
                target_count += int(row['target_match'])
                unhealthy_count += int(row['health'] in ('CRASH','ERROR','IMAGE_PULL_ERROR'))
                batch.append(row)
                if len(batch) >= min(flow.page_size, 200):
                    self.store.stage(sid,cluster.id,batch)
                    batch.clear()
            if batch:
                self.store.stage(sid,cluster.id,batch)
            if self.stop_event.is_set():
                raise ScanError('CANCELLED')
            self.store.publish(sid,cluster.id,baseline)
            emit('INFO','scan_completed',rows=row_count,target_rows=target_count,unhealthy_rows=unhealthy_count,
                 pages=collector.pages if collector else 0,objects=collector.count if collector else 0,published=True,
                 duration_ms=round((time.monotonic()-began)*1000),**context)
            return True
        except Exception as exc:
            # Never return response bodies, token paths or credentials in errors.
            code = 'CANCELLED' if isinstance(exc,Conflict) and self.stop_event.is_set() else str(exc) if isinstance(exc, ScanError) else 'STORAGE_ERROR' if isinstance(exc, sqlite3.Error) else 'SCAN_FAILED'
            emit('INFO' if code=='CANCELLED' else 'ERROR','scan_cancelled' if code=='CANCELLED' else 'scan_failed',
                 error=code,error_type=type(exc).__name__,path=collector.last_path if collector else None,
                 rows=row_count,pages=collector.pages if collector else 0,published=False,
                 duration_ms=round((time.monotonic()-began)*1000),**context)
            try:
                self.store.error(sid,cluster.id,code)
            except sqlite3.Error:
                self.storage_error = 'STORAGE_ERROR'
                self.stop_event.set()
            return False
        finally:
            self.inflight.discard(cluster.id)

    async def capture(self, sid, config):
        s = self.store.session(sid)
        done = {c['cluster'] for c in s['clusters'] if c['baseline']}
        await asyncio.gather(*(asyncio.to_thread(self.scan,sid,c,config,True,0) for c in config.clusters if c.id not in done))
        async with self.action:
            s = self.store.session(sid)
            if s['status'] == 'CAPTURING':
                self.store.status(sid, 'BASELINE_READY' if all(c['baseline'] for c in s['clusters']) else 'DRAFT')
                emit('INFO','baseline_finished',session_id=sid,state=self.store.session(sid)['status'])

    async def worker(self, sid, cluster, config):
        flow = config.flows[self.store.session(sid)['flow']]
        failures, tick = 0, 1
        while not self.stop_event.is_set():
            s = self.store.session(sid)
            if s['status'] != 'RUNNING' or time.time() >= s['ends']:
                break
            began = time.monotonic()
            ok = await asyncio.to_thread(self.scan,sid,cluster,config,False,tick)
            tick += 1
            failures = 0 if ok else min(failures+1,10)
            delay = flow.interval_seconds if ok else min(flow.failure_backoff_max_seconds, flow.interval_seconds*2**failures)
            delay = max(0.1, delay-(time.monotonic()-began))
            # Small async waits allow stop/expiry without orphaned background threads.
            emit('DEBUG' if ok else 'WARNING','scan_scheduled' if ok else 'scan_retry_scheduled',cluster=cluster.id,session_id=sid,retry_seconds=round(delay,2),failures=failures)
            until = time.monotonic()+delay
            while not self.stop_event.is_set() and time.monotonic()<until:
                await asyncio.sleep(min(.25,max(.01,until-time.monotonic())))
        return

    async def expire(self, sid):
        while not self.stop_event.is_set():
            s = self.store.session(sid)
            if s['status'] != 'RUNNING':
                return
            if time.time() >= s['ends']:
                self.stop_event.set()
                self.store.status(sid,'COMPLETED')
                emit('INFO','session_expired',session_id=sid,scanning=False)
                return
            await asyncio.sleep(.25)

    async def stop(self, sid):
        async with self.action:
            state = self.store.session(sid)['status']
            if state in ('STOPPED', 'COMPLETED'):
                return
            if state == 'STOPPING':
                raise Conflict('Session is already stopping')
            active = self.store.active_session_id()
            if (active and active != sid) or (any(not t.done() for t in self.tasks) and self.task_session != sid):
                raise Conflict('Another session is collecting')
            self.stop_event.set()
            self.store.status(sid,'STOPPING')
        await asyncio.gather(*self.tasks, return_exceptions=True)
        async with self.action:
            self.tasks = []
            self.task_session = None
            self.store.status(sid,'STOPPED')
            emit('INFO','session_stopped',session_id=sid,scanning=False)

    def envelope(self, sid, result):
        s = self.store.session(sid)
        threshold = Config.model_validate(s['config']).flows[s['flow']].stale_after_seconds
        now = time.time()
        clusters = [{**c, 'freshness':'UNKNOWN' if not c['observed'] else 'STALE' if c['error'] or now-c['observed']>threshold else 'FRESH'} for c in s['clusters']]
        return {**result,'revision':s['revision'],'status':s['status'],'clusters':clusters,
                'freshness':'FRESH' if all(c['freshness']=='FRESH' for c in clusters) else 'STALE'}


def create_app(config_path=None):
    monitor = Monitor(config_path or os.getenv('PATCH_CONFIG_FILE','/etc/patch/config/central.yaml'))

    @asynccontextmanager
    async def lifespan(app):
        monitor.maintenance_task = asyncio.create_task(monitor.maintenance())
        monitor.state_log_task = asyncio.create_task(monitor.log_state())
        yield
        monitor.stop_event.set()
        monitor.maintenance_task.cancel()
        monitor.state_log_task.cancel()
        await asyncio.gather(monitor.maintenance_task,monitor.state_log_task,*monitor.tasks,return_exceptions=True)
        monitor.store.db.close()

    app = FastAPI(title='Central Patch Monitor', version='0.7.3', lifespan=lifespan)
    app.state.monitor = monitor

    @app.exception_handler(KeyError)
    async def missing(request, exc):
        return JSONResponse({'detail':'Resource not found'},status_code=404)

    @app.exception_handler(Conflict)
    async def conflict(request, exc):
        return JSONResponse({'detail':str(exc)},status_code=409)

    @app.exception_handler(sqlite3.Error)
    async def database_error(request, exc):
        return JSONResponse({'detail':'Storage unavailable or quota reached'},status_code=507)

    @app.middleware('http')
    async def same_origin(request, call_next):
        # Route is expected to be private / protected by your existing ingress auth.
        if request.method in ('POST','PUT','DELETE','PATCH'):
            origin = request.headers.get('origin')
            from urllib.parse import urlparse
            if origin and urlparse(origin).netloc != request.headers.get('host'):
                return JSONResponse({'detail':'Cross-origin write rejected'},status_code=403)
            if request.headers.get('sec-fetch-site') == 'cross-site':
                return JSONResponse({'detail':'Cross-site write rejected'},status_code=403)
        response = await call_next(request)
        response.headers['Cache-Control'] = 'no-store'
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['X-Frame-Options'] = 'DENY'
        return response

    @app.get('/', response_class=HTMLResponse)
    def dashboard():
        return Path(__file__).with_name('central_ui.html').read_text()

    @app.get('/ui.js')
    def ui_script():
        return Response(Path(__file__).with_name('central_ui.js').read_text(),media_type='application/javascript')

    @app.get('/health/live')
    def live():
        return {'status':'ok'}

    @app.get('/health/ready')
    def ready():
        if monitor.storage_error:
            raise HTTPException(503,'Storage unavailable')
        return {'status':'ok','config_error':monitor.config_error}

    @app.get('/api/v1/config')
    def config():
        value = monitor.config.model_dump()
        for c in value['clusters']:
            c.pop('token_file',None)
        return {**value,'version':monitor.version,'reload_error':monitor.config_error,'active_session_config':'frozen'}

    @app.get('/api/v1/clusters')
    def clusters():
        return {'items':[{'id':c.id,'connection':c.connection,'api_url':c.api_url} for c in monitor.config.clusters]}

    @app.get('/api/v1/flows')
    def flows():
        return {'templates':{name:{**flow.model_dump(), 'description':'Patch öncesi durum kaydı → periyodik image ve sağlık taraması → baseline karşılaştırması → süre sonunda durdurma'} for name,flow in monitor.config.flows.items()},'designs':monitor.store.designs()}

    @app.post('/api/v1/flows/preview')
    def preview(body: NewSession):
        cfg=monitor.design(body)
        return {'clusters':[c.id for c in cfg.clusters], 'scope':cfg.scope.model_dump(),
                'target_tag':body.target_tag, 'tag_match_mode':cfg.images.tag_match_mode,
                'interval_seconds':cfg.flows[body.flow].interval_seconds,
                'duration_minutes':body.duration_minutes,
                'steps':['Patch öncesi durumu kaydet','Hazır olduğunda canlı izlemeyi başlat','Image ve sağlığı karşılaştır','Süre sonunda durdur'],
                'note':'Namespace seçiminiz ConfigMap kapsamıyla kesiştirilir; erişim kapsamını genişletmez.'}

    @app.post('/api/v1/flows/designs')
    def save_design(body: SavedDesign):
        monitor.design(body.settings)
        monitor.store.save_design(body.name,body.description,body.settings.model_dump())
        return {'name':body.name,'saved':True}

    @app.delete('/api/v1/flows/designs/{name:path}')
    def delete_design(name: str):
        monitor.store.delete_design(name)
        return {'name': name, 'deleted': True}

    @app.get('/api/v1/sessions')
    def sessions():
        return {'items':monitor.store.sessions()}

    @app.post('/api/v1/sessions',status_code=201)
    async def create(body: NewSession):
        async with monitor.action:
            if any(not t.done() for t in monitor.tasks):
                raise Conflict('Previous collectors are still stopping')
            cfg = monitor.design(body)
            sid = uuid.uuid4().hex
            monitor.store.create(sid,body.target_tag,body.flow,cfg.model_dump(),body.duration_minutes*60)
            return monitor.public_session(sid)

    @app.get('/api/v1/sessions/{sid}')
    def session(sid: str):
        return monitor.public_session(sid)

    @app.post('/api/v1/sessions/{sid}/baseline',status_code=202)
    async def baseline(sid: str):
        async with monitor.action:
            s = monitor.store.session(sid)
            if s['status'] not in ('DRAFT','INTERRUPTED') or all(c['baseline'] for c in s['clusters']):
                raise Conflict('Baseline already captured or session is not a draft')
            if any(not t.done() for t in monitor.tasks):
                raise Conflict('Collectors are busy')
            if any(x['id']!=sid and x['status'] in ('RUNNING','CAPTURING','DRAFT','BASELINE_READY','STOPPING') for x in monitor.store.sessions()):
                raise Conflict('Another session is active')
            monitor.stop_event.clear()
            monitor.store.status(sid,'CAPTURING')
            monitor.task_session = sid
            monitor.tasks = [asyncio.create_task(monitor.capture(sid,Config.model_validate(s['config'])))]
            return monitor.public_session(sid)

    @app.post('/api/v1/sessions/{sid}/start')
    async def start(sid: str):
        async with monitor.action:
            s = monitor.store.session(sid)
            if s['status'] not in ('BASELINE_READY','INTERRUPTED') or not all(c['baseline'] for c in s['clusters']):
                raise Conflict('A complete baseline is required')
            if any(not t.done() for t in monitor.tasks):
                raise Conflict('Collectors are busy')
            if any(x['id']!=sid and x['status'] in ('RUNNING','CAPTURING','DRAFT','BASELINE_READY','STOPPING') for x in monitor.store.sessions()):
                raise Conflict('Another session is active')
            cfg = Config.model_validate(s['config'])
            monitor.stop_event.clear()
            monitor.store.status(sid,'RUNNING',time.time()+s['duration'])
            emit('INFO','session_started',session_id=sid,interval_seconds=cfg.flows[s['flow']].interval_seconds)
            monitor.task_session = sid
            monitor.tasks = [asyncio.create_task(monitor.worker(sid,c,cfg)) for c in cfg.clusters]
            monitor.tasks.append(asyncio.create_task(monitor.expire(sid)))
            return monitor.public_session(sid)

    @app.post('/api/v1/sessions/{sid}/stop')
    async def stop(sid: str):
        await monitor.stop(sid)
        return monitor.public_session(sid)

    @app.get('/api/v1/sessions/{sid}/summary')
    def summary(sid: str, filters: dict=Depends(view_filters)):
        with monitor.store.lock:
            return monitor.envelope(sid, {'counts':monitor.store.counts(sid,**filters), 'change_counts':monitor.store.changes(sid,counts=True,**filters), 'health_counts':monitor.store.changes(sid,counts='health_change',**filters), 'cluster_counts':{c['cluster']:monitor.store.counts(sid,**{**filters,'cluster':c['cluster']}) for c in monitor.store.session(sid)['clusters']}})

    @app.get('/api/v1/sessions/{sid}/images')
    def images(sid: str, known_tag_only: bool=False, target_match: bool|None=None, health: str|None=None, filters: dict=Depends(view_filters),
               limit: int=Query(50,ge=1,le=200), cursor: str=Query('',max_length=128)):
        with monitor.store.lock:
            return monitor.envelope(sid,monitor.store.page(sid,target_match,health or None,limit=limit,cursor=cursor,inventory=True,known_tag_only=known_tag_only,**filters))

    @app.get('/api/v1/sessions/{sid}/targets')
    def targets(sid: str, health: str|None=None, filters: dict=Depends(view_filters),
                limit: int=Query(50,ge=1,le=200), cursor: str=Query('',max_length=128)):
        with monitor.store.lock:
            return monitor.envelope(sid,monitor.store.page(sid,True,health or None,limit=limit,cursor=cursor,**filters))

    @app.get('/api/v1/sessions/{sid}/changes')
    def changes(sid: str, classification: str|None=None, version_status: str|None=None, health_change: str|None=None, filters: dict=Depends(view_filters), limit: int=Query(50,ge=1,le=200), cursor: int=Query(0,ge=0,le=1000000)):
        with monitor.store.lock:
            return monitor.envelope(sid,monitor.store.changes(sid,classification or None,limit,cursor,version_status=version_status or None,health_change=health_change or None,**filters))

    @app.get('/api/v1/sessions/{sid}/facets')
    def facets(sid: str, cluster: str|None=None):
        monitor.store.session(sid)
        return monitor.store.facets(sid,cluster)

    @app.get('/api/v1/sessions/{sid}/containers/{rid}')
    def detail(sid: str,rid: str):
        monitor.store.session(sid)
        return monitor.store.detail(sid,rid)

    @app.get('/api/v1/sessions/{sid}/stream')
    async def stream(sid: str,request: Request):
        monitor.store.session(sid)
        if monitor.stream_clients >= monitor.config.max_stream_clients:
            raise HTTPException(429,'Stream client limit reached')
        monitor.stream_clients += 1
        async def events():
            try:
                while not await request.is_disconnected():
                    s = await asyncio.to_thread(monitor.public_session,sid)
                    # Bounded notification only; no per-client event queue or full snapshot.
                    yield f"id: {s['revision']}\nevent: revision\ndata: {json.dumps({'revision':s['revision'],'status':s['status']})}\n\n"
                    await asyncio.sleep(monitor.config.stream_seconds)
            finally:
                monitor.stream_clients -= 1
        return StreamingResponse(events(),media_type='text/event-stream',headers={'X-Accel-Buffering':'no'})

    return app
