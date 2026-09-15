"""Real HTTP + SSE smoke test, synthetic data only, isolated temporary database."""
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import requests
import yaml

with tempfile.TemporaryDirectory() as temp:
    cfg=yaml.safe_load(Path('config/central.demo.yaml').read_text())
    cfg['storage']['database_path']=temp+'/smoke.db'
    path=Path(temp)/'config.yaml';path.write_text(yaml.safe_dump(cfg))
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    env={**os.environ,'PATCH_CONFIG_FILE':str(path)}
    log_path=Path(temp)/'server.log'
    log_file=log_path.open('w')
    process=subprocess.Popen([sys.executable,'-m','uvicorn','app.central:create_app','--factory','--host','127.0.0.1','--port',str(port)],env=env,stdout=log_file,stderr=subprocess.STDOUT)
    base=f'http://127.0.0.1:{port}'
    http=requests.Session();http.trust_env=False
    try:
        for _ in range(100):
            try:
                if http.get(base+'/health/ready',timeout=.5).status_code==200:break
            except requests.RequestException:pass
            time.sleep(.05)
        else:raise RuntimeError('Server did not start')
        def call(path,body=None):
            r=http.get(base+path,timeout=5) if body is None else http.post(base+path,json=body,timeout=5)
            r.raise_for_status();return r.json()
        assert http.get(base+'/health/live',timeout=5).status_code==200
        assert http.post(base+'/health/live',timeout=5).status_code==405
        assert 'Patch Monitor' in http.get(base+'/',timeout=5).text
        session=call('/api/v1/sessions',{'target_tag':'1.4.1'})
        endpoint='/api/v1/sessions/'+session['id']
        call(endpoint+'/baseline',{})
        for _ in range(100):
            if call(endpoint)['status']=='BASELINE_READY':break
            time.sleep(.03)
        else:raise RuntimeError('Baseline timed out')
        call(endpoint+'/start',{})
        with http.get(base+endpoint+'/stream',stream=True,timeout=5) as stream:
            stream.raise_for_status()
            assert stream.headers['content-type'].startswith('text/event-stream')
            for line in stream.iter_lines(chunk_size=1):
                if line.startswith(b'data: '):
                    event=json.loads(line[6:]);assert 'revision' in event;break
        for _ in range(100):
            summary=call(endpoint+'/summary')
            if summary['change_counts'].get('RECOVERED')==2:break
            time.sleep(.03)
        assert summary['change_counts']['REGRESSION']==2
        assert summary['change_counts']['OLD_VERSION']==2
        assert len(call(endpoint+'/targets?health=CRASH')['items'])==2
        call(endpoint+'/stop',{})
        assert call(endpoint)['status']=='STOPPED'
        print('PASS: real HTTP baseline/start, SSE revision, crash/recovery/old version filters, stop')
    finally:
        http.close();process.terminate()
        try:process.wait(timeout=10)
        except subprocess.TimeoutExpired:process.kill();process.wait()
        log_file.close()
        logs=log_path.read_text()
        assert '"GET /health/ready ' not in logs
        assert '"GET /health/live ' not in logs
        assert '"POST /health/live HTTP/1.1" 405' in logs
        assert '"POST /api/v1/sessions HTTP/1.1" 201' in logs
        print('PASS: successful health access logs suppressed; failed health and API logs retained')
