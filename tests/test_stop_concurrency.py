import asyncio
import tempfile
import unittest
from pathlib import Path
import yaml
from app.central import Monitor
from app.central_store import Conflict


class StopTests(unittest.IsolatedAsyncioTestCase):
    async def test_duplicate_stop_cannot_clear_a_new_session_tasks(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'config.yaml'
            path.write_text(yaml.safe_dump({'clusters':[{'id':'local'}],'storage':{'database_path':directory+'/db'}}))
            monitor=Monitor(str(path))
            release=asyncio.Event()
            try:
                monitor.store.create('active','1.4.1','patch-live',monitor.config.model_dump(),60)
                monitor.store.status('active','RUNNING')
                monitor.task_session='active'
                monitor.tasks=[asyncio.create_task(release.wait())]
                stopping=asyncio.create_task(monitor.stop('active'))
                await asyncio.sleep(0)
                self.assertEqual('STOPPING',monitor.store.session('active')['status'])
                with self.assertRaises(Conflict):
                    await monitor.stop('active')
                release.set()
                await stopping
                self.assertEqual('STOPPED',monitor.store.session('active')['status'])
                monitor.store.create('new','1.4.1','patch-live',monitor.config.model_dump(),60)
                await monitor.stop('active')
                self.assertEqual('DRAFT',monitor.store.session('new')['status'])
            finally:
                release.set()
                await asyncio.gather(*monitor.tasks)
                monitor.store.db.close()

    async def test_historical_session_cannot_cancel_active_collector(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'config.yaml'
            path.write_text(yaml.safe_dump({'clusters':[{'id':'local'}],'storage':{'database_path':directory+'/db'}}))
            monitor=Monitor(str(path))
            release=asyncio.Event()
            try:
                monitor.store.create('old','1.4.0','patch-live',monitor.config.model_dump(),60)
                monitor.store.status('old','INTERRUPTED')
                monitor.store.create('active','1.4.1','patch-live',monitor.config.model_dump(),60)
                monitor.store.status('active','RUNNING')
                monitor.task_session='active'
                monitor.tasks=[asyncio.create_task(release.wait())]
                with self.assertRaises(Conflict):
                    await monitor.stop('old')
                self.assertFalse(monitor.stop_event.is_set())
                self.assertEqual('RUNNING',monitor.store.session('active')['status'])
            finally:
                release.set()
                await asyncio.gather(*monitor.tasks)
                monitor.store.db.close()
