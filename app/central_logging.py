"""Structured operational logs to stdout; credentials/response bodies are never fields."""
import json
import logging
import os
import sys
from datetime import datetime, timezone

LOGGER = logging.getLogger('patch.monitor')
FIELDS = frozenset('cluster session_id scan_id mode api_url namespace resource path status error error_type duration_ms rows target_rows unhealthy_rows pages objects namespace_count published interval_seconds retry_seconds failures state scanning connection last_success_age_seconds demo config_version log_level reason removed_sessions retention_days'.split())

class HealthAccessFilter(logging.Filter):
    """Suppress only successful probe access lines; keep failures and all API logs."""
    def filter(self, record):
        if not isinstance(record.args, tuple) or len(record.args) != 5:
            return True
        try:
            path = str(record.args[2]).split('?', 1)[0]
            status = int(record.args[4])
        except (TypeError, ValueError):
            return True
        return not (path in ('/health/live', '/health/ready') and 200 <= status < 300)


def configure():
    access = logging.getLogger('uvicorn.access')
    for existing in list(access.filters):
        if isinstance(existing, HealthAccessFilter):
            access.removeFilter(existing)
    access.addFilter(HealthAccessFilter())
    level = os.getenv('PATCH_LOG_LEVEL', 'INFO').upper()
    if level not in ('DEBUG','INFO','WARNING','ERROR'):
        raise ValueError('PATCH_LOG_LEVEL must be DEBUG, INFO, WARNING or ERROR')
    LOGGER.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(message)s'))
    LOGGER.addHandler(handler)
    LOGGER.setLevel(level)
    LOGGER.propagate = False
    # Never enable wire/header/body dumps when our operational debug mode is enabled.
    for noisy in ('urllib3','requests','kubernetes.client.rest'):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return level

def emit(level, event, **fields):
    if not LOGGER.isEnabledFor(getattr(logging, level)):
        return
    data = {'timestamp':datetime.now(timezone.utc).isoformat(timespec='milliseconds'),
            'level':level,'event':event}
    for key,value in fields.items():
        if key in FIELDS and value is not None:
            data[key] = value[:512] if isinstance(value,str) else value
    LOGGER.log(getattr(logging,level),json.dumps(data,ensure_ascii=False,separators=(',',':')))
