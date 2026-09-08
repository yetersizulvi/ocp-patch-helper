"""Validated, file-based configuration; active sessions freeze a config snapshot."""
from __future__ import annotations
import hashlib
import re
from pathlib import Path
from urllib.parse import urlparse
import yaml
from app.namespace_patterns import NamespacePatterns
from pydantic import BaseModel, ConfigDict, Field, model_validator

SA = '/var/run/secrets/kubernetes.io/serviceaccount/'

class StrictModel(BaseModel):
    model_config = ConfigDict(extra='forbid')


class Cluster(StrictModel):
    id: str = Field(pattern=r'^[a-zA-Z0-9_-]{1,64}$')
    connection: str = 'inCluster'
    api_url: str = 'https://kubernetes.default.svc:443'
    token_file: str = SA + 'token'
    ca_file: str = SA + 'ca.crt'
    namespace_pattern: str = '^(test-|uat-)'
    # Only for an explicitly provisioned trusted HTTP API proxy, never a TLS fallback.
    allow_http: bool = False

    @model_validator(mode='after')
    def validate_cluster(self):
        re.compile(self.namespace_pattern)
        u = urlparse(self.api_url)
        if self.connection not in ('inCluster', 'remote'):
            raise ValueError('connection must be inCluster or remote')
        if not u.hostname or u.username or u.password or u.query or u.fragment or u.path not in ('', '/'):
            raise ValueError('api_url must be an origin without credentials')
        if u.scheme != 'https' and not (u.scheme == 'http' and self.allow_http):
            raise ValueError('HTTP requires an explicit trusted proxy and allow_http: true')
        if self.connection == 'remote' and (self.token_file == SA+'token' or self.ca_file == SA+'ca.crt'):
            raise ValueError('remote cluster needs its own token_file and ca_file')
        return self

class Flow(StrictModel):
    interval_seconds: float = Field(default=5, ge=1, le=3600)
    request_timeout_seconds: float = Field(default=10, ge=1, le=120)
    scan_timeout_seconds: float = Field(default=120, ge=5, le=1800)
    page_size: int = Field(default=200, ge=1, le=1000)
    max_page_bytes: int = Field(default=4194304, ge=1024, le=16777216)
    max_objects: int = Field(default=20000, ge=10, le=200000)
    max_rows: int = Field(default=30000, ge=10, le=500000)
    session_max_minutes: int = Field(default=120, ge=1, le=720)
    failure_backoff_max_seconds: float = Field(default=60, ge=1, le=3600)
    stale_after_seconds: float = Field(default=20, ge=1)
    resources: list[str] = Field(default_factory=lambda: ['deployments','statefulsets','daemonsets'])

    @model_validator(mode='after')
    def valid_resources(self):
        if not self.resources or set(self.resources) - {'deployments','statefulsets','daemonsets'}:
            raise ValueError('unsupported workload resources')
        return self

class Images(StrictModel):
    registry_prefixes: list[str] = Field(default_factory=list)
    tag_match_mode: str = 'exact'
    release_pattern: str = r'^(\d+\.\d+\.\d+)(?:-.+)?$'

    @model_validator(mode='after')
    def valid_policy(self):
        if self.tag_match_mode not in ('exact','release'):
            raise ValueError('tag_match_mode must be exact or release')
        if re.compile(self.release_pattern).groups != 1:
            raise ValueError('release_pattern must have exactly one capture group')
        return self

class Storage(StrictModel):
    database_path: str = '/data/patch.db'
    retention_days: int = Field(default=14, ge=1, le=365)
    max_database_mb: int = Field(default=512, ge=8, le=10240)

class Scope(StrictModel):
    namespace_glob: NamespacePatterns = '*'
    namespaces: list[str] = Field(default_factory=list, max_length=100)

class Config(StrictModel):
    clusters: list[Cluster] = Field(min_length=1, max_length=20)
    flows: dict[str, Flow] = Field(default_factory=lambda: {'patch-live': Flow()})
    images: Images = Field(default_factory=Images)
    storage: Storage = Field(default_factory=Storage)
    config_reload_seconds: int = Field(default=15, ge=1, le=300)
    stream_seconds: float = Field(default=2, ge=1, le=30)
    max_stream_clients: int = Field(default=30, ge=1, le=500)
    demo: bool = False
    scope: Scope = Field(default_factory=Scope)

    @model_validator(mode='after')
    def unique(self):
        if len({c.id for c in self.clusters}) != len(self.clusters) or not self.flows:
            raise ValueError('unique cluster ids and at least one flow required')
        return self


def load_config(path: str):
    raw = Path(path).read_bytes()
    return Config.model_validate(yaml.safe_load(raw)), hashlib.sha256(raw).hexdigest()[:16]
