"""One grammar for flow scope, collector filtering and SQL view filters."""
import fnmatch
import re
from typing import Annotated
from pydantic import BeforeValidator

def normalize_patterns(value):
    if not isinstance(value,str) or len(value)>1024:
        raise ValueError('Namespace deseni en fazla 1024 karakter olabilir.')
    parts=[part.strip() for part in value.split(',')]
    if not parts or len(parts)>20 or any(not part or len(part)>253 or not re.fullmatch(r'[a-z0-9*?._-]+',part) for part in parts):
        raise ValueError('Desenleri virgülle ayır: test-*,uat-*. Boş desen veya geçersiz karakter kullanma.')
    return ','.join(dict.fromkeys(parts))

def split_patterns(value):
    return normalize_patterns(value).split(',')

def namespace_matches(name, patterns):
    return any(fnmatch.fnmatchcase(name,part) for part in split_patterns(patterns))

NamespacePatterns=Annotated[str,BeforeValidator(normalize_patterns)]
