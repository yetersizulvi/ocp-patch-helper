from __future__ import annotations

import logging


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    for noisy in ("urllib3", "kubernetes.client.rest"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
