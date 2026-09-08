from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app import __version__
from app.config import AgentSettings
from app.logging_config import configure_logging
from app.models import AgentPublicState
from app.runtime import AgentRuntime


def create_app() -> FastAPI:
    settings = AgentSettings.from_env()
    configure_logging(settings.log_level)
    runtime = AgentRuntime(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await runtime.start()
        yield
        await runtime.stop()

    app = FastAPI(
        title="OpenShift Patch Agent",
        version=__version__,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/health/ready", response_model=AgentPublicState)
    async def ready() -> AgentPublicState:
        return runtime.public_state()

    @app.get("/api/v1/state", response_model=AgentPublicState)
    async def state() -> AgentPublicState:
        return runtime.public_state()

    return app
