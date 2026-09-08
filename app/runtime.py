from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import Any

from app import __version__
from app.collectors import CollectorEngine, KubeDataSource
from app.config import AgentSettings, ConfigurationError, load_flows
from app.llm import CrashLLMClient
from app.models import (
    AckEvent,
    AgentPublicState,
    AgentRegistration,
    DesiredState,
    FlowDocument,
    ProcessInstruction,
    RuntimeState,
    utc_now,
)
from app.scheduler import FlowRunner
from app.transport import MasterClient


LOGGER = logging.getLogger(__name__)


class AgentRuntime:
    def __init__(
        self,
        settings: AgentSettings,
        master: MasterClient | None = None,
        source: KubeDataSource | None = None,
        llm: CrashLLMClient | None = None,
    ) -> None:
        self.settings = settings
        self.master = master or MasterClient(settings)
        self.source = source
        self.llm = llm or CrashLLMClient(settings)
        self.flows: dict[str, FlowDocument] = {}
        self.runner: FlowRunner | None = None
        self.state = RuntimeState.STARTING
        self.last_master_contact_at = None
        self.last_error: str | None = None
        self.lease_expires_at = None
        self._lease_deadline = 0.0
        self._last_command_id: str | None = None
        self._flow_digest = ""
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self._registered = False

    async def start(self) -> None:
        self.flows = load_flows(self.settings.flow_config_path)
        self._flow_digest = self._calculate_flow_digest()
        if self.source is None:
            first_flow = next(iter(self.flows.values()))
            self.source = await asyncio.to_thread(
                KubeDataSource,
                self.settings.namespace_pattern,
                self.settings.request_timeout_seconds,
                first_flow.spec.control.shared_cache_seconds,
            )
        self.state = RuntimeState.IDLE
        self._tasks = [
            asyncio.create_task(self._control_loop(), name="agent-control"),
            asyncio.create_task(self._heartbeat_loop(), name="agent-heartbeat"),
            asyncio.create_task(self._flow_reload_loop(), name="agent-flow-reload"),
            asyncio.create_task(self._lease_watchdog(), name="agent-lease-watchdog"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        await self._stop_process("agent shutdown")
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []

    def public_state(self) -> AgentPublicState:
        return AgentPublicState(
            cluster_id=self.settings.cluster_id,
            environment=self.settings.environment,
            state=self.state,
            process_id=self.runner.process_id if self.runner else None,
            flow_name=self.runner.flow_name if self.runner else None,
            target_tag=self.runner.target_tag if self.runner else None,
            lease_expires_at=self.lease_expires_at,
            last_master_contact_at=self.last_master_contact_at,
            last_error=self.last_error,
            collector_status=self.runner.status if self.runner else {},
        )

    async def _ensure_registered(self) -> None:
        if self._registered:
            return
        registration = AgentRegistration(
            cluster_id=self.settings.cluster_id,
            environment=self.settings.environment,
            namespace_pattern=self.settings.namespace_pattern,
            agent_version=__version__,
            flow_names=sorted(self.flows),
            capabilities=[
                "master-controlled-lease-v1",
                "configmap-flow-v1",
                "error-only-health-v1",
                "crash-describe-v1",
                "llm-crash-triage-v1",
            ],
        )
        await asyncio.to_thread(self.master.register, registration)
        self._registered = True
        self.last_master_contact_at = utc_now()

    async def _control_loop(self) -> None:
        while not self._stop.is_set():
            poll_seconds = self._control().control_poll_seconds
            try:
                await self._ensure_registered()
                instruction = await asyncio.to_thread(self.master.poll_control)
                self.last_master_contact_at = utc_now()
                self.last_error = None
                if instruction:
                    await self._apply_instruction(instruction)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{exc.__class__.__name__}: master control failed"
                LOGGER.warning("Master control failed: %s", exc.__class__.__name__)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def _apply_instruction(self, instruction: ProcessInstruction) -> None:
        is_new_command = instruction.command_id != self._last_command_id
        if instruction.desired_state == DesiredState.RUNNING:
            assert instruction.flow_name
            assert instruction.process_id
            assert instruction.target_tag
            flow = self.flows.get(instruction.flow_name)
            if flow is None:
                if is_new_command:
                    await asyncio.to_thread(
                        self.master.acknowledge,
                        AckEvent(
                            cluster_id=self.settings.cluster_id,
                            command_id=instruction.command_id,
                            process_id=instruction.process_id,
                            accepted_state=DesiredState.STOPPED,
                            message=f"unknown flow: {instruction.flow_name}",
                        ),
                    )
                self._last_command_id = instruction.command_id
                return

            lease_seconds = instruction.lease_seconds or self._control().default_lease_seconds
            must_restart = (
                self.runner is None
                or self.runner.process_id != instruction.process_id
                or self.runner.flow_name != instruction.flow_name
                or self.runner.target_tag != instruction.target_tag
            )
            if must_restart:
                await self._stop_process("new master process")
                await self._start_process(instruction, flow)
            self._lease_deadline = time.monotonic() + lease_seconds
            self.lease_expires_at = utc_now() + timedelta(seconds=lease_seconds)
            if is_new_command:
                await asyncio.to_thread(
                    self.master.acknowledge,
                    AckEvent(
                        cluster_id=self.settings.cluster_id,
                        command_id=instruction.command_id,
                        process_id=instruction.process_id,
                        accepted_state=DesiredState.RUNNING,
                        message="process started or lease renewed",
                    ),
                )
        else:
            await self._stop_process("master stop instruction")
            if is_new_command:
                await asyncio.to_thread(
                    self.master.acknowledge,
                    AckEvent(
                        cluster_id=self.settings.cluster_id,
                        command_id=instruction.command_id,
                        process_id=instruction.process_id,
                        accepted_state=instruction.desired_state,
                        message="process stopped; agent is idle",
                    ),
                )
        self._last_command_id = instruction.command_id

    async def _start_process(
        self, instruction: ProcessInstruction, flow: FlowDocument
    ) -> None:
        assert self.source is not None
        self.source.set_cache_seconds(flow.spec.control.shared_cache_seconds)
        engine = CollectorEngine(self.source, self.settings.registry_prefix)
        self.runner = FlowRunner(
            self.settings, instruction, flow, engine, self.llm, self.master
        )
        self.runner.start()
        self.state = RuntimeState.RUNNING

    async def _stop_process(self, reason: str) -> None:
        if self.runner:
            self.state = RuntimeState.STOPPING
            await self.runner.stop()
            LOGGER.info("Patch process stopped: %s", reason)
        self.runner = None
        self.state = RuntimeState.IDLE
        self.lease_expires_at = None
        self._lease_deadline = 0.0

    async def _lease_watchdog(self) -> None:
        while not self._stop.is_set():
            if self.runner and self._lease_deadline and time.monotonic() >= self._lease_deadline:
                await self._stop_process("master lease expired")
                self.last_error = "master lease expired; collectors stopped"
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=1)
            except asyncio.TimeoutError:
                pass

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            heartbeat_seconds = self._control().heartbeat_seconds
            try:
                await self._ensure_registered()
                await asyncio.to_thread(
                    self.master.heartbeat,
                    self.public_state().model_dump(mode="json"),
                )
                self.last_master_contact_at = utc_now()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"{exc.__class__.__name__}: heartbeat failed"
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=heartbeat_seconds)
            except asyncio.TimeoutError:
                pass

    async def _flow_reload_loop(self) -> None:
        while not self._stop.is_set():
            reload_seconds = self._control().flow_reload_seconds
            try:
                digest = self._calculate_flow_digest()
                if digest != self._flow_digest:
                    new_flows = load_flows(self.settings.flow_config_path)
                    self.flows = new_flows
                    self._flow_digest = digest
                    self._registered = False
                    if self.runner:
                        active_name = self.runner.flow_name
                        active_flow = self.flows.get(active_name)
                        instruction = self.runner.instruction
                        if active_flow:
                            remaining_lease = max(
                                0.0, self._lease_deadline - time.monotonic()
                            )
                            lease_expires_at = self.lease_expires_at
                            await self._stop_process("flow ConfigMap reloaded")
                            await self._start_process(instruction, active_flow)
                            self._lease_deadline = time.monotonic() + remaining_lease
                            self.lease_expires_at = lease_expires_at
                        else:
                            await self._stop_process("active flow removed")
            except asyncio.CancelledError:
                raise
            except (OSError, ConfigurationError) as exc:
                self.last_error = f"{exc.__class__.__name__}: flow reload failed"
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=reload_seconds)
            except asyncio.TimeoutError:
                pass

    def _control(self):
        if self.runner:
            return self.runner.flow.spec.control
        return next(iter(self.flows.values())).spec.control

    def _calculate_flow_digest(self) -> str:
        content = Path(self.settings.flow_config_path).read_bytes()
        return hashlib.sha256(content).hexdigest()
