"""
Recovery Orchestrator - State checkpointing and automated agent recovery.

Manages agent state persistence, handles agent crashes, and orchestrates
automatic respawning with context restoration. Coordinates with the
Health Monitor and Circuit Breaker for intelligent recovery decisions.
"""

import asyncio
import json
import pickle
import hashlib
from typing import Any, Dict, List, Optional, Callable, Type
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
import logging
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("RecoveryOrchestrator")


class RecoveryStrategy(Enum):
    """Strategies for recovering failed agents."""
    RESTART_FRESH = "restart_fresh"      # Clean restart, no state
    RESTORE_STATE = "restore_state"       # Restore from last checkpoint
    DEGRADED_MODE = "degraded_mode"        # Run with reduced functionality
    FAILOVER = "failover"                  # Switch to backup agent
    ESCALATE = "escalate"                  # Escalate to human/system admin


@dataclass
class Checkpoint:
    """A state checkpoint for an agent."""
    agent_id: str
    agent_type: str
    state_data: Dict[str, Any]
    timestamp: float
    version: int
    checksum: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "state_data": self.state_data,
            "timestamp": self.timestamp,
            "version": self.version,
            "checksum": self.checksum,
            "metadata": self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> "Checkpoint":
        return cls(**data)


@dataclass
class RecoveryContext:
    """Context for recovery decisions."""
    agent_id: str
    agent_type: str
    failure_count: int
    last_error: Optional[str]
    checkpoint_available: bool
    checkpoint_age_seconds: float
    circuit_state: str
    health_status: str
    available_backups: List[str]


@dataclass
class RecoveryResult:
    """Result of a recovery attempt."""
    success: bool
    strategy_used: RecoveryStrategy
    new_agent_id: Optional[str]
    error: Optional[str]
    recovery_time_ms: float
    checkpoint_restored: bool


class StateStore:
    """
    Persistent store for agent state checkpoints.
    
    Supports both in-memory and filesystem storage with
    automatic garbage collection of old checkpoints.
    """
    
    def __init__(
        self,
        storage_path: Optional[Path] = None,
        max_checkpoints_per_agent: int = 10,
        keep_checkpoints_hours: float = 24.0
    ):
        self.storage_path = storage_path
        self.max_checkpoints = max_checkpoints_per_agent
        self.retention_hours = keep_checkpoints_hours
        
        # In-memory cache
        self.checkpoints: Dict[str, List[Checkpoint]] = {}
        self._lock = asyncio.Lock()
        
        if storage_path:
            storage_path.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()
    
    def _compute_checksum(self, state_data: Dict) -> str:
        """Compute checksum for state integrity verification."""
        state_bytes = json.dumps(state_data, sort_keys=True, default=str).encode()
        return hashlib.sha256(state_bytes).hexdigest()[:16]
    
    async def save_checkpoint(
        self,
        agent_id: str,
        agent_type: str,
        state_data: Dict[str, Any],
        metadata: Optional[Dict] = None
    ) -> Checkpoint:
        """Save a new checkpoint for an agent."""
        async with self._lock:
            timestamp = time.time()
            checksum = self._compute_checksum(state_data)
            
            # Calculate version
            if agent_id not in self.checkpoints:
                self.checkpoints[agent_id] = []
            version = len(self.checkpoints[agent_id]) + 1
            
            checkpoint = Checkpoint(
                agent_id=agent_id,
                agent_type=agent_type,
                state_data=state_data,
                timestamp=timestamp,
                version=version,
                checksum=checksum,
                metadata=metadata or {}
            )
            
            self.checkpoints[agent_id].append(checkpoint)
            
            # Keep only recent checkpoints
            if len(self.checkpoints[agent_id]) > self.max_checkpoints:
                self.checkpoints[agent_id] = self.checkpoints[agent_id][-self.max_checkpoints:]
            
            # Persist to disk if enabled
            if self.storage_path:
                await self._persist_checkpoint(checkpoint)
            
            logger.debug(f"Saved checkpoint v{version} for {agent_id}")
            return checkpoint
    
    async def get_latest_checkpoint(self, agent_id: str) -> Optional[Checkpoint]:
        """Get the most recent checkpoint for an agent."""
        async with self._lock:
            if agent_id not in self.checkpoints or not self.checkpoints[agent_id]:
                return None
            return self.checkpoints[agent_id][-1]
    
    async def get_checkpoint_history(self, agent_id: str) -> List[Checkpoint]:
        """Get all checkpoints for an agent."""
        async with self._lock:
            return list(self.checkpoints.get(agent_id, []))
    
    async def verify_checkpoint(self, checkpoint: Checkpoint) -> bool:
        """Verify checkpoint integrity."""
        expected_checksum = self._compute_checksum(checkpoint.state_data)
        return expected_checksum == checkpoint.checksum
    
    async def delete_checkpoints(self, agent_id: str):
        """Delete all checkpoints for an agent."""
        async with self._lock:
            if agent_id in self.checkpoints:
                del self.checkpoints[agent_id]
            
            if self.storage_path:
                checkpoint_file = self.storage_path / f"{agent_id}.json"
                if checkpoint_file.exists():
                    checkpoint_file.unlink()
    
    async def _persist_checkpoint(self, checkpoint: Checkpoint):
        """Persist checkpoint to disk."""
        if not self.storage_path:
            return
        
        checkpoint_file = self.storage_path / f"{checkpoint.agent_id}.json"
        
        # Load existing or create new
        if checkpoint_file.exists():
            with open(checkpoint_file, 'r') as f:
                data = json.load(f)
        else:
            data = {"checkpoints": []}
        
        data["checkpoints"].append(checkpoint.to_dict())
        
        # Write atomically
        temp_file = checkpoint_file.with_suffix('.tmp')
        with open(temp_file, 'w') as f:
            json.dump(data, f, indent=2, default=str)
        temp_file.rename(checkpoint_file)
    
    def _load_from_disk(self):
        """Load checkpoints from disk on startup."""
        if not self.storage_path:
            return
        
        for checkpoint_file in self.storage_path.glob("*.json"):
            try:
                with open(checkpoint_file, 'r') as f:
                    data = json.load(f)
                
                agent_id = checkpoint_file.stem
                self.checkpoints[agent_id] = [
                    Checkpoint.from_dict(cp) for cp in data.get("checkpoints", [])
                ]
                logger.info(f"Loaded {len(self.checkpoints[agent_id])} checkpoints for {agent_id}")
            except Exception as e:
                logger.error(f"Failed to load checkpoint {checkpoint_file}: {e}")


class RecoveryOrchestrator:
    """
    Orchestrates automatic recovery of failed agents.
    
    Integrates with Health Monitor and Circuit Breaker to make
    intelligent recovery decisions with state restoration.
    """
    
    def __init__(
        self,
        state_store: Optional[StateStore] = None,
        max_retries: int = 3,
        recovery_timeout: float = 60.0,
        enable_auto_recovery: bool = True
    ):
        self.state_store = state_store or StateStore()
        self.max_retries = max_retries
        self.recovery_timeout = recovery_timeout
        self.enable_auto_recovery = enable_auto_recovery
        
        self.active_agents: Dict[str, Dict] = {}
        self.recovery_history: List[Dict] = []
        self.agent_factory: Optional[Callable] = None
        self._recovery_callbacks: List[Callable] = []
        self._lock = asyncio.Lock()
    
    def register_agent_factory(self, factory: Callable):
        """Register a factory function for creating agents."""
        self.agent_factory = factory
    
    def on_recovery(self, callback: Callable):
        """Register a callback for recovery events."""
        self._recovery_callbacks.append(callback)
    
    async def register_agent(
        self,
        agent_id: str,
        agent_type: str,
        initial_state: Optional[Dict] = None
    ):
        """Register an agent for monitoring and recovery."""
        async with self._lock:
            self.active_agents[agent_id] = {
                "agent_id": agent_id,
                "agent_type": agent_type,
                "status": "active",
                "created_at": time.time(),
                "state": initial_state or {},
                "restart_count": 0
            }
            
            # Save initial checkpoint
            if initial_state:
                await self.state_store.save_checkpoint(
                    agent_id, agent_type, initial_state,
                    metadata={"event": "initial_state"}
                )
        
        logger.info(f"Registered agent {agent_id} (type: {agent_type})")
    
    async def unregister_agent(self, agent_id: str):
        """Unregister an agent (called on graceful shutdown)."""
        async with self._lock:
            if agent_id in self.active_agents:
                del self.active_agents[agent_id]
        
        logger.info(f"Unregistered agent {agent_id}")
    
    async def save_state(self, agent_id: str, state_data: Dict, metadata: Optional[Dict] = None):
        """Manually save agent state to checkpoint."""
        if agent_id not in self.active_agents:
            logger.warning(f"Cannot save state for unknown agent {agent_id}")
            return None
        
        agent_type = self.active_agents[agent_id]["agent_type"]
        
        async with self._lock:
            self.active_agents[agent_id]["state"] = state_data
        
        checkpoint = await self.state_store.save_checkpoint(
            agent_id, agent_type, state_data, metadata
        )
        
        return checkpoint
    
    async def handle_failure(
        self,
        agent_id: str,
        error: Optional[str] = None,
        health_status: str = "unhealthy",
        circuit_state: str = "closed"
    ) -> RecoveryResult:
        """
        Handle an agent failure and attempt recovery.
        
        This is the main entry point for automatic recovery.
        """
        start_time = time.time()
        
        if not self.enable_auto_recovery:
            return RecoveryResult(
                success=False,
                strategy_used=RecoveryStrategy.ESCALATE,
                new_agent_id=None,
                error="Auto-recovery disabled",
                recovery_time_ms=0,
                checkpoint_restored=False
            )
        
        async with self._lock:
            if agent_id not in self.active_agents:
                return RecoveryResult(
                    success=False,
                    strategy_used=RecoveryStrategy.ESCALATE,
                    new_agent_id=None,
                    error=f"Unknown agent {agent_id}",
                    recovery_time_ms=0,
                    checkpoint_restored=False
                )
            
            agent_info = self.active_agents[agent_id]
            agent_info["status"] = "recovering"
            agent_info["restart_count"] += 1
            
            # Build recovery context
            checkpoint = await self.state_store.get_latest_checkpoint(agent_id)
            context = RecoveryContext(
                agent_id=agent_id,
                agent_type=agent_info["agent_type"],
                failure_count=agent_info["restart_count"],
                last_error=error,
                checkpoint_available=checkpoint is not None,
                checkpoint_age_seconds=time.time() - checkpoint.timestamp if checkpoint else float('inf'),
                circuit_state=circuit_state,
                health_status=health_status,
                available_backups=[]  # TODO: implement backup agents
            )
        
        # Determine recovery strategy
        strategy = self._select_strategy(context)
        logger.info(f"Selected recovery strategy {strategy.value} for {agent_id}")
        
        # Execute recovery
        result = await self._execute_recovery(agent_id, agent_info, strategy, checkpoint)
        
        # Update agent status
        async with self._lock:
            if result.success:
                self.active_agents[agent_id]["status"] = "active"
            else:
                self.active_agents[agent_id]["status"] = "failed"
        
        # Record recovery
        recovery_time = (time.time() - start_time) * 1000
        self.recovery_history.append({
            "agent_id": agent_id,
            "strategy": strategy.value,
            "success": result.success,
            "timestamp": datetime.now().isoformat(),
            "recovery_time_ms": recovery_time
        })
        
        # Notify callbacks
        for callback in self._recovery_callbacks:
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(result)
                else:
                    callback(result)
            except Exception as e:
                logger.error(f"Recovery callback error: {e}")
        
        return result
    
    def _select_strategy(self, context: RecoveryContext) -> RecoveryStrategy:
        """Select the appropriate recovery strategy based on context."""
        # Too many failures - escalate
        if context.failure_count > self.max_retries:
            return RecoveryStrategy.ESCALATE
        
        # Circuit is open - use degraded mode
        if context.circuit_state == "open":
            return RecoveryStrategy.DEGRADED_MODE
        
        # Fresh checkpoint available - restore state
        if context.checkpoint_available and context.checkpoint_age_seconds < 300:  # < 5 min old
            return RecoveryStrategy.RESTORE_STATE
        
        # Default: fresh restart
        return RecoveryStrategy.RESTART_FRESH
    
    async def _execute_recovery(
        self,
        agent_id: str,
        agent_info: Dict,
        strategy: RecoveryStrategy,
        checkpoint: Optional[Checkpoint]
    ) -> RecoveryResult:
        """Execute the selected recovery strategy."""
        start_time = time.time()
        
        if not self.agent_factory:
            return RecoveryResult(
                success=False,
                strategy_used=strategy,
                new_agent_id=None,
                error="No agent factory registered",
                recovery_time_ms=0,
                checkpoint_restored=False
            )
        
        try:
            if strategy == RecoveryStrategy.RESTORE_STATE and checkpoint:
                # Verify checkpoint integrity
                if not await self.state_store.verify_checkpoint(checkpoint):
                    logger.error(f"Checkpoint verification failed for {agent_id}")
                    strategy = RecoveryStrategy.RESTART_FRESH
                else:
                    new_agent = await self.agent_factory(
                        agent_id=agent_id,
                        agent_type=agent_info["agent_type"],
                        restored_state=checkpoint.state_data
                    )
                    recovery_time = (time.time() - start_time) * 1000
                    return RecoveryResult(
                        success=True,
                        strategy_used=strategy,
                        new_agent_id=agent_id,
                        error=None,
                        recovery_time_ms=recovery_time,
                        checkpoint_restored=True
                    )
            
            if strategy == RecoveryStrategy.RESTART_FRESH:
                new_agent = await self.agent_factory(
                    agent_id=agent_id,
                    agent_type=agent_info["agent_type"],
                    restored_state=None
                )
                recovery_time = (time.time() - start_time) * 1000
                return RecoveryResult(
                    success=True,
                    strategy_used=strategy,
                    new_agent_id=agent_id,
                    error=None,
                    recovery_time_ms=recovery_time,
                    checkpoint_restored=False
                )
            
            if strategy == RecoveryStrategy.DEGRADED_MODE:
                new_agent = await self.agent_factory(
                    agent_id=agent_id,
                    agent_type=agent_info["agent_type"],
                    restored_state=checkpoint.state_data if checkpoint else None,
                    degraded=True
                )
                recovery_time = (time.time() - start_time) * 1000
                return RecoveryResult(
                    success=True,
                    strategy_used=strategy,
                    new_agent_id=agent_id,
                    error=None,
                    recovery_time_ms=recovery_time,
                    checkpoint_restored=checkpoint is not None
                )
            
            return RecoveryResult(
                success=False,
                strategy_used=strategy,
                new_agent_id=None,
                error=f"Strategy {strategy.value} not implemented",
                recovery_time_ms=(time.time() - start_time) * 1000,
                checkpoint_restored=False
            )
            
        except Exception as e:
            recovery_time = (time.time() - start_time) * 1000
            logger.error(f"Recovery execution failed: {e}")
            return RecoveryResult(
                success=False,
                strategy_used=strategy,
                new_agent_id=None,
                error=str(e),
                recovery_time_ms=recovery_time,
                checkpoint_restored=False
            )
    
    async def get_recovery_stats(self) -> Dict:
        """Get statistics on recovery operations."""
        total = len(self.recovery_history)
        successful = sum(1 for r in self.recovery_history if r["success"])
        
        return {
            "total_recoveries": total,
            "successful_recoveries": successful,
            "success_rate": successful / total if total > 0 else 0.0,
            "active_agents": len(self.active_agents),
            "avg_recovery_time_ms": sum(r["recovery_time_ms"] for r in self.recovery_history) / total if total > 0 else 0,
            "recent_history": self.recovery_history[-10:]
        }


# Singleton
_recovery_orchestrator: Optional[RecoveryOrchestrator] = None


def get_recovery_orchestrator() -> RecoveryOrchestrator:
    """Get or create the global recovery orchestrator."""
    global _recovery_orchestrator
    if _recovery_orchestrator is None:
        _recovery_orchestrator = RecoveryOrchestrator()
    return _recovery_orchestrator
