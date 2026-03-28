"""
Autonomy Broker - Drop-in replacement for subagent spawning with resilience.

This is the main entry point for the AUTONOMY-ENGINE. It wraps all the
resilience components (health monitoring, failure prediction, circuit breaking,
recovery orchestration) into a clean interface that can replace direct
subagent spawning.

Features:
- Automatic health monitoring for all spawned agents
- ML-based failure prediction on agent outputs
- Circuit breaker protection per agent
- Automatic recovery with state restoration
- Self-optimization of retry strategies
"""

import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional, Callable, Type, Union
from dataclasses import dataclass, field
from datetime import datetime
import logging

from health_monitor import HealthMonitor, HealthStatus, get_health_monitor
from failure_predictor import FailurePredictor, FailureType, get_failure_predictor
from circuit_breaker import CircuitBreakerRegistry, CircuitOpenError, get_circuit_registry
from recovery_orchestrator import RecoveryOrchestrator, RecoveryStrategy, StateStore, get_recovery_orchestrator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("AutonomyBroker")


@dataclass
class SpawnConfig:
    """Configuration for spawning a resilient agent."""
    agent_id: Optional[str] = None
    agent_type: str = "default"
    enable_health_monitoring: bool = True
    enable_failure_prediction: bool = True
    enable_circuit_breaker: bool = True
    enable_auto_recovery: bool = True
    heartbeat_interval_sec: float = 30.0
    heartbeat_timeout_sec: float = 90.0
    max_retries: int = 3
    retry_delay_base: float = 1.0
    retry_delay_max: float = 60.0
    timeout_seconds: float = 300.0
    initial_state: Optional[Dict] = None
    fallback_agent_type: Optional[str] = None  # Degrade to this on failure
    checkpoint_interval_sec: Optional[float] = 60.0


@dataclass
class SpawnResult:
    """Result of spawning a resilient agent."""
    agent_id: str
    success: bool
    result: Any
    error: Optional[str]
    execution_time_ms: float
    retry_count: int
    health_status: str
    failure_detected: bool
    recovery_performed: bool


@dataclass
class AgentHandle:
    """Handle to a managed agent."""
    agent_id: str
    agent_type: str
    config: SpawnConfig
    spawn_result: Optional[SpawnResult] = None
    
    async def call(self, *args, **kwargs) -> Any:
        """Call the agent through the autonomy broker."""
        return await get_autonomy_broker().call_agent(self.agent_id, *args, **kwargs)
    
    async def heartbeat(self, latency_ms: float = 0.0, metadata: Optional[Dict] = None):
        """Send a heartbeat for this agent."""
        return await get_autonomy_broker().heartbeat(self.agent_id, latency_ms, metadata)
    
    async def checkpoint(self, state: Dict, metadata: Optional[Dict] = None):
        """Save a state checkpoint."""
        return await get_autonomy_broker().checkpoint(self.agent_id, state, metadata)
    
    async def status(self) -> str:
        """Get current health status."""
        return await get_autonomy_broker().get_agent_status(self.agent_id)


class RetryPolicy:
    """
    Self-optimizing retry policy that learns from historical success patterns.
    
    Tracks success/failure per agent type and adjusts delays accordingly.
    """
    
    def __init__(self):
        self.type_stats: Dict[str, Dict] = {}
        self.default_base_delay = 1.0
    
    def record_outcome(self, agent_type: str, retry_num: int, success: bool, duration_ms: float):
        """Record an outcome to improve future retry decisions."""
        if agent_type not in self.type_stats:
            self.type_stats[agent_type] = {
                "total_attempts": 0,
                "total_successes": 0,
                "retry_successes": {i: {"attempts": 0, "successes": 0} for i in range(5)}
            }
        
        stats = self.type_stats[agent_type]
        stats["total_attempts"] += 1
        if success:
            stats["total_successes"] += 1
        
        if retry_num > 0 and retry_num < 5:
            stats["retry_successes"][retry_num]["attempts"] += 1
            if success:
                stats["retry_successes"][retry_num]["successes"] += 1
    
    def get_delay(self, agent_type: str, retry_num: int, base_delay: float, max_delay: float) -> float:
        """Calculate retry delay with optimization."""
        # Exponential backoff
        delay = base_delay * (2 ** retry_num)
        
        # Adjust based on agent type success patterns
        if agent_type in self.type_stats:
            stats = self.type_stats[agent_type]
            success_rate = stats["total_successes"] / stats["total_attempts"] if stats["total_attempts"] > 0 else 0.5
            
            # Reduce delay for agent types with high success rates
            if success_rate > 0.8:
                delay *= 0.8
            elif success_rate < 0.3:
                delay *= 1.2
        
        # Add jitter
        import random
        delay *= (0.9 + random.random() * 0.2)
        
        return min(delay, max_delay)
    
    def should_retry(self, agent_type: str, retry_num: int, max_retries: int) -> bool:
        """Determine if we should attempt another retry."""
        if retry_num >= max_retries:
            return False
        
        # Check if retries tend to succeed for this agent type
        if agent_type in self.type_stats:
            stats = self.type_stats[agent_type]
            if retry_num + 1 in stats["retry_successes"]:
                retry_stats = stats["retry_successes"][retry_num + 1]
                if retry_stats["attempts"] > 5:
                    success_rate = retry_stats["successes"] / retry_stats["attempts"]
                    return success_rate > 0.1  # Retry if >10% success rate
        
        return True


class AutonomyBroker:
    """
    Main entry point for spawning resilient agents.
    
    Integrates all resilience components:
    - Health Monitor: Tracks agent health via heartbeats
    - Failure Predictor: Detects anomalous outputs
    - Circuit Breaker: Prevents cascading failures
    - Recovery Orchestrator: Auto-recovery with checkpoint restoration
    """
    
    def __init__(
        self,
        health_monitor: Optional[HealthMonitor] = None,
        failure_predictor: Optional[FailurePredictor] = None,
        circuit_registry: Optional[CircuitBreakerRegistry] = None,
        recovery_orchestrator: Optional[RecoveryOrchestrator] = None
    ):
        self.health_monitor = health_monitor or get_health_monitor()
        self.failure_predictor = failure_predictor or get_failure_predictor()
        self.circuit_registry = circuit_registry or get_circuit_registry()
        self.recovery = recovery_orchestrator or get_recovery_orchestrator()
        
        self.retry_policy = RetryPolicy()
        self._agent_functions: Dict[str, Callable] = {}
        self._config: Dict[str, SpawnConfig] = {}
        self._running = False
    
    async def start(self):
        """Start all background monitoring."""
        await self.health_monitor.start()
        self._running = True
        logger.info("Autonomy broker started")
    
    async def stop(self):
        """Stop all background monitoring."""
        await self.health_monitor.stop()
        self._running = False
        logger.info("Autonomy broker stopped")
    
    async def spawn(
        self,
        agent_func: Callable,
        config: Optional[SpawnConfig] = None,
        *args,
        **kwargs
    ) -> SpawnResult:
        """
        Spawn a resilient agent with full resilience wrapping.
        
        This is the main API for creating managed agents.
        """
        config = config or SpawnConfig()
        agent_id = config.agent_id or f"agent-{uuid.uuid4().hex[:8]}"
        config.agent_id = agent_id
        
        self._agent_functions[agent_id] = agent_func
        self._config[agent_id] = config
        
        start_time = time.time()
        retry_count = 0
        last_error = None
        
        # Register with health monitor
        if config.enable_health_monitoring:
            await self.health_monitor.register_agent(agent_id, config.agent_type)
            await self.health_monitor.heartbeat(agent_id, 0.0)
        
        # Register with recovery orchestrator
        if config.enable_auto_recovery:
            await self.recovery.register_agent(agent_id, config.agent_type, config.initial_state)
        
        # Get circuit breaker
        breaker = None
        if config.enable_circuit_breaker:
            breaker = self.circuit_registry.get_circuit_breaker(agent_id, config.agent_type)
        
        # Attempt execution with retries
        while retry_count <= config.max_retries:
            try:
                execution_start = time.time()
                
                # Call through circuit breaker if enabled
                if breaker:
                    result = await breaker.call(agent_func, *args, **kwargs)
                else:
                    result = await agent_func(*args, **kwargs) if asyncio.iscoroutinefunction(agent_func) else agent_func(*args, **kwargs)
                
                execution_time = (time.time() - execution_start) * 1000
                
                # Success - record health
                if config.enable_health_monitoring:
                    status = await self.health_monitor.heartbeat(agent_id, execution_time)
                else:
                    status = HealthStatus.HEALTHY
                
                # Predict failure from output
                failure_detected = False
                if config.enable_failure_prediction:
                    prediction = self.failure_predictor.predict(agent_id, config.agent_type, result)
                    if prediction.is_anomaly:
                        failure_detected = True
                        should_retry, reason = self.failure_predictor.should_retry(prediction)
                        if should_retry and retry_count < config.max_retries:
                            logger.warning(f"Failure predicted for {agent_id}: {reason}")
                            retry_count += 1
                            delay = self.retry_policy.get_delay(
                                config.agent_type, retry_count, 
                                config.retry_delay_base, config.retry_delay_max
                            )
                            await asyncio.sleep(delay)
                            continue
                
                # Record success in retry policy
                self.retry_policy.record_outcome(
                    config.agent_type, retry_count, True, execution_time
                )
                
                total_time = (time.time() - start_time) * 1000
                return SpawnResult(
                    agent_id=agent_id,
                    success=True,
                    result=result,
                    error=None,
                    execution_time_ms=total_time,
                    retry_count=retry_count,
                    health_status=status.value if hasattr(status, 'value') else str(status),
                    failure_detected=failure_detected,
                    recovery_performed=False
                )
                
            except CircuitOpenError:
                # Circuit breaker is open - attempt recovery
                logger.warning(f"Circuit open for {agent_id}, initiating recovery")
                recovery_result = await self.recovery.handle_failure(
                    agent_id,
                    error="Circuit breaker open",
                    circuit_state="open"
                )
                
                # Try again with recovered agent
                if recovery_result.success:
                    retry_count += 1
                    continue
                else:
                    total_time = (time.time() - start_time) * 1000
                    return SpawnResult(
                        agent_id=agent_id,
                        success=False,
                        result=None,
                        error=f"Recovery failed: {recovery_result.error}",
                        execution_time_ms=total_time,
                        retry_count=retry_count,
                        health_status="unhealthy",
                        failure_detected=True,
                        recovery_performed=True
                    )
                    
            except Exception as e:
                execution_time = (time.time() - start_time) * 1000
                last_error = str(e)
                
                # Record failure in retry policy
                self.retry_policy.record_outcome(
                    config.agent_type, retry_count, False, execution_time
                )
                
                # Update health status
                if config.enable_health_monitoring:
                    await self.health_monitor.heartbeat(agent_id, execution_time * 10)  # High latency for failures
                
                logger.warning(f"Agent {agent_id} failed (attempt {retry_count + 1}): {e}")
                
                # Check if we should retry
                if not self.retry_policy.should_retry(config.agent_type, retry_count, config.max_retries):
                    break
                
                retry_count += 1
                if retry_count <= config.max_retries:
                    delay = self.retry_policy.get_delay(
                        config.agent_type, retry_count - 1,
                        config.retry_delay_base, config.retry_delay_max
                    )
                    logger.info(f"Retrying {agent_id} in {delay:.1f}s (attempt {retry_count + 1})")
                    await asyncio.sleep(delay)
        
        # All retries exhausted
        total_time = (time.time() - start_time) * 1000
        
        # Attempt final recovery
        recovery_performed = False
        if config.enable_auto_recovery:
            recovery_result = await self.recovery.handle_failure(
                agent_id,
                error=last_error,
                circuit_state=breaker.get_state().value if breaker else "closed"
            )
            recovery_performed = recovery_result.success
        
        return SpawnResult(
            agent_id=agent_id,
            success=False,
            result=None,
            error=f"Failed after {retry_count + 1} attempts: {last_error}",
            execution_time_ms=total_time,
            retry_count=retry_count,
            health_status="dead",
            failure_detected=True,
            recovery_performed=recovery_performed
        )
    
    async def spawn_handle(
        self,
        agent_func: Callable,
        config: Optional[SpawnConfig] = None,
        *args,
        **kwargs
    ) -> AgentHandle:
        """
        Spawn a resilient agent and return a handle for future calls.
        """
        config = config or SpawnConfig()
        agent_id = config.agent_id or f"agent-{uuid.uuid4().hex[:8]}"
        config.agent_id = agent_id
        
        # Store function
        self._agent_functions[agent_id] = agent_func
        self._config[agent_id] = config
        
        # Register with systems
        if config.enable_health_monitoring:
            await self.health_monitor.register_agent(agent_id, config.agent_type)
        if config.enable_auto_recovery:
            await self.recovery.register_agent(agent_id, config.agent_type, config.initial_state)
        
        return AgentHandle(agent_id=agent_id, agent_type=config.agent_type, config=config)
    
    async def call_agent(self, agent_id: str, *args, **kwargs) -> Any:
        """Call an existing agent through the broker."""
        if agent_id not in self._agent_functions:
            raise ValueError(f"Unknown agent: {agent_id}")
        
        func = self._agent_functions[agent_id]
        config = self._config[agent_id]
        
        spawn_result = await self.spawn(func, config, *args, **kwargs)
        if not spawn_result.success:
            raise Exception(f"Agent call failed: {spawn_result.error}")
        
        return spawn_result.result
    
    async def heartbeat(self, agent_id: str, latency_ms: float = 0.0, metadata: Optional[Dict] = None):
        """Send a heartbeat for an agent."""
        return await self.health_monitor.heartbeat(agent_id, latency_ms, metadata)
    
    async def checkpoint(self, agent_id: str, state: Dict, metadata: Optional[Dict] = None):
        """Save a checkpoint for an agent."""
        return await self.recovery.save_state(agent_id, state, metadata)
    
    async def get_agent_status(self, agent_id: str) -> str:
        """Get health status of an agent."""
        health = await self.health_monitor.get_agent_health(agent_id)
        if health:
            return health.status.value
        return "unknown"
    
    async def get_swarm_health(self) -> Dict:
        """Get health summary of entire agent swarm."""
        return await self.health_monitor.get_swarm_health_summary()
    
    async def get_circuit_stats(self) -> Dict:
        """Get circuit breaker statistics."""
        return self.circuit_registry.get_all_stats()
    
    async def get_recovery_stats(self) -> Dict:
        """Get recovery statistics."""
        return await self.recovery.get_recovery_stats()
    
    async def get_full_dashboard(self) -> Dict:
        """Get complete dashboard data."""
        return {
            "swarm_health": await self.get_swarm_health(),
            "circuit_stats": await self.get_circuit_stats(),
            "recovery_stats": await self.get_recovery_stats(),
            "retry_policy": {
                "type_stats": self.retry_policy.type_stats
            },
            "timestamp": datetime.now().isoformat()
        }


# Singleton
_autonomy_broker: Optional[AutonomyBroker] = None


def get_autonomy_broker() -> AutonomyBroker:
    """Get or create the global autonomy broker."""
    global _autonomy_broker
    if _autonomy_broker is None:
        _autonomy_broker = AutonomyBroker()
    return _autonomy_broker


# Convenience function for direct use
async def resilient_spawn(
    agent_func: Callable,
    *args,
    agent_type: str = "default",
    max_retries: int = 3,
    **kwargs
) -> SpawnResult:
    """
    Convenience function to spawn a resilient agent.
    
    Example:
        result = await resilient_spawn(
            my_agent_function,
            data_input,
            agent_type="data_processor",
            max_retries=3
        )
    """
    config = SpawnConfig(
        agent_type=agent_type,
        max_retries=max_retries
    )
    return await get_autonomy_broker().spawn(agent_func, config, *args, **kwargs)
