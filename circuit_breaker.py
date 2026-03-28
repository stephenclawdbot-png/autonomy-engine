"""
Circuit Breaker - Prevents cascading failures in agent swarms.

Implements the circuit breaker pattern with three states:
- CLOSED: Normal operation, requests pass through
- OPEN: Failure threshold exceeded, requests fail fast
- HALF_OPEN: Testing if service recovered

Includes automatic recovery, adaptive timeouts, and per-agent-type configuration.
"""

import asyncio
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Type, Union
from functools import wraps
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("CircuitBreaker")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass 
class CircuitBreakerConfig:
    """Configuration for a circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 5
    exponential_backoff_base: float = 2.0
    max_timeout_seconds: float = 300.0
    jitter: bool = True


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker monitoring."""
    failures: int = 0
    successes: int = 0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    total_calls: int = 0
    total_failures: int = 0
    total_successes: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None
    state_changes: int = 0
    opened_count: int = 0


class CircuitBreaker:
    """
    Circuit breaker for agent calls.
    
    Protects the system from cascading failures by stopping calls to
    agents that are consistently failing, while periodically testing
    if they've recovered.
    """
    
    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        on_state_change: Optional[Callable] = None
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.on_state_change = on_state_change
        
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0
        self.last_state_change = time.time()
        self._lock = asyncio.Lock()
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function through the circuit breaker.
        
        Returns the function result or raises CircuitOpenError if circuit is open.
        """
        async with self._lock:
            await self._transition_state()
            
            if self.state == CircuitState.OPEN:
                raise CircuitOpenError(f"Circuit {self.name} is OPEN")
            
            if self.state == CircuitState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise CircuitOpenError(f"Circuit {self.name} HALF_OPEN limit reached")
                self.half_open_calls += 1
        
        # Execute outside lock
        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            await self.record_success()
            return result
        except Exception as e:
            await self.record_failure()
            raise
    
    async def _transition_state(self):
        """Check and perform state transitions."""
        if self.state == CircuitState.OPEN:
            # Check if timeout elapsed for HALF_OPEN
            time_in_open = time.time() - self.last_state_change
            timeout = self._calculate_timeout()
            
            if time_in_open >= timeout:
                await self._set_state(CircuitState.HALF_OPEN)
                self.half_open_calls = 0
                logger.info(f"Circuit {self.name} transitioning OPEN -> HALF_OPEN")
        
        elif self.state == CircuitState.HALF_OPEN:
            # Check if half-open calls succeeded
            if self.stats.consecutive_successes >= self.config.success_threshold:
                await self._set_state(CircuitState.CLOSED)
                logger.info(f"Circuit {self.name} transitioning HALF_OPEN -> CLOSED")
            elif self.stats.consecutive_failures >= self.config.failure_threshold:
                await self._set_state(CircuitState.OPEN)
                logger.info(f"Circuit {self.name} transitioning HALF_OPEN -> OPEN")
    
    async def _set_state(self, new_state: CircuitState):
        """Set the circuit breaker state."""
        old_state = self.state
        self.state = new_state
        self.last_state_change = time.time()
        self.stats.state_changes += 1
        
        if new_state == CircuitState.OPEN:
            self.stats.opened_count += 1
        elif new_state == CircuitState.CLOSED:
            # Reset counters on close
            self.stats.consecutive_failures = 0
            self.stats.consecutive_successes = 0
            self.half_open_calls = 0
        
        if self.on_state_change:
            try:
                if asyncio.iscoroutinefunction(self.on_state_change):
                    await self.on_state_change(self.name, old_state, new_state)
                else:
                    self.on_state_change(self.name, old_state, new_state)
            except Exception as e:
                logger.error(f"State change callback error: {e}")
    
    async def record_success(self):
        """Record a successful call."""
        async with self._lock:
            self.stats.successes += 1
            self.stats.total_successes += 1
            self.stats.total_calls += 1
            self.stats.consecutive_successes += 1
            self.stats.consecutive_failures = 0
            self.stats.last_success_time = time.time()
            
            await self._transition_state()
    
    async def record_failure(self):
        """Record a failed call."""
        async with self._lock:
            self.stats.failures += 1
            self.stats.total_failures += 1
            self.stats.total_calls += 1
            self.stats.consecutive_failures += 1
            self.stats.consecutive_successes = 0
            self.stats.last_failure_time = time.time()
            
            # Transition to OPEN if threshold reached
            if (self.state == CircuitState.CLOSED and 
                self.stats.consecutive_failures >= self.config.failure_threshold):
                await self._set_state(CircuitState.OPEN)
                logger.warning(f"Circuit {self.name} opened after {self.stats.consecutive_failures} failures")
            else:
                await self._transition_state()
    
    def _calculate_timeout(self) -> float:
        """Calculate current timeout with exponential backoff."""
        base = self.config.timeout_seconds
        multiplier = self.config.exponential_backoff_base ** self.stats.opened_count
        timeout = min(base * multiplier, self.config.max_timeout_seconds)
        
        if self.config.jitter:
            # Add random jitter (±10%)
            import random
            timeout *= (0.9 + random.random() * 0.2)
        
        return timeout
    
    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        return self.state
    
    def get_stats(self) -> CircuitBreakerStats:
        """Get circuit statistics."""
        return self.stats
    
    def reset(self):
        """Manually reset the circuit breaker."""
        self.state = CircuitState.CLOSED
        self.stats = CircuitBreakerStats()
        self.half_open_calls = 0
        self.last_state_change = time.time()
        logger.info(f"Circuit {self.name} manually reset")


class CircuitOpenError(Exception):
    """Raised when calling through an open circuit."""
    pass


class CircuitBreakerRegistry:
    """
    Registry for managing multiple circuit breakers.
    
    Provides per-agent-type circuit breakers with shared configuration.
    """
    
    def __init__(self):
        self.breakers: Dict[str, CircuitBreaker] = {}
        self.default_config = CircuitBreakerConfig()
        self.type_configs: Dict[str, CircuitBreakerConfig] = {}
    
    def register_type_config(self, agent_type: str, config: CircuitBreakerConfig):
        """Register a custom config for an agent type."""
        self.type_configs[agent_type] = config
    
    def get_circuit_breaker(
        self, 
        agent_id: str, 
        agent_type: str = "default",
        on_state_change: Optional[Callable] = None
    ) -> CircuitBreaker:
        """Get or create a circuit breaker for an agent."""
        if agent_id not in self.breakers:
            config = self.type_configs.get(agent_type, self.default_config)
            breaker = CircuitBreaker(
                name=agent_id,
                config=config,
                on_state_change=on_state_change
            )
            self.breakers[agent_id] = breaker
            logger.info(f"Created circuit breaker for {agent_id} (type: {agent_type})")
        
        return self.breakers[agent_id]
    
    async def call_with_breaker(
        self,
        agent_id: str,
        agent_type: str,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Call a function through the appropriate circuit breaker."""
        breaker = self.get_circuit_breaker(agent_id, agent_type)
        return await breaker.call(func, *args, **kwargs)
    
    def get_all_stats(self) -> Dict[str, Dict]:
        """Get statistics for all circuits."""
        return {
            agent_id: {
                "state": breaker.state.value,
                "stats": {
                    "total_calls": breaker.stats.total_calls,
                    "total_failures": breaker.stats.total_failures,
                    "total_successes": breaker.stats.total_successes,
                    "opened_count": breaker.stats.opened_count
                }
            }
            for agent_id, breaker in self.breakers.items()
        }
    
    def reset_all(self):
        """Reset all circuit breakers."""
        for breaker in self.breakers.values():
            breaker.reset()
    
    def remove_breaker(self, agent_id: str):
        """Remove a circuit breaker."""
        if agent_id in self.breakers:
            del self.breakers[agent_id]


# Singleton registry
_circuit_registry: Optional[CircuitBreakerRegistry] = None


def get_circuit_registry() -> CircuitBreakerRegistry:
    """Get or create the global circuit breaker registry."""
    global _circuit_registry
    if _circuit_registry is None:
        _circuit_registry = CircuitBreakerRegistry()
    return _circuit_registry


def circuit_breaker(
    agent_id: str,
    agent_type: str = "default",
    fallback: Optional[Callable] = None
):
    """
    Decorator to wrap a function with circuit breaker protection.
    
    Example:
        @circuit_breaker("my_agent", "data_processor")
        async def process_data(data):
            # Processing logic
            pass
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs):
            registry = get_circuit_registry()
            breaker = registry.get_circuit_breaker(agent_id, agent_type)
            
            try:
                return await breaker.call(func, *args, **kwargs)
            except CircuitOpenError:
                if fallback:
                    return await fallback(*args, **kwargs) if asyncio.iscoroutinefunction(fallback) else fallback(*args, **kwargs)
                raise
        
        return wrapper
    return decorator
