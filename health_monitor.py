"""
Health Monitor - Tracks subagent heartbeats, latency histograms, and SLA compliance.

Provides real-time visibility into agent swarm health with configurable thresholds,
exponential decay histograms, and automatic degradation detection.
"""

import asyncio
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from collections import deque
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("HealthMonitor")


class HealthStatus(Enum):
    """Agent health states."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    DEAD = "dead"
    UNKNOWN = "unknown"


@dataclass
class AgentHealthRecord:
    """Complete health record for a single agent."""
    agent_id: str
    agent_type: str
    status: HealthStatus = HealthStatus.UNKNOWN
    last_heartbeat: float = 0.0
    heartbeats_received: int = 0
    heartbeats_missed: int = 0
    latency_ms: deque = field(default_factory=lambda: deque(maxlen=100))
    errors: deque = field(default_factory=lambda: deque(maxlen=50))
    restarts: int = 0
    created_at: float = field(default_factory=time.time)
    last_status_change: float = field(default_factory=time.time)
    sla_violations: int = 0
    
    def __post_init__(self):
        if self.last_heartbeat == 0.0:
            self.last_heartbeat = time.time()


@dataclass
class LatencyHistogram:
    """Exponentially decaying histogram for latency distribution."""
    buckets: List[int] = field(default_factory=lambda: [10, 50, 100, 250, 500, 1000, 2500, 5000, 10000])
    counts: Dict[int, int] = field(default_factory=dict)
    total_samples: int = 0
    decay_factor: float = 0.95
    
    def __post_init__(self):
        for bucket in self.buckets:
            self.counts[bucket] = 0
    
    def add(self, latency_ms: float):
        """Add a latency sample with exponential decay."""
        # Decay existing counts
        for bucket in self.buckets:
            self.counts[bucket] = int(self.counts[bucket] * self.decay_factor)
        
        # Add new sample
        for bucket in self.buckets:
            if latency_ms <= bucket:
                self.counts[bucket] += 1
                break
        else:
            # Beyond last bucket
            self.counts[self.buckets[-1]] += 1
        
        self.total_samples += 1
    
    def percentile(self, p: float) -> float:
        """Calculate approximate percentile latency."""
        if self.total_samples == 0:
            return 0.0
        
        target = int(self.total_samples * p / 100)
        cumulative = 0
        
        for bucket in sorted(self.buckets):
            cumulative += self.counts[bucket]
            if cumulative >= target:
                return bucket
        
        return self.buckets[-1]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "p50": self.percentile(50),
            "p95": self.percentile(95),
            "p99": self.percentile(99),
            "buckets": {str(k): v for k, v in self.counts.items()}
        }


class HealthMonitor:
    """
    Central health monitoring for all subagents in the swarm.
    
    Tracks heartbeats, maintains latency histograms, detects SLA violations,
    and provides real-time health status for recovery decisions.
    """
    
    def __init__(
        self,
        heartbeat_interval_sec: float = 30.0,
        heartbeat_timeout_sec: float = 90.0,
        degraded_threshold_ms: float = 5000.0,
        unhealthy_threshold_ms: float = 30000.0,
        sla_target_ms: float = 2000.0,
        history_retention_hours: float = 24.0
    ):
        self.heartbeat_interval = heartbeat_interval_sec
        self.heartbeat_timeout = heartbeat_timeout_sec
        self.degraded_threshold = degraded_threshold_ms
        self.unhealthy_threshold = unhealthy_threshold_ms
        self.sla_target = sla_target_ms
        
        self.agents: Dict[str, AgentHealthRecord] = {}
        self.latency_histograms: Dict[str, LatencyHistogram] = {}
        self.callbacks: Dict[str, List[Callable]] = {
            "status_change": [],
            "sla_violation": [],
            "missed_heartbeat": []
        }
        
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
    
    async def register_agent(self, agent_id: str, agent_type: str) -> AgentHealthRecord:
        """Register a new agent for health monitoring."""
        async with self._lock:
            record = AgentHealthRecord(
                agent_id=agent_id,
                agent_type=agent_type,
                status=HealthStatus.HEALTHY,
                last_heartbeat=time.time()
            )
            self.agents[agent_id] = record
            self.latency_histograms[agent_id] = LatencyHistogram()
            logger.info(f"Registered agent {agent_id} (type: {agent_type})")
            return record
    
    async def unregister_agent(self, agent_id: str):
        """Remove an agent from health monitoring."""
        async with self._lock:
            if agent_id in self.agents:
                del self.agents[agent_id]
                del self.latency_histograms[agent_id]
                logger.info(f"Unregistered agent {agent_id}")
    
    async def heartbeat(
        self, 
        agent_id: str, 
        latency_ms: float = 0.0,
        metadata: Optional[Dict] = None
    ) -> HealthStatus:
        """
        Process a heartbeat from an agent.
        
        Returns the current health status and triggers callbacks if status changed.
        """
        async with self._lock:
            if agent_id not in self.agents:
                logger.warning(f"Heartbeat from unknown agent {agent_id}")
                return HealthStatus.UNKNOWN
            
            record = self.agents[agent_id]
            record.heartbeats_received += 1
            record.latency_ms.append(latency_ms)
            record.last_heartbeat = time.time()
            
            # Update latency histogram
            self.latency_histograms[agent_id].add(latency_ms)
            
            # Check SLA violations
            if latency_ms > self.sla_target:
                record.sla_violations += 1
                await self._trigger_callback("sla_violation", agent_id, latency_ms)
            
            # Determine new status based on latency
            old_status = record.status
            new_status = self._calculate_status(record, latency_ms)
            
            if new_status != old_status:
                record.status = new_status
                record.last_status_change = time.time()
                logger.info(f"Agent {agent_id} status changed: {old_status.value} -> {new_status.value}")
                await self._trigger_callback("status_change", agent_id, old_status, new_status)
            
            return record.status
    
    def _calculate_status(self, record: AgentHealthRecord, latency_ms: float) -> HealthStatus:
        """Calculate health status based on current metrics."""
        time_since_heartbeat = time.time() - record.last_heartbeat
        
        if time_since_heartbeat > self.heartbeat_timeout * 2:
            return HealthStatus.DEAD
        elif time_since_heartbeat > self.heartbeat_timeout:
            return HealthStatus.UNHEALTHY
        elif latency_ms > self.unhealthy_threshold or record.sla_violations > 10:
            return HealthStatus.UNHEALTHY
        elif latency_ms > self.degraded_threshold or record.sla_violations > 3:
            return HealthStatus.DEGRADED
        
        return HealthStatus.HEALTHY
    
    async def _check_missed_heartbeats(self):
        """Periodic check for agents that missed heartbeats."""
        async with self._lock:
            current_time = time.time()
            for agent_id, record in self.agents.items():
                time_since = current_time - record.last_heartbeat
                
                if time_since > self.heartbeat_timeout:
                    record.heartbeats_missed += 1
                    old_status = record.status
                    new_status = HealthStatus.UNHEALTHY if time_since < self.heartbeat_timeout * 2 else HealthStatus.DEAD
                    
                    if new_status != old_status:
                        record.status = new_status
                        record.last_status_change = current_time
                        await self._trigger_callback("status_change", agent_id, old_status, new_status)
                    
                    await self._trigger_callback("missed_heartbeat", agent_id, time_since)
                    logger.warning(f"Agent {agent_id} missed heartbeat ({time_since:.1f}s ago)")
    
    async def get_agent_health(self, agent_id: str) -> Optional[AgentHealthRecord]:
        """Get current health record for an agent."""
        async with self._lock:
            return self.agents.get(agent_id)
    
    async def get_all_health(self) -> Dict[str, AgentHealthRecord]:
        """Get health records for all agents."""
        async with self._lock:
            return dict(self.agents)
    
    async def get_swarm_health_summary(self) -> Dict[str, Any]:
        """Get aggregate health metrics for the entire swarm."""
        async with self._lock:
            total = len(self.agents)
            if total == 0:
                return {"status": "empty", "total_agents": 0}
            
            status_counts = {status.value: 0 for status in HealthStatus}
            for record in self.agents.values():
                status_counts[record.status.value] += 1
            
            return {
                "status": "critical" if status_counts[HealthStatus.DEAD.value] > 0 else
                         "degraded" if status_counts[HealthStatus.UNHEALTHY.value] > 0 else
                         "healthy",
                "total_agents": total,
                "healthy_count": status_counts[HealthStatus.HEALTHY.value],
                "degraded_count": status_counts[HealthStatus.DEGRADED.value],
                "unhealthy_count": status_counts[HealthStatus.UNHEALTHY.value],
                "dead_count": status_counts[HealthStatus.DEAD.value],
                "by_status": status_counts,
                "timestamp": datetime.now().isoformat()
            }
    
    async def get_agent_latency_stats(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get latency statistics for an agent."""
        async with self._lock:
            histogram = self.latency_histograms.get(agent_id)
            if histogram:
                return histogram.to_dict()
            return None
    
    def on(self, event: str, callback: Callable):
        """Register a callback for health events."""
        if event in self.callbacks:
            self.callbacks[event].append(callback)
    
    async def _trigger_callback(self, event: str, *args):
        """Trigger all callbacks for an event."""
        for callback in self.callbacks.get(event, []):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback(*args)
                else:
                    callback(*args)
            except Exception as e:
                logger.error(f"Callback error for {event}: {e}")
    
    async def start(self):
        """Start the health monitoring loop."""
        self._running = True
        self._task = asyncio.create_task(self._monitoring_loop())
        logger.info("Health monitor started")
    
    async def stop(self):
        """Stop the health monitoring loop."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Health monitor stopped")
    
    async def _monitoring_loop(self):
        """Main monitoring loop - checks for missed heartbeats."""
        while self._running:
            try:
                await self._check_missed_heartbeats()
                await asyncio.sleep(self.heartbeat_interval / 2)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Monitoring loop error: {e}")
                await asyncio.sleep(5)
    
    async def export_metrics(self) -> Dict[str, Any]:
        """Export all metrics for external consumption."""
        async with self._lock:
            return {
                "agents": {
                    agent_id: {
                        "status": record.status.value,
                        "last_heartbeat": record.last_heartbeat,
                        "heartbeats_received": record.heartbeats_received,
                        "heartbeats_missed": record.heartbeats_missed,
                        "sla_violations": record.sla_violations,
                        "restarts": record.restarts,
                        "latency_stats": self.latency_histograms[agent_id].to_dict() if agent_id in self.latency_histograms else None
                    }
                    for agent_id, record in self.agents.items()
                },
                "timestamp": datetime.now().isoformat()
            }


# Singleton instance for the autonomy engine
_health_monitor: Optional[HealthMonitor] = None


def get_health_monitor() -> HealthMonitor:
    """Get or create the global health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = HealthMonitor()
    return _health_monitor
