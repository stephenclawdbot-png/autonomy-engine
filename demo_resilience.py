#!/usr/bin/env python3
"""
Demo: Chaos Engineering Test for AUTONOMY-ENGINE

This demo creates 5 agents, randomly kills/crashes them, and demonstrates
the AUTONOMY-ENGINE's self-healing capabilities:
- Automatic failure detection
- Circuit breaker activation
- Auto-recovery with state restoration
- Health monitoring dashboard updates

Run: python demo_resilience.py
"""

import asyncio
import random
import time
import json
from datetime import datetime
from typing import Dict, Any, Optional

from health_monitor import HealthStatus, get_health_monitor
from failure_predictor import get_failure_predictor, FailureType
from circuit_breaker import get_circuit_registry
from recovery_orchestrator import get_recovery_orchestrator, RecoveryStrategy
from autonomy_broker import (
    AutonomyBroker, SpawnConfig, get_autonomy_broker, 
    resilient_spawn, AgentHandle
)


# Global failure simulation
demo_failures: Dict[str, Dict] = {}


class DemoAgent:
    """Simulated agent that can experience various failures."""
    
    def __init__(self, agent_id: str, agent_type: str, initial_state: Optional[Dict] = None):
        self.agent_id = agent_id
        self.agent_type = agent_type
        self.state = initial_state or {"counter": 0, "data": []}
        self.created_at = time.time()
        self.call_count = 0
        self.is_alive = True
        self.degraded = False
    
    async def process(self, task_id: str, input_data: str) -> Dict:
        """Process a task - may randomly fail."""
        if not self.is_alive:
            raise Exception(f"Agent {self.agent_id} is DEAD (simulated)")
        
        self.call_count += 1
        self.state["counter"] += 1
        self.state["data"].append({"task": task_id, "input": input_data, "time": time.time()})
        
        # Simulate processing time
        processing_time = random.uniform(0.1, 0.5)
        await asyncio.sleep(processing_time)
        
        # Check if this agent has failures configured
        if self.agent_id in demo_failures:
            failure_config = demo_failures[self.agent_id]
            
            # Random crash
            if failure_config.get("random_crash") and random.random() < failure_config.get("crash_prob", 0.2):
                self.is_alive = False
                raise Exception(f"💀 {self.agent_id} CRASHED randomly!")
            
            # Slow response
            if failure_config.get("slow_mode"):
                await asyncio.sleep(random.uniform(2, 5))
            
            # Garbage output
            if failure_config.get("garbage_output") and random.random() < 0.3:
                return {
                    "status": "error",
                    "output": "garbage@!#%!#$^%#$" * 50,
                    "agent_id": self.agent_id
                }
            
            # Repetitive output
            if failure_config.get("repetitive") and self.call_count > 3:
                return {
                    "status": "ok",
                    "output": "same same same same same",
                    "agent_id": self.agent_id
                }
        
        return {
            "status": "success",
            "result": f"Processed '{input_data}' by {self.agent_id}",
            "agent_id": self.agent_id,
            "call_count": self.call_count,
            "state_counter": self.state["counter"],
            "processing_time_ms": processing_time * 1000
        }
    
    def destroy(self):
        """Simulate agent destruction."""
        self.is_alive = False


async def agent_factory(agent_id: str, agent_type: str, restored_state: Optional[Dict] = None, degraded: bool = False):
    """Factory function to create/recover agents."""
    agent = DemoAgent(agent_id, agent_type, restored_state)
    if degraded:
        agent.degraded = True
        print(f"  ⚠️  {agent_id} restarting in DEGRADED mode")
    else:
        print(f"  ✅ {agent_id} recovered with state: {restored_state}")
    return agent


def print_banner(text: str):
    """Print a formatted banner."""
    width = 70
    print()
    print("=" * width)
    print(f"  {text}")
    print("=" * width)
    print()


def print_section(text: str):
    """Print a section header."""
    print(f"\n{'─' * 60}")
    print(f"  📌 {text}")
    print(f"{'─' * 60}\n")


async def print_dashboard(broker: AutonomyBroker):
    """Print current system health dashboard."""
    health = await broker.get_swarm_health()
    circuits = await broker.get_circuit_stats()
    recovery = await broker.get_recovery_stats()
    
    print("\n  📊 SWARM HEALTH DASHBOARD")
    print("  ┌─────────────────────────────────────────────────────────┐")
    
    # Overall status
    status_emoji = {
        "healthy": "🟢",
        "degraded": "🟡",
        "critical": "🔴",
        "empty": "⚪"
    }.get(health.get("status"), "⚪")
    
    print(f"  │ Overall Status: {status_emoji} {health.get('status', 'unknown').upper()}")
    print(f"  │ Agents: {health.get('healthy_count', 0)} healthy, "
          f"{health.get('degraded_count', 0)} degraded, "
          f"{health.get('unhealthy_count', 0)} unhealthy, "
          f"{health.get('dead_count', 0)} dead")
    print(f"  │ Total: {health.get('total_agents', 0)} agents")
    
    print("  ├─────────────────────────────────────────────────────────┤")
    print("  │ CIRCUIT BREAKERS:")
    for agent_id, stats in circuits.items():
        state_emoji = {
            "closed": "🟢", "open": "🔴", "half_open": "🟡"
        }.get(stats["state"], "⚪")
        print(f"  │   {state_emoji} {agent_id}: {stats['state'].upper()} "
              f"({stats['stats']['total_calls']} calls, "
              f"{stats['stats']['total_failures']} failures)")
    
    print("  ├─────────────────────────────────────────────────────────┤")
    print("  │ RECOVERY STATS:")
    print(f"  │   Total: {recovery.get('total_recoveries', 0)} "
          f"| Success: {recovery.get('successful_recoveries', 0)} "
          f"| Rate: {recovery.get('success_rate', 0)*100:.1f}%")
    
    print("  └─────────────────────────────────────────────────────────┘")


async def chaos_test():
    """
    Main chaos engineering test.
    
    Demonstrates AUTONOMY-ENGINE handling all failure scenarios.
    """
    print_banner("AUTONOMY-ENGINE v1.0 - Chaos Engineering Demo")
    print("  Testing self-healing agent swarm capabilities...")
    print("  Components: Health Monitor | Failure Predictor | Circuit Breaker | Recovery Orchestrator")
    
    # Initialize
    broker = get_autonomy_broker()
    
    # Register agent factory with recovery orchestrator
    recovery = get_recovery_orchestrator()
    recovery.register_agent_factory(agent_factory)
    
    await broker.start()
    
    # Create 5 agents
    print_section("Phase 1: Creating Agent Swarm (5 agents)")
    
    agents: Dict[str, Any] = {}
    agent_handles: Dict[str, AgentHandle] = {}
    
    for i in range(5):
        agent_id = f"demo-agent-{i+1}"
        agent_type = f"type-{i % 2 + 1}"  # Two types: type-1, type-2
        
        agent = DemoAgent(agent_id, agent_type, initial_state={"id": agent_id, "version": 1})
        agents[agent_id] = agent
        
        # Wrap with autonomy broker
        handle = await broker.spawn_handle(
            agent.process,
            SpawnConfig(
                agent_id=agent_id,
                agent_type=agent_type,
                enable_health_monitoring=True,
                enable_failure_prediction=True,
                enable_circuit_breaker=True,
                enable_auto_recovery=True,
                max_retries=3,
                heartbeat_interval_sec=5
            )
        )
        agent_handles[agent_id] = handle
        
        print(f"  ✅ Created {agent_id} (type: {agent_type})")
    
    await print_dashboard(broker)
    
    # Phase 2: Normal operations
    print_section("Phase 2: Normal Operations (10 tasks)")
    
    for i in range(10):
        agent_id = random.choice(list(agent_handles.keys()))
        handle = agent_handles[agent_id]
        
        try:
            result = await handle.call(f"task-{i}", f"data-{i}")
            print(f"  ✓ {agent_id}: {result['result'][:40]}...")
        except Exception as e:
            print(f"  ✗ {agent_id}: {e}")
        
        await asyncio.sleep(0.1)
    
    await print_dashboard(broker)
    
    # Phase 3: Inject failures
    print_section("Phase 3: Chaos Injection - Simulating Failures")
    
    # Configure specific failure modes
    demo_failures["demo-agent-2"] = {
        "random_crash": True,
        "crash_prob": 0.4
    }
    print("  💥 demo-agent-2: Will randomly crash (40% probability)")
    
    demo_failures["demo-agent-3"] = {
        "slow_mode": True
    }
    print("  🐌 demo-agent-3: Will become slow (2-5s delays)")
    
    demo_failures["demo-agent-4"] = {
        "garbage_output": True
    }
    print("  🗑️  demo-agent-4: Will produce garbage outputs")
    
    demo_failures["demo-agent-5"] = {
        "repetitive": True
    }
    print("  🔄 demo-agent-5: Will become repetitive after 3 calls")
    
    # Phase 4: Operations with failures
    print_section("Phase 4: Operations During Failures (20 tasks)")
    
    success_count = 0
    failure_count = 0
    auto_recovered_count = 0
    
    for i in range(20):
        agent_id = random.choice(list(agent_handles.keys()))
        handle = agent_handles[agent_id]
        
        print(f"\n  Task {i+1}/20: Sending to {agent_id}")
        
        try:
            spawn_result = await broker.spawn(
                agents[agent_id].process,
                SpawnConfig(
                    agent_id=agent_id,
                    agent_type=agents[agent_id].agent_type,
                    max_retries=2,
                    enable_auto_recovery=True
                ),
                f"chaos-task-{i}",
                f"chaos-data-{i}"
            )
            
            if spawn_result.success:
                success_count += 1
                print(f"    ✅ Success: {spawn_result.result['result'][:30]}...")
            else:
                failure_count += 1
                if spawn_result.recovery_performed:
                    auto_recovered_count += 1
                    print(f"    ♻️  Auto-recovered after failure")
                else:
                    print(f"    ❌ Failed: {spawn_result.error}")
            
            if spawn_result.retry_count > 0:
                print(f"    🔄 Required {spawn_result.retry_count} retries")
            
            if spawn_result.failure_detected:
                print(f"    ⚠️  Failure was detected by predictor")
            
        except Exception as e:
            failure_count += 1
            print(f"    💥 Exception: {e}")
        
        # Print dashboard every 5 tasks
        if (i + 1) % 5 == 0:
            await print_dashboard(broker)
        
        await asyncio.sleep(0.2)
    
    # Phase 5: Manual recovery demonstration
    print_section("Phase 5: Manual Recovery Test")
    
    # Kill an agent and show recovery
    print("  🔪 Manually killing demo-agent-1...")
    agents["demo-agent-1"].destroy()
    
    # Trigger recovery
    recovery_result = await recovery.handle_failure(
        "demo-agent-1",
        error="Manual kill for demo",
        circuit_state="open"
    )
    
    if recovery_result.success:
        print(f"  ✅ Recovery successful!")
        print(f"     Strategy: {recovery_result.strategy_used.value}")
        print(f"     Time: {recovery_result.recovery_time_ms:.1f}ms")
        print(f"     State restored: {recovery_result.checkpoint_restored}")
    else:
        print(f"  ❌ Recovery failed: {recovery_result.error}")
    
    await print_dashboard(broker)
    
    # Phase 6: Final stats
    print_section("Phase 6: Final Statistics")
    
    final_stats = await broker.get_full_dashboard()
    
    print("  📈 Chaos Test Results:")
    print(f"    Total tasks: 30 (10 normal + 20 chaos)")
    print(f"    Successful: {success_count}")
    print(f"    Failed: {failure_count}")
    print(f"    Auto-recovered: {auto_recovered_count}")
    
    print("\n  🎯 Resilience Features Demonstrated:")
    print("    ✓ Health monitoring with heartbeats")
    print("    ✓ ML-based failure prediction")
    print("    ✓ Circuit breaker pattern")
    print("    ✓ Automatic retry with exponential backoff")
    print("    ✓ State checkpoint and restoration")
    print("    ✓ Auto-recovery on failure")
    print("    ✓ Self-optimizing retry policies")
    
    # Cleanup
    await broker.stop()
    
    print_banner("Demo Complete - AUTONOMY-ENGINE v1.0 Operational")
    print("  " + final_stats.get("swarm_health", {}).get("status", "unknown"))


if __name__ == "__main__":
    try:
        asyncio.run(chaos_test())
    except KeyboardInterrupt:
        print("\n\n  ⚠️  Demo interrupted by user")
    except Exception as e:
        print(f"\n\n  💥 Demo failed: {e}")
        import traceback
        traceback.print_exc()
