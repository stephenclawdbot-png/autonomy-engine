# AUTONOMY-ENGINE v1.0

**Self-Healing Agent Swarm System**

> 🚀 The next evolution beyond AGENT-BROKER. A resilience layer that enables agent swarms to self-diagnose, self-repair, and auto-recover from failures without human intervention.

---

## Problem Statement

Current agent systems fail silently. When a subagent:
- 💀 Crashes unexpectedly
- ⏱️ Times out on long-running tasks
- 🗑️ Produces garbage or hallucinated outputs
- 🔥 Triggers cascading failures

**The orchestrator must manually detect and restart. This doesn't scale.**

---

## Solution: AUTONOMY-ENGINE

A comprehensive resilience layer providing:

| Component | Purpose |
|-----------|---------|
| **Health Monitor** | Heartbeat tracking, latency histograms, SLA monitoring |
| **Failure Predictor** | ML-based anomaly detection (Isolation Forest) on agent outputs |
| **Circuit Breaker** | Prevents cascading failures with open/closed/half-open states |
| **Recovery Orchestrator** | Automated state checkpointing and agent respawn |
| **Autonomy Broker** | Drop-in replacement for subagent spawning |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    AUTONOMY-ENGINE v1.0                              │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐       │
│  │   Health        │  │   Failure       │  │   Circuit       │       │
│  │   Monitor       │  │   Predictor     │  │   Breaker       │       │
│  │                 │  │                 │  │                 │       │
│  │ • Heartbeats    │  │ • Isolation     │  │ • Open/Closed   │       │
│  │ • Latency       │  │   Forest ML     │  │ • Half-Open     │       │
│  │ • SLA Tracking  │  │ • Rule-based    │  │ • Auto-Backoff  │       │
│  └───────┬─────────┘  └───────┬─────────┘  └───────┬─────────┘       │
│          │                    │                    │                  │
│          └────────────────────┼────────────────────┘                  │
│                               │                                       │
│                    ┌──────────┴──────────┐                              │
│                    ▼                   ▼                              │
│         ┌──────────────────┐  ┌──────────────────┐                     │
│         │   Recovery       │  │   State Store    │                     │
│         │   Orchestrator   │◄─┤   (checkpoints)  │                     │
│         │                  │  │                  │                     │
│         │ • Respawn        │  │ • Persistence    │                     │
│         │ • State Restore  │  │ • Integrity      │                     │
│         │ • Strategy       │  │ • History        │                     │
│         └──────────────────┘  └──────────────────┘                     │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐ │
│  │                    Subagent Pool (managed)                       │ │
│  │  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐       │ │
│  │  │Agent 1 │ │Agent 2 │ │Agent 3 │ │Agent 4 │ │Agent 5 │   ...  │ │
│  │  │[HEALTH]│ │[HEALTH]│ │[UNHEALTHY]│[HEALTH]│ │[HEALTH]│       │ │
│  │  └────────┘ └────────┘ └────────┘ └────────┘ └────────┘       │ │
│  └─────────────────────────────────────────────────────────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
┌──────────┐     ┌──────────────┐     ┌────────────────┐
│  Client  │────▶│ AUTONOMY     │────▶│  Subagent      │
│  Request │     │   BROKER     │     │  Execution     │
└──────────┘     └──────────────┘     └────────────────┘
                        │                       │
                        ▼                       ▼
                ┌──────────────┐         ┌──────────────┐
                │ Health       │◄────────│ Heartbeat    │
                │ Monitor      │         │ (injected)   │
                └──────────────┘         └──────────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌────────────┐ ┌────────────┐ ┌────────────┐
   │  Failure   │ │  Circuit   │ │  Recovery  │
   │ Predictor  │ │  Breaker   │ │Orchestrator│
   └────────────┘ └────────────┘ └────────────┘
```

---

## Components

### 1. Health Monitor (`health_monitor.py`)

Tracks agent health through configurable heartbeat intervals.

**Features:**
- Exponential decay histograms for latency tracking
- Configurable SLA thresholds
- Automatic status transitions (healthy → degraded → unhealthy → dead)
- Event callbacks for status changes

```python
from health_monitor import get_health_monitor, HealthStatus

monitor = get_health_monitor()
await monitor.register_agent("agent-1", "data_processor")
status = await monitor.heartbeat("agent-1", latency_ms=150.0)

summary = await monitor.get_swarm_health_summary()
# Returns: {status: "healthy", total_agents: 5, ...}
```

### 2. Failure Predictor (`failure_predictor.py`)

ML-based anomaly detection using Isolation Forest algorithm.

**Features:**
- Feature extraction from agent outputs (length patterns, structure, vocabulary)
- Rule-based heuristics for common failure patterns
- Repetitive output detection
- Training on normal (good) outputs

```python
from failure_predictor import get_failure_predictor, FailureType

predictor = get_failure_predictor()

# Train on normal outputs
predictor.train("data_processor", good_outputs)

# Predict failures
result = predictor.predict(agent_id, "data_processor", output)
if result.is_anomaly:
    print(f"Detected: {result.detected_failures}")
    print(f"Explanation: {result.explanation}")
```

### 3. Circuit Breaker (`circuit_breaker.py`)

Prevents cascading failures by temporarily stopping calls to unhealthy agents.

**States:**
- **CLOSED** → Normal operation
- **OPEN** → Failing fast (no calls pass)
- **HALF_OPEN** → Testing if recovered

```python
from circuit_breaker import circuit_breaker, CircuitOpenError

@circuit_breaker("my_agent", "processor")
async def process_data(data):
    # If this fails 5 times, circuit opens
    return await risky_operation(data)

# Or explicit usage
breaker = get_circuit_registry().get_circuit_breaker("agent-1")
result = await breaker.call(risky_function)
```

### 4. Recovery Orchestrator (`recovery_orchestrator.py`)

Automates agent recovery with state persistence.

**Features:**
- Automatic checkpointing
- State restoration on respawn
- Pluggable recovery strategies
- Persistence to disk or memory

```python
from recovery_orchestrator import get_recovery_orchestrator, RecoveryStrategy

recovery = get_recovery_orchestrator()
recovery.register_agent_factory(my_agent_factory)

# Save state
await recovery.save_state(agent_id, {"progress": 50})

# Trigger recovery on failure
result = await recovery.handle_failure(agent_id, error="crashed")
# Returns: RecoveryResult with new_agent_id, checkpoint_restored, etc.
```

### 5. Autonomy Broker (`autonomy_broker.py`)

Drop-in replacement for subagent spawning with automatic resilience.

```python
from autonomy_broker import resilient_spawn, SpawnConfig, get_autonomy_broker

# Simple usage
result = await resilient_spawn(
    my_agent_function,
    input_data,
    agent_type="data_processor",
    max_retries=3
)

# Advanced usage with full control
broker = get_autonomy_broker()
await broker.start()

config = SpawnConfig(
    agent_type="processor",
    enable_health_monitoring=True,
    enable_failure_prediction=True,
    enable_circuit_breaker=True,
    enable_auto_recovery=True
)

result = await broker.spawn(agent_func, config, *args)
# Returns: SpawnResult with success, retry_count, health_status, etc.
```

### 6. Trader Analysis System (`trader_profiler.py`, `copy_signal_engine.py`)

On-chain behavioral analysis of a Solana trader wallet plus a real-time
copy-signal engine with a risk pipeline. Full study and strategy rules in
[TRADER_ANALYSIS.md](TRADER_ANALYSIS.md).

```python
from trader_profiler import get_trader_profiler

# Profile any wallet's trading behavior from public RPC
profile = get_trader_profiler().profile(
    "6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS",
    max_transactions=200,
)
print(profile.summarize())
# {win_rate: 0.35, median_buy_usd: 440, median_hold_seconds: 354, ...}
```

```bash
# Watch the wallet live and emit copy signals (paper mode)
python copy_signal_engine.py 6SHqkzJfZYiNqmz4xDiwndEAqbubuAVt44LwJ9GF3obS
```

Identity enrichment via the FomoScan API (`fomoscan_client.py`,
reference in [FOMOSCAN_API.md](FOMOSCAN_API.md)): resolve a profiled
wallet to the fomo.family trader behind it and follow their thesis
posts per token, with compute-unit cost tracking built in.

---

## Quick Start

### Installation

```bash
# Clone or copy the autonomy-engine/ directory
cd autonomy-engine

# No external dependencies required - pure Python standard library
# (Optional) Install for development
pip install -e .
```

### Demo: Chaos Engineering Test

```bash
python demo_resilience.py
```

This creates 5 agents and demonstrates:
- ✅ Normal operations
- 💀 Random agent crashes
- 🐌 Slow response times
- 🗑️ Garbage output detection
- 🔄 Repetitive output detection
- ♻️ Automatic recovery with state restoration
- 📊 Real-time health dashboard

Expected output:
```
======================================================================
  AUTONOMY-ENGINE v1.0 - Chaos Engineering Demo
======================================================================

Chaos Test Results:
  Total tasks: 30 (10 normal + 20 chaos)
  Successful: 26
  Failed: 4
  Auto-recovered: 4

Resilience Features Demonstrated:
  ✓ Health monitoring with heartbeats
  ✓ ML-based failure prediction
  ✓ Circuit breaker pattern
  ✓ Automatic retry with exponential backoff
  ✓ State checkpoint and restoration
  ✓ Auto-recovery on failure
  ✓ Self-optimizing retry policies
```

---

## Configuration Options

### Health Monitor

```python
HealthMonitor(
    heartbeat_interval_sec=30.0,   # How often to expect heartbeats
    heartbeat_timeout_sec=90.0,   # When to mark as unhealthy
    degraded_threshold_ms=5000.0, # Latency threshold for degraded
    unhealthy_threshold_ms=30000.0,# Latency threshold for unhealthy
    sla_target_ms=2000.0            # Target response time
)
```

### Circuit Breaker

```python
CircuitBreakerConfig(
    failure_threshold=5,         # Failures before opening
    success_threshold=3,         # Successes to close
    timeout_seconds=60.0,        # Time before half-open
    half_open_max_calls=5,       # Tests in half-open
    exponential_backoff_base=2.0 # Backoff multiplier
)
```

### Autonomy Broker

```python
SpawnConfig(
    agent_type="default",
    max_retries=3,
    retry_delay_base=1.0,
    retry_delay_max=60.0,
    heartbeat_interval_sec=30.0,
    checkpoint_interval_sec=60.0,
    enable_health_monitoring=True,
    enable_failure_prediction=True,
    enable_circuit_breaker=True,
    enable_auto_recovery=True
)
```

---

## API Reference

### Health Monitor API

| Method | Description |
|--------|-------------|
| `register_agent(id, type)` | Register agent for monitoring |
| `heartbeat(id, latency_ms)` | Process heartbeat |
| `get_swarm_health_summary()` | Get aggregate health |
| `on(event, callback)` | Register event callback |

### Failure Predictor API

| Method | Description |
|--------|-------------|
| `train(type, outputs)` | Train on normal outputs |
| `predict(id, type, output)` | Detect anomalies |
| `should_retry(result)` | Determine if retry advised |
| `get_agent_failure_rate(id)` | Get recent failure rate |

### Circuit Breaker API

| Method | Description |
|--------|-------------|
| `call(func, *args, **kwargs)` | Execute through breaker |
| `get_state()` | Get current state |
| `reset()` | Manual reset |
| `get_stats()` | Get statistics |

### Recovery Orchestrator API

| Method | Description |
|--------|-------------|
| `register_agent_factory(factory)` | Set agent factory |
| `save_state(id, state)` | Save checkpoint |
| `handle_failure(id, error)` | Initiate recovery |
| `get_recovery_stats()` | Get statistics |

---

## Why This Is Revolutionary

No existing open-source system provides:

1. **ML-based failure detection** specifically for LLM agent outputs
2. **Automatic state restoration** after agent crashes
3. **Circuit breaking** integrated with agent lifecycle
4. **Self-optimizing retry policies** that learn per agent type
5. **Real-time health monitoring** with configurable SLA thresholds
6. **All of the above** in a single, cohesive system

Most systems rely on:
- Simple retry logic with fixed delays
- Manual monitoring dashboards
- Log analysis after failures
- State loss on restarts

**AUTONOMY-ENGINE eliminates manual intervention.**

---

## Production Considerations

### Scaling

- Store checkpoints on distributed storage (S3, Redis)
- Use external metrics systems (Prometheus, Datadog)
- Horizontally scale the autonomy broker

### Security

- Encrypt checkpoints at rest
- Validate checksums before state restoration
- Sanitize agent outputs before failure prediction

### Reliability

- The autonomy broker itself should be monitored
- Consider running multiple instances with leader election
- Implement graceful degradation when broker is unavailable

---

## Contributing

This is a reference implementation. Enhancements welcome:

- [ ] Distributed checkpoint storage backends
- [ ] Additional ML models for failure prediction
- [ ] Web dashboard for real-time visualization
- [ ] Prometheus metrics exporter
- [ ] Kubernetes operator for automatic agent management

---

## License

MIT License - See LICENSE file

---

## Authors

**AUTONOMY-ENGINE v1.0** was created to solve the silent failure problem in agent swarms.

> "Systems should heal themselves. Humans should focus on higher-order decisions."

---

**Status**: ✅ Operational | **Version**: 1.0.0 | **Date**: 2026-03-28
