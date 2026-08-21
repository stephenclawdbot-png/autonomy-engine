"""
AUTONOMY-ENGINE v1.0 - Self-Healing Agent Swarm System

A revolutionary meta-system that enables agent swarms to self-diagnose,
self-repair, and auto-recover from failures without human intervention.

Components:
- health_monitor: Heartbeat tracking and SLA monitoring
- failure_predictor: ML-based anomaly detection
- circuit_breaker: Cascading failure prevention
- recovery_orchestrator: State checkpointing and automatic recovery
- autonomy_broker: Drop-in replacement for subagent spawning
"""

__version__ = "1.0.0"
__author__ = "AUTONOMY-ENGINE Team"

from .health_monitor import (
    HealthMonitor,
    HealthStatus,
    AgentHealthRecord,
    LatencyHistogram,
    get_health_monitor
)

from .failure_predictor import (
    FailurePredictor,
    FailureType,
    PredictionResult,
    OutputFeatures,
    get_failure_predictor
)

from .circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerStats,
    CircuitBreakerRegistry,
    CircuitState,
    CircuitOpenError,
    circuit_breaker,
    get_circuit_registry
)

from .recovery_orchestrator import (
    RecoveryOrchestrator,
    RecoveryStrategy,
    RecoveryResult,
    RecoveryContext,
    Checkpoint,
    StateStore,
    get_recovery_orchestrator
)

from .trader_profiler import (
    TraderProfiler,
    TraderProfile,
    Trade,
    TradeSide,
    RoundTrip,
    SolanaRpcClient,
    get_trader_profiler
)

from .copy_signal_engine import (
    WalletWatcher,
    SignalConfig,
    Signal,
    SignalAction,
    SignalVerdict,
    RiskManager,
    PaperBook,
    PaperPosition
)

from .backtester import (
    Backtester,
    BacktestResult,
    BacktestPosition
)

from .wallet_stream import (
    WebSocketClient,
    StreamingWalletWatcher
)

from .insider_alpha import (
    InsiderAlphaAnalyzer,
    InsiderReport,
    SignalScore,
    EvmActivitySource,
    EvmEvent,
    NullEvmSource
)

from .autonomy_broker import (
    AutonomyBroker,
    SpawnConfig,
    SpawnResult,
    AgentHandle,
    RetryPolicy,
    resilient_spawn,
    get_autonomy_broker
)

__all__ = [
    # Core classes
    "HealthMonitor",
    "HealthStatus",
    "FailurePredictor",
    "FailureType",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitState",
    "CircuitOpenError",
    "RecoveryOrchestrator",
    "RecoveryStrategy",
    "AutonomyBroker",
    "SpawnConfig",
    "SpawnResult",
    "AgentHandle",
    "RetryPolicy",
    
    # Data classes
    "AgentHealthRecord",
    "LatencyHistogram",
    "PredictionResult",
    "OutputFeatures",
    "CircuitBreakerStats",
    "RecoveryResult",
    "RecoveryContext",
    "Checkpoint",
    "StateStore",
    
    # Convenience functions
    "get_health_monitor",
    "get_failure_predictor",
    "get_circuit_registry",
    "get_recovery_orchestrator",
    "get_autonomy_broker",
    "resilient_spawn",
    "circuit_breaker",

    # Trader analysis system
    "TraderProfiler",
    "TraderProfile",
    "Trade",
    "TradeSide",
    "RoundTrip",
    "SolanaRpcClient",
    "get_trader_profiler",
    "WalletWatcher",
    "SignalConfig",
    "Signal",
    "SignalAction",
    "SignalVerdict",
    "RiskManager",
    "PaperBook",
    "PaperPosition",
    "Backtester",
    "BacktestResult",
    "BacktestPosition",
    "WebSocketClient",
    "StreamingWalletWatcher",
    "InsiderAlphaAnalyzer",
    "InsiderReport",
    "SignalScore",
    "EvmActivitySource",
    "EvmEvent",
    "NullEvmSource",
]
