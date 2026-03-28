"""
Failure Predictor - ML-based anomaly detection for agent outputs.

Uses Isolation Forest for unsupervised anomaly detection on agent outputs,
combined with rule-based heuristics for common failure patterns like
empty responses, repetitive outputs, and timeout indicators.
"""

import json
import hashlib
import numpy as np
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from datetime import datetime
from collections import deque
import logging
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FailurePredictor")


class FailureType(Enum):
    """Types of failures that can be predicted."""
    ANOMALY = "anomaly"
    EMPTY_OUTPUT = "empty_output"
    REPETITIVE = "repetitive"
    GARBAGE = "garbage"
    TIMEOUT = "timeout"
    EXCEPTION = "exception"
    QUALITY_DEGRADATION = "quality_degradation"


@dataclass
class OutputFeatures:
    """Features extracted from agent output for ML analysis."""
    length: int
    word_count: int
    avg_word_length: float
    sentence_count: int
    punctuation_ratio: float
    uppercase_ratio: float
    digit_ratio: float
    unique_word_ratio: float
    repetitive_score: float
    has_structure: bool  # Has proper formatting/structure
    special_char_ratio: float
    
    def to_vector(self) -> np.ndarray:
        """Convert features to numpy vector for ML."""
        return np.array([
            np.log1p(self.length),  # Log transform for scale invariance
            np.log1p(self.word_count),
            self.avg_word_length,
            np.log1p(self.sentence_count),
            self.punctuation_ratio,
            self.uppercase_ratio,
            self.digit_ratio,
            self.unique_word_ratio,
            self.repetitive_score,
            float(self.has_structure),
            self.special_char_ratio
        ])


@dataclass
class PredictionResult:
    """Result of failure prediction."""
    is_anomaly: bool
    anomaly_score: float
    confidence: float
    detected_failures: List[FailureType]
    features: OutputFeatures
    explanation: str
    timestamp: datetime = field(default_factory=datetime.now)


class SimpleIsolationForest:
    """
    Simplified Isolation Forest implementation.
    
    Detects anomalies by measuring how quickly random splits isolate samples.
    Anomalies are easier to isolate (shorter path length).
    """
    
    def __init__(self, n_trees: int = 50, sample_size: int = 256, max_depth: int = 10):
        self.n_trees = n_trees
        self.sample_size = sample_size
        self.max_depth = max_depth
        self.trees: List[Dict] = []
        self.baseline_data: List[np.ndarray] = []
        self.is_fitted = False
    
    def fit(self, X: List[np.ndarray]):
        """Fit the model on baseline (normal) data."""
        self.baseline_data = X
        
        # Build random trees
        self.trees = []
        for _ in range(self.n_trees):
            # Sample subset of data
            if len(X) > self.sample_size:
                indices = np.random.choice(len(X), self.sample_size, replace=False)
                sample = [X[i] for i in indices]
            else:
                sample = X
            
            tree = self._build_tree(sample, depth=0)
            self.trees.append(tree)
        
        # Calculate baseline scores for normalization
        self.baseline_scores = [self._score_tree(x, tree) for tree in self.trees for x in sample[:10]]
        self.is_fitted = True
    
    def _build_tree(self, data: List[np.ndarray], depth: int) -> Dict:
        """Build a random tree node."""
        if len(data) <= 1 or depth >= self.max_depth:
            return {"is_leaf": True, "size": len(data)}
        
        # Random feature and split
        n_features = len(data[0])
        feature_idx = np.random.randint(n_features)
        feature_values = [x[feature_idx] for x in data]
        split_value = np.random.uniform(min(feature_values), max(feature_values))
        
        left_data = [x for x in data if x[feature_idx] < split_value]
        right_data = [x for x in data if x[feature_idx] >= split_value]
        
        return {
            "is_leaf": False,
            "feature_idx": feature_idx,
            "split_value": split_value,
            "left": self._build_tree(left_data, depth + 1),
            "right": self._build_tree(right_data, depth + 1)
        }
    
    def _score_tree(self, x: np.ndarray, tree: Dict) -> int:
        """Calculate path length for a sample."""
        if tree["is_leaf"]:
            return 0
        
        if x[tree["feature_idx"]] < tree["split_value"]:
            return 1 + self._score_tree(x, tree["left"])
        else:
            return 1 + self._score_tree(x, tree["right"])
    
    def anomaly_score(self, x: np.ndarray) -> float:
        """Calculate anomaly score (0-1, higher = more anomalous)."""
        if not self.is_fitted:
            return 0.5  # Default when not trained
        
        # Average path length across trees
        depths = [self._score_tree(x, tree) for tree in self.trees]
        avg_depth = np.mean(depths)
        
        # Normalize (shorter depth = more anomaly)
        max_depth = self.max_depth
        normalized = 1.0 - (avg_depth / max_depth)
        
        return max(0.0, min(1.0, normalized))


class FailurePredictor:
    """
    ML-based failure prediction for agent outputs.
    
    Combines:
    1. Isolation Forest for anomaly detection
    2. Rule-based heuristics for common failure patterns
    3. Historical pattern matching
    4. Quality scoring
    """
    
    def __init__(
        self,
        anomaly_threshold: float = 0.7,
        min_training_samples: int = 10,
        history_size: int = 100,
        max_history_per_agent: int = 1000
    ):
        self.anomaly_threshold = anomaly_threshold
        self.min_training_samples = min_training_samples
        self.history_size = history_size
        
        # ML model per agent type
        self.models: Dict[str, SimpleIsolationForest] = {}
        
        # Output history per agent
        self.agent_history: Dict[str, deque] = {}
        self.agent_hashes: Dict[str, deque] = {}
        
        # Training data per agent type
        self.training_data: Dict[str, List[np.ndarray]] = {}
        
        self.max_history_per_agent = max_history_per_agent
    
    def _extract_features(self, output: Any) -> OutputFeatures:
        """Extract features from agent output."""
        # Convert to string
        if isinstance(output, dict):
            text = json.dumps(output)
        elif isinstance(output, list):
            text = " ".join(str(x) for x in output)
        else:
            text = str(output)
        
        # Basic metrics
        length = len(text)
        words = text.split() if text else []
        word_count = len(words)
        avg_word_length = np.mean([len(w) for w in words]) if words else 0.0
        
        # Sentence detection
        sentences = re.split(r'[.!?]+', text)
        sentence_count = len([s for s in sentences if s.strip()])
        
        # Character distributions
        if length > 0:
            punctuation_ratio = len(re.findall(r'[^\w\s]', text)) / length
            uppercase_ratio = len(re.findall(r'[A-Z]', text)) / length
            digit_ratio = len(re.findall(r'\d', text)) / length
            special_char_ratio = len(re.findall(r'[^\w\s\d\.,!?;:\-\'"()]', text)) / length
        else:
            punctuation_ratio = uppercase_ratio = digit_ratio = special_char_ratio = 0.0
        
        # Vocabulary diversity
        unique_words = set(w.lower() for w in words)
        unique_word_ratio = len(unique_words) / word_count if word_count > 0 else 0.0
        
        # Repetitive score (how similar to historical outputs)
        repetitive_score = 0.0
        
        # Structure detection
        has_structure = bool(
            re.search(r'\n\n', text) or  # Paragraphs
            re.search(r'^\s*[-*]\s', text, re.MULTILINE) or  # Lists
            re.search(r'^\s*\d+\.\s', text, re.MULTILINE) or  # Numbered lists
            len(text.split('\n')) > 3  # Multiple lines
        )
        
        return OutputFeatures(
            length=length,
            word_count=word_count,
            avg_word_length=avg_word_length,
            sentence_count=sentence_count,
            punctuation_ratio=punctuation_ratio,
            uppercase_ratio=uppercase_ratio,
            digit_ratio=digit_ratio,
            unique_word_ratio=unique_word_ratio,
            repetitive_score=repetitive_score,
            has_structure=has_structure,
            special_char_ratio=special_char_ratio
        )
    
    def _check_repetitive(self, agent_id: str, features: OutputFeatures, output_hash: str) -> float:
        """Check if output is repetitive compared to history."""
        if agent_id not in self.agent_hashes:
            self.agent_hashes[agent_id] = deque(maxlen=self.history_size)
        
        history = self.agent_hashes[agent_id]
        
        if len(history) == 0:
            return 0.0
        
        # Check for exact or near-exact duplicates
        duplicate_count = sum(1 for h in history if h == output_hash)
        
        # Also check feature similarity
        recent_hashes = list(history)[-10:]
        similarity_scores = []
        for h in recent_hashes:
            # Simple string similarity via hash comparison (placeholder for more complex logic)
            similarity_scores.append(1.0 if h == output_hash else 0.0)
        
        if len(similarity_scores) > 0:
            avg_similarity = sum(similarity_scores) / len(similarity_scores)
        else:
            avg_similarity = 0.0
        
        return min(1.0, (duplicate_count * 0.2) + (avg_similarity * 0.5))
    
    def _rule_based_checks(self, output: Any, features: OutputFeatures) -> List[Tuple[FailureType, float, str]]:
        """Apply rule-based failure detection heuristics."""
        findings = []
        
        text = str(output) if not isinstance(output, str) else output
        
        # Check for empty/very short outputs
        if features.length < 10 or features.word_count < 2:
            findings.append((FailureType.EMPTY_OUTPUT, 0.9, "Output is nearly empty"))
        
        # Check for garbage (high special char ratio)
        if features.special_char_ratio > 0.3:
            findings.append((FailureType.GARBAGE, 0.8, "High special character ratio suggests garbage"))
        
        # Check for timeout indicators
        timeout_patterns = [
            r'timeout', r'timed out', r'deadline exceeded',
            r'operation took too long', r'request expired'
        ]
        for pattern in timeout_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                findings.append((FailureType.TIMEOUT, 0.95, f"Timeout pattern detected: {pattern}"))
                break
        
        # Check for exception indicators
        exception_patterns = [
            r'exception', r'error occurred', r'failed to',
            r'traceback', r'stack trace', r'runtime error'
        ]
        for pattern in exception_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                findings.append((FailureType.EXCEPTION, 0.85, f"Exception pattern detected: {pattern}"))
                break
        
        # Check for quality degradation (low structure)
        if not features.has_structure and features.length > 200:
            findings.append((FailureType.QUALITY_DEGRADATION, 0.6, "Long output lacks structure"))
        
        # Check for very low vocabulary diversity
        if features.unique_word_ratio < 0.1 and features.word_count > 50:
            findings.append((FailureType.REPETITIVE, 0.75, "Low vocabulary diversity"))
        
        return findings
    
    def train(self, agent_type: str, normal_outputs: List[Any]):
        """Train the model on normal (good) outputs for an agent type."""
        features_list = [self._extract_features(out) for out in normal_outputs]
        vectors = [f.to_vector() for f in features_list]
        
        self.training_data[agent_type] = vectors
        
        model = SimpleIsolationForest(n_trees=30, sample_size=min(len(vectors), 100))
        model.fit(vectors)
        self.models[agent_type] = model
        
        logger.info(f"Trained failure predictor for agent type '{agent_type}' with {len(vectors)} samples")
    
    def predict(self, agent_id: str, agent_type: str, output: Any) -> PredictionResult:
        """
        Predict if an output indicates imminent failure.
        
        Returns a PredictionResult with anomaly score and detected issues.
        """
        # Extract features
        features = self._extract_features(output)
        feature_vector = features.to_vector()
        
        # Calculate output hash for repetition detection
        output_str = json.dumps(output) if isinstance(output, (dict, list)) else str(output)
        output_hash = hashlib.md5(output_str.encode()).hexdigest()
        
        # Check repetition
        features.repetitive_score = self._check_repetitive(agent_id, features, output_hash)
        
        # Rule-based checks
        rule_findings = self._rule_based_checks(output, features)
        
        # ML anomaly detection (if model exists)
        ml_anomaly_score = 0.0
        if agent_type in self.models:
            ml_anomaly_score = self.models[agent_type].anomaly_score(feature_vector)
        
        # Combine scores
        combined_score = ml_anomaly_score
        detected_failures = []
        
        for failure_type, confidence, explanation in rule_findings:
            combined_score = max(combined_score, confidence * 0.5)  # Boost with rule findings
            detected_failures.append(failure_type)
        
        # Repetitive outputs are anomalies
        if features.repetitive_score > 0.8:
            combined_score = max(combined_score, 0.8)
            if FailureType.REPETITIVE not in detected_failures:
                detected_failures.append(FailureType.REPETITIVE)
        
        is_anomaly = combined_score > self.anomaly_threshold or len(detected_failures) > 0
        
        # Build explanation
        explanations = [f"{f[0].value}: {f[2]}" for f in rule_findings]
        if features.repetitive_score > 0.7:
            explanations.append(f"Highly repetitive output (score: {features.repetitive_score:.2f})")
        if ml_anomaly_score > 0.5:
            explanations.append(f"ML anomaly score: {ml_anomaly_score:.2f}")
        
        explanation_text = "; ".join(explanations) if explanations else "No issues detected"
        
        # Store in history
        if agent_id not in self.agent_history:
            self.agent_history[agent_id] = deque(maxlen=self.max_history_per_agent)
            self.agent_hashes[agent_id] = deque(maxlen=self.history_size)
        
        self.agent_history[agent_id].append({
            "features": features,
            "result": is_anomaly,
            "timestamp": datetime.now().isoformat()
        })
        self.agent_hashes[agent_id].append(output_hash)
        
        return PredictionResult(
            is_anomaly=is_anomaly,
            anomaly_score=combined_score,
            confidence=min(1.0, 0.5 + len(detected_failures) * 0.2 + ml_anomaly_score * 0.3),
            detected_failures=detected_failures,
            features=features,
            explanation=explanation_text
        )
    
    def get_agent_failure_rate(self, agent_id: str, window: int = 100) -> float:
        """Calculate recent failure rate for an agent."""
        if agent_id not in self.agent_history:
            return 0.0
        
        history = list(self.agent_history[agent_id])[-window:]
        if not history:
            return 0.0
        
        failures = sum(1 for h in history if h["result"])
        return failures / len(history)
    
    def should_retry(self, prediction: PredictionResult) -> Tuple[bool, str]:
        """
        Determine if a failed output should be retried.
        
        Returns (should_retry, reason)
        """
        if not prediction.is_anomaly:
            return False, "No failure detected"
        
        # Don't retry certain failure types
        if FailureType.GARBAGE in prediction.detected_failures and prediction.confidence > 0.9:
            return False, "High-confidence garbage output, retry unlikely to help"
        
        if FailureType.TIMEOUT in prediction.detected_failures:
            return True, "Timeout error, retry with longer timeout may succeed"
        
        if FailureType.EXCEPTION in prediction.detected_failures:
            return True, "Exception occurred, retry may succeed"
        
        if FailureType.REPETITIVE in prediction.detected_failures:
            return False, "Repetitive output, retry likely to produce same result"
        
        return True, f"Anomaly detected with confidence {prediction.confidence:.2f}, retry recommended"


# Singleton instance
_failure_predictor: Optional[FailurePredictor] = None


def get_failure_predictor() -> FailurePredictor:
    """Get or create the global failure predictor instance."""
    global _failure_predictor
    if _failure_predictor is None:
        _failure_predictor = FailurePredictor()
    return _failure_predictor
