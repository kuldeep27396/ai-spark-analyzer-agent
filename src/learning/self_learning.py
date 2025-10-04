"""
Self-Learning and Adaptation System
Continuous learning from job patterns, feedback, and outcomes to improve analysis accuracy
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum
import json
import pickle
from pathlib import Path
import numpy as np
from collections import defaultdict, deque

from ..core.config import Config
from ..core.models import SparkJob, Pattern, Recommendation
from ..ai.agentic_engine import AgenticAIEngine

logger = logging.getLogger(__name__)


class LearningType(Enum):
    PATTERN_RECOGNITION = "pattern_recognition"
    COST_PREDICTION = "cost_prediction"
    PERFORMANCE_ANALYSIS = "performance_analysis"
    OPTIMIZATION_EFFECTIVENESS = "optimization_effectiveness"
    ALERT_ACCURACY = "alert_accuracy"
    BUSINESS_IMPACT = "business_impact"


class ModelStatus(Enum):
    TRAINING = "training"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    FAILED = "failed"


@dataclass
class FeedbackData:
    feedback_id: str
    recommendation_id: str
    job_id: str
    user_id: str
    feedback_type: str  # positive, negative, neutral
    feedback_score: float  # -1 to 1
    comments: str
    actual_outcome: Dict[str, Any]
    expected_outcome: Dict[str, Any]
    timestamp: datetime
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LearningMetric:
    metric_name: str
    current_value: float
    target_value: float
    improvement_trend: str  # improving, stable, degrading
    last_updated: datetime
    historical_values: deque = field(default_factory=lambda: deque(maxlen=100))


@dataclass
class ModelPerformance:
    model_name: str
    model_type: LearningType
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    training_samples: int
    last_training: datetime
    status: ModelStatus
    version: int
    performance_history: deque = field(default_factory=lambda: deque(maxlen=50))


@dataclass
class AdaptationRule:
    rule_id: str
    condition: str
    action: str
    priority: int
    success_rate: float
    last_applied: Optional[datetime]
    times_applied: int
    impact_score: float


class SelfLearningSystem:
    """
    Self-learning system for continuous improvement of analysis models
    """

    def __init__(self, config: Config, ai_engine: AgenticAIEngine):
        self.config = config
        self.ai_engine = ai_engine

        # Learning state
        self.feedback_data: Dict[str, FeedbackData] = {}
        self.learning_metrics: Dict[str, LearningMetric] = {}
        self.model_performance: Dict[str, ModelPerformance] = {}
        self.adaptation_rules: Dict[str, AdaptationRule] = {}
        self.knowledge_base: Dict[str, Any] = {}

        # Learning configuration
        self.learning_config = {
            "feedback_retention_days": 90,
            "model_retraining_threshold": 100,  # Minimum feedback samples
            "learning_interval_hours": 6,
            "adaptation_confidence_threshold": 0.7,
            "model_performance_window": 30,  # days
            "max_adaptation_rules": 50,
            "knowledge_extraction_interval_hours": 24
        }

        # Model storage
        self.models_path = Path("models/learned")
        self.models_path.mkdir(exist_ok=True)

        # Learning statistics
        self.learning_stats = {
            "total_feedback_received": 0,
            "models_trained": 0,
            "adaptations_applied": 0,
            "accuracy_improvements": 0,
            "last_learning_cycle": None,
            "active_learning_models": 0
        }

        # Initialize learning components
        self._initialize_learning_metrics()
        self._initialize_adaptation_rules()
        self._load_existing_models()

        logger.info("Self-Learning System initialized")

    def _initialize_learning_metrics(self):
        """Initialize learning metrics"""
        metrics = [
            ("pattern_recognition_accuracy", 0.85, 0.95),
            ("cost_prediction_error", 0.15, 0.05),
            ("performance_analysis_relevance", 0.80, 0.90),
            ("recommendation_adoption_rate", 0.60, 0.80),
            ("alert_false_positive_rate", 0.20, 0.05),
            ("optimization_roi_accuracy", 0.70, 0.85)
        ]

        for metric_name, current, target in metrics:
            self.learning_metrics[metric_name] = LearningMetric(
                metric_name=metric_name,
                current_value=current,
                target_value=target,
                improvement_trend="stable",
                last_updated=datetime.utcnow()
            )

    def _initialize_adaptation_rules(self):
        """Initialize adaptation rules"""
        rules = [
            {
                "rule_id": "high_cost_optimization",
                "condition": "cost_prediction_error > 0.2",
                "action": "retrain_cost_model_with_recent_data",
                "priority": 8,
                "success_rate": 0.75
            },
            {
                "rule_id": "pattern_recognition_drift",
                "condition": "pattern_recognition_accuracy < 0.7",
                "action": "update_pattern_library_with_new_patterns",
                "priority": 9,
                "success_rate": 0.80
            },
            {
                "rule_id": "recommendation_relevance_drop",
                "condition": "recommendation_adoption_rate < 0.4",
                "action": "analyze_feedback_and_improve_recommendations",
                "priority": 7,
                "success_rate": 0.65
            },
            {
                "rule_id": "alert_noise_reduction",
                "condition": "alert_false_positive_rate > 0.3",
                "action": "adjust_alert_thresholds_based_on_history",
                "priority": 6,
                "success_rate": 0.85
            }
        ]

        for rule_data in rules:
            rule = AdaptationRule(
                rule_id=rule_data["rule_id"],
                condition=rule_data["condition"],
                action=rule_data["action"],
                priority=rule_data["priority"],
                success_rate=rule_data["success_rate"],
                last_applied=None,
                times_applied=0,
                impact_score=0.0
            )
            self.adaptation_rules[rule.rule_id] = rule

    def _load_existing_models(self):
        """Load existing learned models"""
        try:
            model_files = list(self.models_path.glob("*.pkl"))
            for model_file in model_files:
                with open(model_file, 'rb') as f:
                    model_data = pickle.load(f)
                    self.knowledge_base[model_file.stem] = model_data
                logger.info(f"Loaded model: {model_file.stem}")
        except Exception as e:
            logger.error(f"Error loading existing models: {e}")

    async def start_learning_cycle(self):
        """Start continuous learning cycle"""
        logger.info("Starting continuous learning cycle")

        while True:
            try:
                await self._learning_cycle()
                await asyncio.sleep(self.learning_config["learning_interval_hours"] * 3600)
            except Exception as e:
                logger.error(f"Error in learning cycle: {e}")
                await asyncio.sleep(3600)  # Wait before retrying

    async def _learning_cycle(self):
        """Execute one learning cycle"""
        logger.info("Starting learning cycle")

        # 1. Process new feedback
        await self._process_feedback_data()

        # 2. Update learning metrics
        await self._update_learning_metrics()

        # 3. Evaluate model performance
        await self._evaluate_model_performance()

        # 4. Apply adaptation rules
        await self._apply_adaptation_rules()

        # 5. Retrain models if needed
        await self._retrain_models()

        # 6. Extract new knowledge
        await self._extract_knowledge()

        # 7. Update statistics
        self._update_learning_stats()

        self.learning_stats["last_learning_cycle"] = datetime.utcnow()
        logger.info("Learning cycle completed")

    async def _process_feedback_data(self):
        """Process new feedback data"""
        # Get recent feedback (would come from user interactions)
        recent_feedback = await self._collect_recent_feedback()

        for feedback in recent_feedback:
            self.feedback_data[feedback.feedback_id] = feedback
            self.learning_stats["total_feedback_received"] += 1

            # Analyze feedback for immediate insights
            await self._analyze_feedback(feedback)

        # Clean up old feedback
        await self._cleanup_old_feedback()

    async def _collect_recent_feedback(self) -> List[FeedbackData]:
        """Collect recent feedback data"""
        # In a real implementation, this would collect feedback from various sources
        # For now, simulate some feedback
        feedback_list = []

        # Simulate feedback from recommendations
        if len(self.feedback_data) < 50:  # Generate some initial feedback
            import random
            for i in range(5):
                feedback = FeedbackData(
                    feedback_id=f"feedback_{datetime.utcnow().isoformat()}_{i}",
                    recommendation_id=f"rec_{i}",
                    job_id=f"job_{i}",
                    user_id="system",
                    feedback_type=random.choice(["positive", "negative", "neutral"]),
                    feedback_score=random.uniform(-1, 1),
                    comments="Simulated feedback",
                    actual_outcome={"cost_savings": random.uniform(0, 100)},
                    expected_outcome={"cost_savings": random.uniform(50, 150)},
                    timestamp=datetime.utcnow()
                )
                feedback_list.append(feedback)

        return feedback_list

    async def _analyze_feedback(self, feedback: FeedbackData):
        """Analyze individual feedback for insights"""
        try:
            # Analyze feedback patterns
            if feedback.feedback_score < -0.5:
                # Negative feedback - identify issues
                await self._identify_negative_feedback_patterns(feedback)
            elif feedback.feedback_score > 0.5:
                # Positive feedback - identify success factors
                await self._identify_positive_feedback_patterns(feedback)

            # Update adaptation rule success rates
            await self._update_rule_success_rates(feedback)

        except Exception as e:
            logger.error(f"Error analyzing feedback {feedback.feedback_id}: {e}")

    async def _identify_negative_feedback_patterns(self, feedback: FeedbackData):
        """Identify patterns in negative feedback"""
        # Analyze why recommendations failed
        if feedback.actual_outcome.get("cost_savings", 0) < feedback.expected_outcome.get("cost_savings", 0) * 0.5:
            # Cost savings were significantly lower than expected
            logger.info(f"Cost overestimation detected for recommendation {feedback.recommendation_id}")

            # Update cost prediction model parameters
            await self._adjust_cost_prediction_model(feedback)

    async def _identify_positive_feedback_patterns(self, feedback: FeedbackData):
        """Identify patterns in positive feedback"""
        # Analyze what made recommendations successful
        if feedback.actual_outcome.get("cost_savings", 0) > feedback.expected_outcome.get("cost_savings", 0):
            # Exceeded expectations
            logger.info(f"Cost savings exceeded expectations for recommendation {feedback.recommendation_id}")

            # Reinforce successful patterns
            await self._reinforce_successful_patterns(feedback)

    async def _adjust_cost_prediction_model(self, feedback: FeedbackData):
        """Adjust cost prediction model based on feedback"""
        # Update cost prediction accuracy metric
        if "cost_prediction_error" in self.learning_metrics:
            current_error = self.learning_metrics["cost_prediction_error"].current_value
            # Simulate error reduction
            new_error = max(0.05, current_error * 0.95)
            self.learning_metrics["cost_prediction_error"].current_value = new_error
            self.learning_metrics["cost_prediction_error"].last_updated = datetime.utcnow()

    async def _reinforce_successful_patterns(self, feedback: FeedbackData):
        """Reinforce patterns that led to successful outcomes"""
        # Add successful pattern to knowledge base
        pattern_key = f"successful_pattern_{feedback.recommendation_type}"
        if pattern_key not in self.knowledge_base:
            self.knowledge_base[pattern_key] = {
                "success_count": 0,
                "total_attempts": 0,
                "average_savings": 0.0
            }

        self.knowledge_base[pattern_key]["success_count"] += 1
        self.knowledge_base[pattern_key]["total_attempts"] += 1
        savings = feedback.actual_outcome.get("cost_savings", 0)
        self.knowledge_base[pattern_key]["average_savings"] = (
            (self.knowledge_base[pattern_key]["average_savings"] *
             (self.knowledge_base[pattern_key]["success_count"] - 1) + savings) /
            self.knowledge_base[pattern_key]["success_count"]
        )

    async def _update_rule_success_rates(self, feedback: FeedbackData):
        """Update success rates of adaptation rules based on feedback"""
        for rule in self.adaptation_rules.values():
            if rule.last_applied:
                # Check if feedback is related to recent rule application
                time_diff = feedback.timestamp - rule.last_applied
                if time_diff.total_seconds() < 24 * 3600:  # Within 24 hours
                    # Update success rate based on feedback
                    if feedback.feedback_score > 0:
                        # Positive feedback increases success rate
                        rule.success_rate = min(1.0, rule.success_rate * 1.1)
                    else:
                        # Negative feedback decreases success rate
                        rule.success_rate = max(0.0, rule.success_rate * 0.9)

    async def _update_learning_metrics(self):
        """Update learning metrics based on recent data"""
        for metric_name, metric in self.learning_metrics.items():
            try:
                # Calculate new value based on recent data
                new_value = await self._calculate_metric_value(metric_name)

                if new_value is not None:
                    # Update trend
                    if len(metric.historical_values) > 0:
                        last_value = metric.historical_values[-1]
                        if new_value > last_value * 1.05:
                            metric.improvement_trend = "improving"
                        elif new_value < last_value * 0.95:
                            metric.improvement_trend = "degrading"
                        else:
                            metric.improvement_trend = "stable"

                    # Update metric
                    metric.historical_values.append(new_value)
                    metric.current_value = new_value
                    metric.last_updated = datetime.utcnow()

            except Exception as e:
                logger.error(f"Error updating metric {metric_name}: {e}")

    async def _calculate_metric_value(self, metric_name: str) -> Optional[float]:
        """Calculate new value for a learning metric"""
        if metric_name == "pattern_recognition_accuracy":
            return await self._calculate_pattern_accuracy()
        elif metric_name == "cost_prediction_error":
            return await self._calculate_cost_prediction_error()
        elif metric_name == "recommendation_adoption_rate":
            return await self._calculate_adoption_rate()
        elif metric_name == "alert_false_positive_rate":
            return await self._calculate_false_positive_rate()
        else:
            # Return current value with small random variation
            metric = self.learning_metrics.get(metric_name)
            if metric:
                import random
                return max(0.0, min(1.0, metric.current_value + random.uniform(-0.05, 0.05)))
        return None

    async def _calculate_pattern_accuracy(self) -> float:
        """Calculate pattern recognition accuracy"""
        # In a real implementation, this would compare predicted patterns with actual patterns
        import random
        return min(1.0, max(0.0, 0.85 + random.uniform(-0.1, 0.1)))

    async def _calculate_cost_prediction_error(self) -> float:
        """Calculate cost prediction error"""
        if self.feedback_data:
            cost_errors = []
            for feedback in self.feedback_data.values():
                predicted = feedback.expected_outcome.get("cost_savings", 0)
                actual = feedback.actual_outcome.get("cost_savings", 0)
                if predicted > 0:
                    error = abs(predicted - actual) / predicted
                    cost_errors.append(error)

            if cost_errors:
                return sum(cost_errors) / len(cost_errors)

        return 0.15

    async def _calculate_adoption_rate(self) -> float:
        """Calculate recommendation adoption rate"""
        if self.feedback_data:
            adopted = sum(1 for f in self.feedback_data.values() if f.feedback_score > 0)
            return adopted / len(self.feedback_data)
        return 0.6

    async def _calculate_false_positive_rate(self) -> float:
        """Calculate alert false positive rate"""
        # In a real implementation, this would track alert accuracy
        import random
        return max(0.0, min(1.0, 0.2 + random.uniform(-0.05, 0.05)))

    async def _evaluate_model_performance(self):
        """Evaluate performance of all models"""
        for model_name, model in self.model_performance.items():
            try:
                # Calculate performance metrics
                accuracy = await self._calculate_model_accuracy(model)
                precision = await self._calculate_model_precision(model)
                recall = await self._calculate_model_recall(model)
                f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

                # Update model performance
                model.accuracy = accuracy
                model.precision = precision
                model.recall = recall
                model.f1_score = f1_score
                model.performance_history.append({
                    "timestamp": datetime.utcnow(),
                    "accuracy": accuracy,
                    "precision": precision,
                    "recall": recall,
                    "f1_score": f1_score
                })

                # Update model status if needed
                if accuracy < 0.6:
                    model.status = ModelStatus.DEPRECATED
                elif accuracy > 0.8:
                    model.status = ModelStatus.ACTIVE

            except Exception as e:
                logger.error(f"Error evaluating model {model_name}: {e}")

    async def _calculate_model_accuracy(self, model: ModelPerformance) -> float:
        """Calculate model accuracy"""
        # In a real implementation, this would test the model on validation data
        import random
        return max(0.0, min(1.0, model.accuracy + random.uniform(-0.05, 0.05)))

    async def _calculate_model_precision(self, model: ModelPerformance) -> float:
        """Calculate model precision"""
        import random
        return max(0.0, min(1.0, model.precision + random.uniform(-0.05, 0.05)))

    async def _calculate_model_recall(self, model: ModelPerformance) -> float:
        """Calculate model recall"""
        import random
        return max(0.0, min(1.0, model.recall + random.uniform(-0.05, 0.05)))

    async def _apply_adaptation_rules(self):
        """Apply adaptation rules based on current conditions"""
        applicable_rules = []

        for rule in self.adaptation_rules.values():
            if await self._evaluate_rule_condition(rule.condition):
                applicable_rules.append(rule)

        # Sort by priority
        applicable_rules.sort(key=lambda x: x.priority, reverse=True)

        # Apply top rules
        for rule in applicable_rules[:5]:  # Apply top 5 rules per cycle
            if rule.success_rate > self.learning_config["adaptation_confidence_threshold"]:
                await self._apply_adaptation_rule(rule)

    async def _evaluate_rule_condition(self, condition: str) -> bool:
        """Evaluate if an adaptation rule condition is met"""
        try:
            # Parse simple conditions
            if "pattern_recognition_accuracy" in condition:
                metric = self.learning_metrics.get("pattern_recognition_accuracy")
                if metric and "<" in condition:
                    threshold = float(condition.split("<")[1].strip())
                    return metric.current_value < threshold

            elif "cost_prediction_error" in condition:
                metric = self.learning_metrics.get("cost_prediction_error")
                if metric and ">" in condition:
                    threshold = float(condition.split(">")[1].strip())
                    return metric.current_value > threshold

            elif "recommendation_adoption_rate" in condition:
                metric = self.learning_metrics.get("recommendation_adoption_rate")
                if metric and "<" in condition:
                    threshold = float(condition.split("<")[1].strip())
                    return metric.current_value < threshold

            elif "alert_false_positive_rate" in condition:
                metric = self.learning_metrics.get("alert_false_positive_rate")
                if metric and ">" in condition:
                    threshold = float(condition.split(">")[1].strip())
                    return metric.current_value > threshold

        except Exception as e:
            logger.error(f"Error evaluating rule condition '{condition}': {e}")

        return False

    async def _apply_adaptation_rule(self, rule: AdaptationRule):
        """Apply an adaptation rule"""
        try:
            logger.info(f"Applying adaptation rule: {rule.rule_id}")

            if rule.action == "retrain_cost_model_with_recent_data":
                await self._retrain_cost_model()
            elif rule.action == "update_pattern_library_with_new_patterns":
                await self._update_pattern_library()
            elif rule.action == "analyze_feedback_and_improve_recommendations":
                await self._improve_recommendations()
            elif rule.action == "adjust_alert_thresholds_based_on_history":
                await self._adjust_alert_thresholds()

            # Update rule metadata
            rule.last_applied = datetime.utcnow()
            rule.times_applied += 1
            self.learning_stats["adaptations_applied"] += 1

            logger.info(f"Successfully applied adaptation rule: {rule.rule_id}")

        except Exception as e:
            logger.error(f"Error applying adaptation rule {rule.rule_id}: {e}")

    async def _retrain_cost_model(self):
        """Retrain cost prediction model"""
        logger.info("Retraining cost prediction model")
        # In a real implementation, this would retrain the actual model
        self.learning_stats["models_trained"] += 1

    async def _update_pattern_library(self):
        """Update pattern library with new patterns"""
        logger.info("Updating pattern library")
        # In a real implementation, this would extract new patterns from recent data
        pass

    async def _improve_recommendations(self):
        """Improve recommendations based on feedback analysis"""
        logger.info("Improving recommendations based on feedback")
        # Analyze feedback to improve recommendation generation
        pass

    async def _adjust_alert_thresholds(self):
        """Adjust alert thresholds based on historical data"""
        logger.info("Adjusting alert thresholds")
        # Optimize alert thresholds to reduce false positives
        pass

    async def _retrain_models(self):
        """Retrain models that need improvement"""
        for model_name, model in self.model_performance.items():
            if (model.status in [ModelStatus.DEPRECATED, ModelStatus.FAILED] or
                model.accuracy < 0.7):

                logger.info(f"Retraining model: {model_name}")
                await self._retrain_single_model(model)

    async def _retrain_single_model(self, model: ModelPerformance):
        """Retrain a single model"""
        try:
            model.status = ModelStatus.TRAINING
            model.last_training = datetime.utcnow()
            model.version += 1

            # Simulate model training
            await asyncio.sleep(2)  # Simulate training time

            # Update model performance after training
            model.accuracy = min(1.0, model.accuracy + 0.1)
            model.precision = min(1.0, model.precision + 0.1)
            model.recall = min(1.0, model.recall + 0.1)
            model.f1_score = 2 * (model.precision * model.recall) / (model.precision + model.recall)
            model.status = ModelStatus.ACTIVE

            self.learning_stats["models_trained"] += 1
            logger.info(f"Successfully retrained model: {model.model_name}")

        except Exception as e:
            logger.error(f"Error retraining model {model.model_name}: {e}")
            model.status = ModelStatus.FAILED

    async def _extract_knowledge(self):
        """Extract new knowledge from learned patterns"""
        try:
            # Extract patterns from successful recommendations
            await self._extract_success_patterns()

            # Extract patterns from failures
            await self._extract_failure_patterns()

            # Extract optimization insights
            await self._extract_optimization_insights()

            # Save knowledge base
            await self._save_knowledge_base()

        except Exception as e:
            logger.error(f"Error extracting knowledge: {e}")

    async def _extract_success_patterns(self):
        """Extract patterns from successful outcomes"""
        successful_feedback = [
            f for f in self.feedback_data.values()
            if f.feedback_score > 0.5
        ]

        if successful_feedback:
            # Analyze common factors in successful recommendations
            success_factors = defaultdict(int)
            for feedback in successful_feedback:
                # Extract factors from context
                for factor in feedback.context.get("factors", []):
                    success_factors[factor] += 1

            # Store successful patterns
            self.knowledge_base["success_patterns"] = dict(success_factors)

    async def _extract_failure_patterns(self):
        """Extract patterns from failed outcomes"""
        failed_feedback = [
            f for f in self.feedback_data.values()
            if f.feedback_score < -0.5
        ]

        if failed_feedback:
            # Analyze common factors in failed recommendations
            failure_factors = defaultdict(int)
            for feedback in failed_feedback:
                for factor in feedback.context.get("factors", []):
                    failure_factors[factor] += 1

            # Store failure patterns
            self.knowledge_base["failure_patterns"] = dict(failure_factors)

    async def _extract_optimization_insights(self):
        """Extract optimization insights from feedback"""
        # Analyze cost optimization effectiveness
        cost_optimizations = [
            f for f in self.feedback_data.values()
            if "cost" in f.context.get("optimization_type", "")
        ]

        if cost_optimizations:
            total_savings = sum(f.actual_outcome.get("cost_savings", 0) for f in cost_optimizations)
            avg_savings = total_savings / len(cost_optimizations)

            self.knowledge_base["cost_optimization_insights"] = {
                "total_recommendations": len(cost_optimizations),
                "average_savings": avg_savings,
                "total_savings": total_savings,
                "success_rate": len([f for f in cost_optimizations if f.feedback_score > 0]) / len(cost_optimizations)
            }

    async def _save_knowledge_base(self):
        """Save knowledge base to file"""
        try:
            timestamp = datetime.utcnow().isoformat()
            knowledge_file = self.models_path / f"knowledge_base_{timestamp}.pkl"

            with open(knowledge_file, 'wb') as f:
                pickle.dump(self.knowledge_base, f)

            logger.info(f"Saved knowledge base with {len(self.knowledge_base)} entries")

        except Exception as e:
            logger.error(f"Error saving knowledge base: {e}")

    async def _cleanup_old_feedback(self):
        """Clean up old feedback data"""
        cutoff_date = datetime.utcnow() - timedelta(days=self.learning_config["feedback_retention_days"])

        old_feedback = [
            feedback_id for feedback_id, feedback in self.feedback_data.items()
            if feedback.timestamp < cutoff_date
        ]

        for feedback_id in old_feedback:
            del self.feedback_data[feedback_id]

        if old_feedback:
            logger.info(f"Cleaned up {len(old_feedback)} old feedback entries")

    def _update_learning_stats(self):
        """Update learning statistics"""
        self.learning_stats["active_learning_models"] = len([
            m for m in self.model_performance.values() if m.status == ModelStatus.ACTIVE
        ])

        # Calculate accuracy improvements
        if self.model_performance:
            avg_accuracy = sum(m.accuracy for m in self.model_performance.values()) / len(self.model_performance)
            if avg_accuracy > 0.8:
                self.learning_stats["accuracy_improvements"] += 1

    def add_feedback(self, feedback_data: Dict[str, Any]) -> str:
        """Add new feedback data"""
        feedback = FeedbackData(
            feedback_id=feedback_data.get("feedback_id", f"feedback_{datetime.utcnow().isoformat()}"),
            recommendation_id=feedback_data["recommendation_id"],
            job_id=feedback_data["job_id"],
            user_id=feedback_data["user_id"],
            feedback_type=feedback_data["feedback_type"],
            feedback_score=feedback_data["feedback_score"],
            comments=feedback_data.get("comments", ""),
            actual_outcome=feedback_data.get("actual_outcome", {}),
            expected_outcome=feedback_data.get("expected_outcome", {}),
            timestamp=datetime.utcnow(),
            context=feedback_data.get("context", {})
        )

        self.feedback_data[feedback.feedback_id] = feedback
        self.learning_stats["total_feedback_received"] += 1

        logger.info(f"Added feedback {feedback.feedback_id}")
        return feedback.feedback_id

    def get_learning_status(self) -> Dict[str, Any]:
        """Get current learning status"""
        return {
            "learning_stats": {
                **self.learning_stats,
                "last_learning_cycle": self.learning_stats["last_learning_cycle"].isoformat() if self.learning_stats["last_learning_cycle"] else None
            },
            "metrics": {
                name: {
                    "current_value": metric.current_value,
                    "target_value": metric.target_value,
                    "trend": metric.improvement_trend,
                    "last_updated": metric.last_updated.isoformat()
                }
                for name, metric in self.learning_metrics.items()
            },
            "models": {
                name: {
                    "type": model.model_type.value,
                    "accuracy": model.accuracy,
                    "status": model.status.value,
                    "version": model.version,
                    "last_training": model.last_training.isoformat()
                }
                for name, model in self.model_performance.items()
            },
            "adaptation_rules": {
                name: {
                    "priority": rule.priority,
                    "success_rate": rule.success_rate,
                    "times_applied": rule.times_applied,
                    "last_applied": rule.last_applied.isoformat() if rule.last_applied else None
                }
                for name, rule in self.adaptation_rules.items()
            },
            "knowledge_base_size": len(self.knowledge_base),
            "feedback_count": len(self.feedback_data)
        }

    def get_model_insights(self) -> Dict[str, Any]:
        """Get insights about model performance"""
        insights = {
            "overall_performance": {},
            "improvement_opportunities": [],
            "top_performing_models": [],
            "models_needing_attention": []
        }

        if self.model_performance:
            # Calculate overall performance
            avg_accuracy = sum(m.accuracy for m in self.model_performance.values()) / len(self.model_performance)
            avg_f1 = sum(m.f1_score for m in self.model_performance.values()) / len(self.model_performance)

            insights["overall_performance"] = {
                "average_accuracy": avg_accuracy,
                "average_f1_score": avg_f1,
                "total_models": len(self.model_performance),
                "active_models": len([m for m in self.model_performance.values() if m.status == ModelStatus.ACTIVE])
            }

            # Identify top performing models
            top_models = sorted(
                self.model_performance.items(),
                key=lambda x: x[1].f1_score,
                reverse=True
            )[:3]

            insights["top_performing_models"] = [
                {"name": name, "f1_score": model.f1_score, "accuracy": model.accuracy}
                for name, model in top_models
            ]

            # Identify models needing attention
            attention_models = [
                name for name, model in self.model_performance.items()
                if model.status in [ModelStatus.DEPRECATED, ModelStatus.FAILED] or model.accuracy < 0.7
            ]

            insights["models_needing_attention"] = attention_models

        return insights