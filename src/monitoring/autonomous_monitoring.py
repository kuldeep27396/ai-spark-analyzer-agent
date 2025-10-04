"""
Autonomous Monitoring and Alerting System
Real-time monitoring with intelligent alerting for Spark job health and performance
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict, deque

from ..core.config import Config
from ..core.models import SparkJob
from ..ai.agentic_engine import AgenticAIEngine

logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    INFO = "info"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AlertType(Enum):
    PERFORMANCE_DEGRADATION = "performance_degradation"
    COST_OVERRUN = "cost_overrun"
    RESOURCE_ANOMALY = "resource_anomaly"
    JOB_FAILURE = "job_failure"
    SLA_VIOLATION = "sla_violation"
    PREDICTIVE_FAILURE = "predictive_failure"
    EFFICIENCY_DROP = "efficiency_drop"


class HealthStatus(Enum):
    HEALTHY = "healthy"
    WARNING = "warning"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class MetricThreshold:
    metric_name: str
    warning_threshold: float
    critical_threshold: float
    operator: str = "greater_than"  # greater_than, less_than, equals
    window_minutes: int = 5
    consecutive_violations: int = 2


@dataclass
class Alert:
    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    job_id: str
    cluster_name: str
    title: str
    description: str
    metric_value: float
    threshold_value: float
    timestamp: datetime
    status: str = "active"  # active, acknowledged, resolved
    acknowledged_by: Optional[str] = None
    resolution_time: Optional[datetime] = None
    recommended_actions: List[str] = field(default_factory=list)
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthScore:
    job_id: str
    overall_score: float
    performance_score: float
    cost_score: float
    resource_score: float
    reliability_score: float
    status: HealthStatus
    last_updated: datetime
    trends: Dict[str, str] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)


@dataclass
class MonitoringBaseline:
    job_id: str
    metric_name: str
    baseline_value: float
    variance_allowed: float
    created_at: datetime
    sample_size: int
    confidence_level: float


class AutonomousMonitoringSystem:
    """
    Autonomous monitoring system with intelligent alerting
    """

    def __init__(self, config: Config, ai_engine: AgenticAIEngine):
        self.config = config
        self.ai_engine = ai_engine

        # Monitoring state
        self.active_jobs: Dict[str, SparkJob] = {}
        self.alerts: Dict[str, Alert] = {}
        self.health_scores: Dict[str, HealthScore] = {}
        self.baselines: Dict[str, MonitoringBaseline] = {}
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))

        # Alert configuration
        self.thresholds = self._initialize_default_thresholds()
        self.alert_rules = self._initialize_alert_rules()
        self.suppression_rules = self._initialize_suppression_rules()

        # Monitoring configuration
        self.monitoring_config = {
            "check_interval_seconds": 30,
            "history_retention_hours": 168,  # 7 days
            "alert_cooldown_minutes": 15,
            "health_score_window_minutes": 60,
            "baseline_learning_days": 14,
            "prediction_window_hours": 24,
            "max_alerts_per_job": 10
        }

        # Statistics
        self.monitoring_stats = {
            "total_checks": 0,
            "alerts_generated": 0,
            "alerts_resolved": 0,
            "predictions_made": 0,
            "accuracy_score": 0.0,
            "last_check": None
        }

        logger.info("Autonomous Monitoring System initialized")

    def _initialize_default_thresholds(self) -> Dict[str, MetricThreshold]:
        """Initialize default metric thresholds"""
        return {
            "job_duration": MetricThreshold(
                metric_name="job_duration",
                warning_threshold=1.5,  # 150% of baseline
                critical_threshold=2.0,  # 200% of baseline
                operator="greater_than",
                window_minutes=10
            ),
            "cpu_utilization": MetricThreshold(
                metric_name="cpu_utilization",
                warning_threshold=80.0,
                critical_threshold=95.0,
                operator="greater_than",
                window_minutes=5,
                consecutive_violations=3
            ),
            "memory_utilization": MetricThreshold(
                metric_name="memory_utilization",
                warning_threshold=85.0,
                critical_threshold=98.0,
                operator="greater_than",
                window_minutes=5,
                consecutive_violations=3
            ),
            "job_failure_rate": MetricThreshold(
                metric_name="job_failure_rate",
                warning_threshold=5.0,
                critical_threshold=15.0,
                operator="greater_than",
                window_minutes=60
            ),
            "cost_per_hour": MetricThreshold(
                metric_name="cost_per_hour",
                warning_threshold=1.5,
                critical_threshold=2.0,
                operator="greater_than",
                window_minutes=30
            ),
            "task_failure_rate": MetricThreshold(
                metric_name="task_failure_rate",
                warning_threshold=3.0,
                critical_threshold=10.0,
                operator="greater_than",
                window_minutes=15
            ),
            "data_io_efficiency": MetricThreshold(
                metric_name="data_io_efficiency",
                warning_threshold=0.7,
                critical_threshold=0.5,
                operator="less_than",
                window_minutes=20
            )
        }

    def _initialize_alert_rules(self) -> Dict[str, Any]:
        """Initialize alert rules"""
        return {
            "correlation_rules": [
                {
                    "name": "high_cost_with_slow_performance",
                    "conditions": [
                        {"metric": "cost_per_hour", "operator": ">", "value": 1.2},
                        {"metric": "job_duration", "operator": ">", "value": 1.3}
                    ],
                    "severity": "high",
                    "description": "Job is expensive and underperforming"
                }
            ],
            "escalation_rules": [
                {
                    "condition": "alert_duration_minutes > 60",
                    "action": "escalate_severity",
                    "new_severity": "high"
                }
            ],
            "auto_resolution_rules": [
                {
                    "condition": "metric_normal_duration_minutes > 30",
                    "action": "auto_resolve"
                }
            ]
        }

    def _initialize_suppression_rules(self) -> List[Dict[str, Any]]:
        """Initialize alert suppression rules"""
        return [
            {
                "name": "maintenance_window",
                "condition": "maintenance_mode = true",
                "suppressed_alerts": ["performance_degradation", "resource_anomaly"],
                "duration_minutes": 240
            },
            {
                "name": "known_issue",
                "condition": "known_issue_id exists",
                "suppressed_alerts": ["job_failure"],
                "duration_minutes": 60
            }
        ]

    async def start_monitoring(self):
        """Start continuous monitoring"""
        logger.info("Starting autonomous monitoring")

        while True:
            try:
                await self._monitoring_cycle()
                await asyncio.sleep(self.monitoring_config["check_interval_seconds"])
            except Exception as e:
                logger.error(f"Error in monitoring cycle: {e}")
                await asyncio.sleep(60)

    async def _monitoring_cycle(self):
        """Execute one monitoring cycle"""
        self.monitoring_stats["total_checks"] += 1

        # 1. Collect metrics
        await self._collect_metrics()

        # 2. Check thresholds and generate alerts
        await self._check_thresholds()

        # 3. Update health scores
        await self._update_health_scores()

        # 4. Run predictive analysis
        await self._run_predictive_analysis()

        # 5. Process alert correlations and escalations
        await self._process_alert_rules()

        # 6. Update baselines
        await self._update_baselines()

        # 7. Clean up old data
        await self._cleanup_old_data()

        self.monitoring_stats["last_check"] = datetime.utcnow()

    async def _collect_metrics(self):
        """Collect metrics from all active jobs"""
        for job_id, job in self.active_jobs.items():
            try:
                # In a real implementation, this would collect actual metrics
                # For now, simulate metric collection
                metrics = await self._simulate_job_metrics(job)

                # Store metric history
                for metric_name, value in metrics.items():
                    key = f"{job_id}_{metric_name}"
                    self.metric_history[key].append({
                        "timestamp": datetime.utcnow(),
                        "value": value
                    })

            except Exception as e:
                logger.error(f"Error collecting metrics for job {job_id}: {e}")

    async def _simulate_job_metrics(self, job: SparkJob) -> Dict[str, float]:
        """Simulate job metrics for demonstration"""
        import random

        return {
            "cpu_utilization": random.uniform(20, 95),
            "memory_utilization": random.uniform(30, 98),
            "job_duration": random.uniform(0.5, 3.0),
            "cost_per_hour": random.uniform(0.5, 5.0),
            "task_failure_rate": random.uniform(0, 15),
            "data_io_efficiency": random.uniform(0.4, 1.0),
            "job_success_rate": random.uniform(70, 100)
        }

    async def _check_thresholds(self):
        """Check metric thresholds and generate alerts"""
        for job_id in self.active_jobs:
            for threshold_name, threshold in self.thresholds.items():
                await self._check_single_threshold(job_id, threshold_name, threshold)

    async def _check_single_threshold(self, job_id: str, threshold_name: str, threshold: MetricThreshold):
        """Check a single threshold for a job"""
        try:
            # Get recent metric values
            key = f"{job_id}_{threshold.metric_name}"
            recent_metrics = [
                entry for entry in self.metric_history[key]
                if entry["timestamp"] > datetime.utcnow() - timedelta(minutes=threshold.window_minutes)
            ]

            if len(recent_metrics) < threshold.consecutive_violations:
                return

            # Check for consecutive violations
            recent_values = [entry["value"] for entry in recent_metrics[-threshold.consecutive_violations:]]

            violations = 0
            for value in recent_values:
                if self._is_threshold_violation(value, threshold):
                    violations += 1

            if violations == threshold.consecutive_violations:
                await self._generate_threshold_alert(job_id, threshold_name, threshold, recent_values[-1])

        except Exception as e:
            logger.error(f"Error checking threshold {threshold_name} for job {job_id}: {e}")

    def _is_threshold_violation(self, value: float, threshold: MetricThreshold) -> bool:
        """Check if a value violates the threshold"""
        if threshold.operator == "greater_than":
            return value > threshold.critical_threshold
        elif threshold.operator == "less_than":
            return value < threshold.critical_threshold
        elif threshold.operator == "equals":
            return abs(value - threshold.critical_threshold) < 0.01
        return False

    async def _generate_threshold_alert(self, job_id: str, threshold_name: str, threshold: MetricThreshold, value: float):
        """Generate an alert for threshold violation"""
        try:
            # Check if similar alert already exists and is in cooldown
            existing_alerts = [
                alert for alert in self.alerts.values()
                if (alert.job_id == job_id and
                    alert.alert_type.value == threshold_name and
                    alert.status == "active" and
                    alert.timestamp > datetime.utcnow() - timedelta(minutes=self.monitoring_config["alert_cooldown_minutes"]))
            ]

            if existing_alerts:
                return

            # Determine severity
            severity = AlertSeverity.CRITICAL if value > threshold.critical_threshold else AlertSeverity.HIGH

            # Create alert
            alert = Alert(
                alert_id=f"alert_{job_id}_{threshold_name}_{datetime.utcnow().isoformat()}",
                alert_type=AlertType(threshold_name),
                severity=severity,
                job_id=job_id,
                cluster_name=self.active_jobs.get(job_id, {}).cluster_name or "unknown",
                title=f"{threshold_name.replace('_', ' ').title()} Alert for Job {job_id}",
                description=f"Metric {threshold.metric_name} has exceeded threshold",
                metric_value=value,
                threshold_value=threshold.critical_threshold,
                timestamp=datetime.utcnow(),
                recommended_actions=await self._get_recommended_actions(threshold_name, value, threshold),
                context={
                    "threshold_name": threshold_name,
                    "window_minutes": threshold.window_minutes,
                    "consecutive_violations": threshold.consecutive_violations
                }
            )

            self.alerts[alert.alert_id] = alert
            self.monitoring_stats["alerts_generated"] += 1

            logger.warning(f"Generated {severity.value} alert for job {job_id}: {threshold_name} = {value}")

            # Send to AI engine for analysis
            await self._send_alert_to_ai_engine(alert)

        except Exception as e:
            logger.error(f"Error generating alert for job {job_id}: {e}")

    async def _get_recommended_actions(self, threshold_name: str, value: float, threshold: MetricThreshold) -> List[str]:
        """Get recommended actions for threshold violation"""
        actions = {
            "cpu_utilization": [
                "Check for CPU-intensive operations",
                "Consider increasing executor cores",
                "Optimize CPU-bound transformations",
                "Review data skew issues"
            ],
            "memory_utilization": [
                "Increase executor memory",
                "Check for memory leaks",
                "Optimize memory-intensive operations",
                "Review broadcast joins"
            ],
            "job_duration": [
                "Check for inefficient transformations",
                "Review data partitioning strategy",
                "Consider caching frequently used data",
                "Optimize shuffle operations"
            ],
            "job_failure_rate": [
                "Review job logs for error patterns",
                "Check data quality issues",
                "Validate input data formats",
                "Review resource allocation"
            ],
            "cost_per_hour": [
                "Optimize cluster sizing",
                "Use spot instances where possible",
                "Implement auto-scaling",
                "Review resource utilization"
            ]
        }

        return actions.get(threshold_name, ["Investigate the metric and take corrective action"])

    async def _send_alert_to_ai_engine(self, alert: Alert):
        """Send alert to AI engine for analysis"""
        try:
            # This would integrate with the AI engine to analyze the alert
            # and provide additional insights or recommendations
            pass
        except Exception as e:
            logger.error(f"Error sending alert to AI engine: {e}")

    async def _update_health_scores(self):
        """Update health scores for all jobs"""
        for job_id in self.active_jobs:
            await self._calculate_job_health_score(job_id)

    async def _calculate_job_health_score(self, job_id: str):
        """Calculate health score for a specific job"""
        try:
            # Collect recent metrics
            window_minutes = self.monitoring_config["health_score_window_minutes"]
            cutoff_time = datetime.utcnow() - timedelta(minutes=window_minutes)

            recent_metrics = {}
            for key, history in self.metric_history.items():
                if key.startswith(f"{job_id}_"):
                    metric_name = key.split("_", 1)[1]
                    recent_values = [
                        entry["value"] for entry in history
                        if entry["timestamp"] > cutoff_time
                    ]
                    if recent_values:
                        recent_metrics[metric_name] = sum(recent_values) / len(recent_values)

            # Calculate component scores
            performance_score = self._calculate_performance_score(recent_metrics)
            cost_score = self._calculate_cost_score(recent_metrics)
            resource_score = self._calculate_resource_score(recent_metrics)
            reliability_score = self._calculate_reliability_score(job_id)

            # Calculate overall score
            overall_score = (performance_score + cost_score + resource_score + reliability_score) / 4

            # Determine health status
            status = self._determine_health_status(overall_score)

            # Identify issues
            issues = self._identify_health_issues(recent_metrics)

            # Calculate trends
            trends = self._calculate_metric_trends(job_id, recent_metrics)

            # Create health score
            health_score = HealthScore(
                job_id=job_id,
                overall_score=overall_score,
                performance_score=performance_score,
                cost_score=cost_score,
                resource_score=resource_score,
                reliability_score=reliability_score,
                status=status,
                last_updated=datetime.utcnow(),
                trends=trends,
                issues=issues
            )

            self.health_scores[job_id] = health_score

        except Exception as e:
            logger.error(f"Error calculating health score for job {job_id}: {e}")

    def _calculate_performance_score(self, metrics: Dict[str, float]) -> float:
        """Calculate performance score"""
        score = 100.0

        # CPU utilization score
        cpu = metrics.get("cpu_utilization", 50)
        if cpu > 90:
            score -= 30
        elif cpu > 80:
            score -= 15
        elif cpu > 70:
            score -= 5

        # Memory utilization score
        memory = metrics.get("memory_utilization", 50)
        if memory > 95:
            score -= 30
        elif memory > 85:
            score -= 15
        elif memory > 75:
            score -= 5

        # Job duration score
        duration = metrics.get("job_duration", 1.0)
        if duration > 3.0:
            score -= 25
        elif duration > 2.0:
            score -= 10
        elif duration > 1.5:
            score -= 5

        return max(score, 0.0)

    def _calculate_cost_score(self, metrics: Dict[str, float]) -> float:
        """Calculate cost efficiency score"""
        score = 100.0

        cost = metrics.get("cost_per_hour", 1.0)
        if cost > 4.0:
            score -= 40
        elif cost > 2.5:
            score -= 20
        elif cost > 1.5:
            score -= 10

        return max(score, 0.0)

    def _calculate_resource_score(self, metrics: Dict[str, float]) -> float:
        """Calculate resource utilization score"""
        score = 100.0

        # Data I/O efficiency
        io_efficiency = metrics.get("data_io_efficiency", 0.8)
        if io_efficiency < 0.5:
            score -= 35
        elif io_efficiency < 0.7:
            score -= 15
        elif io_efficiency < 0.8:
            score -= 5

        # Task failure rate
        failure_rate = metrics.get("task_failure_rate", 0)
        if failure_rate > 10:
            score -= 30
        elif failure_rate > 5:
            score -= 15
        elif failure_rate > 2:
            score -= 5

        return max(score, 0.0)

    def _calculate_reliability_score(self, job_id: str) -> float:
        """Calculate reliability score based on job history"""
        # In a real implementation, this would analyze historical job success rates
        # For now, return a reasonable default
        return 85.0

    def _determine_health_status(self, overall_score: float) -> HealthStatus:
        """Determine health status from overall score"""
        if overall_score >= 90:
            return HealthStatus.HEALTHY
        elif overall_score >= 75:
            return HealthStatus.WARNING
        elif overall_score >= 60:
            return HealthStatus.DEGRADED
        elif overall_score >= 40:
            return HealthStatus.CRITICAL
        else:
            return HealthStatus.UNKNOWN

    def _identify_health_issues(self, metrics: Dict[str, float]) -> List[str]:
        """Identify specific health issues"""
        issues = []

        if metrics.get("cpu_utilization", 0) > 90:
            issues.append("High CPU utilization")

        if metrics.get("memory_utilization", 0) > 95:
            issues.append("High memory utilization")

        if metrics.get("job_duration", 0) > 3.0:
            issues.append("Long job duration")

        if metrics.get("cost_per_hour", 0) > 4.0:
            issues.append("High cost per hour")

        if metrics.get("task_failure_rate", 0) > 10:
            issues.append("High task failure rate")

        return issues

    def _calculate_metric_trends(self, job_id: str, current_metrics: Dict[str, float]) -> Dict[str, str]:
        """Calculate metric trends"""
        trends = {}

        for metric_name in current_metrics:
            key = f"{job_id}_{metric_name}"
            history = list(self.metric_history[key])

            if len(history) >= 10:
                # Compare recent average to older average
                recent_values = [entry["value"] for entry in history[-5:]]
                older_values = [entry["value"] for entry in history[-10:-5]]

                recent_avg = sum(recent_values) / len(recent_values)
                older_avg = sum(older_values) / len(older_values)

                change_percent = ((recent_avg - older_avg) / older_avg) * 100

                if change_percent > 10:
                    trends[metric_name] = "increasing"
                elif change_percent < -10:
                    trends[metric_name] = "decreasing"
                else:
                    trends[metric_name] = "stable"

        return trends

    async def _run_predictive_analysis(self):
        """Run predictive analysis for potential issues"""
        for job_id in self.active_jobs:
            await self._predict_job_issues(job_id)

    async def _predict_job_issues(self, job_id: str):
        """Predict potential issues for a job"""
        try:
            # Analyze recent metric trends
            trends = self.health_scores.get(job_id, HealthScore(job_id, 0, 0, 0, 0, 0, HealthStatus.UNKNOWN, datetime.utcnow())).trends

            # Predictive rules
            predictions = []

            # Predict performance degradation
            if trends.get("cpu_utilization") == "increasing" and trends.get("memory_utilization") == "increasing":
                predictions.append({
                    "type": "performance_degradation",
                    "confidence": 0.8,
                    "timeframe_hours": 6,
                    "description": "CPU and memory utilization trending upward"
                })

            # Predict cost overrun
            if trends.get("cost_per_hour") == "increasing":
                predictions.append({
                    "type": "cost_overrun",
                    "confidence": 0.7,
                    "timeframe_hours": 12,
                    "description": "Cost per hour is increasing"
                })

            # Generate predictive alerts
            for prediction in predictions:
                if prediction["confidence"] > 0.7:
                    await self._generate_predictive_alert(job_id, prediction)

            self.monitoring_stats["predictions_made"] += len(predictions)

        except Exception as e:
            logger.error(f"Error running predictive analysis for job {job_id}: {e}")

    async def _generate_predictive_alert(self, job_id: str, prediction: Dict[str, Any]):
        """Generate a predictive alert"""
        alert = Alert(
            alert_id=f"predictive_{job_id}_{prediction['type']}_{datetime.utcnow().isoformat()}",
            alert_type=AlertType.PREDICTIVE_FAILURE,
            severity=AlertSeverity.MEDIUM,
            job_id=job_id,
            cluster_name=self.active_jobs.get(job_id, {}).cluster_name or "unknown",
            title=f"Predictive Alert: {prediction['type'].replace('_', ' ').title()}",
            description=prediction["description"],
            metric_value=prediction["confidence"],
            threshold_value=0.7,
            timestamp=datetime.utcnow(),
            context={
                "prediction_type": prediction["type"],
                "confidence": prediction["confidence"],
                "timeframe_hours": prediction["timeframe_hours"]
            }
        )

        self.alerts[alert.alert_id] = alert
        logger.info(f"Generated predictive alert for job {job_id}: {prediction['type']}")

    async def _process_alert_rules(self):
        """Process alert correlation and escalation rules"""
        # Check for alert correlations
        await self._check_alert_correlations()

        # Check for escalation rules
        await self._check_alert_escalations()

        # Check for auto-resolution
        await self._check_auto_resolution()

    async def _check_alert_correlations(self):
        """Check for alert correlations"""
        active_alerts = [alert for alert in self.alerts.values() if alert.status == "active"]

        for rule in self.alert_rules.get("correlation_rules", []):
            matching_alerts = []

            for condition in rule["conditions"]:
                metric = condition["metric"]
                operator = condition["operator"]
                value = condition["value"]

                for alert in active_alerts:
                    if alert.alert_type.value == metric:
                        if self._evaluate_condition(alert.metric_value, operator, value):
                            matching_alerts.append(alert)

            # If multiple conditions match, create correlated alert
            if len(matching_alerts) > 1:
                await self._create_correlated_alert(matching_alerts, rule)

    def _evaluate_condition(self, value: float, operator: str, threshold: float) -> bool:
        """Evaluate a condition"""
        if operator == ">":
            return value > threshold
        elif operator == "<":
            return value < threshold
        elif operator == ">=":
            return value >= threshold
        elif operator == "<=":
            return value <= threshold
        elif operator == "==":
            return abs(value - threshold) < 0.01
        return False

    async def _create_correlated_alert(self, alerts: List[Alert], rule: Dict[str, Any]):
        """Create a correlated alert"""
        # Check if correlated alert already exists
        existing_correlated = [
            alert for alert in self.alerts.values()
            if (alert.alert_type == AlertType.PERFORMANCE_DEGRADATION and
                alert.job_id == alerts[0].job_id and
                alert.status == "active")
        ]

        if existing_correlated:
            return

        correlated_alert = Alert(
            alert_id=f"correlated_{alerts[0].job_id}_{datetime.utcnow().isoformat()}",
            alert_type=AlertType.PERFORMANCE_DEGRADATION,
            severity=AlertSeverity(rule["severity"]),
            job_id=alerts[0].job_id,
            cluster_name=alerts[0].cluster_name,
            title=rule["description"],
            description=f"Multiple metrics indicate performance issues",
            metric_value=0.0,
            threshold_value=0.0,
            timestamp=datetime.utcnow(),
            context={
                "correlated_alerts": [alert.alert_id for alert in alerts],
                "rule": rule["name"]
            }
        )

        self.alerts[correlated_alert.alert_id] = correlated_alert
        logger.warning(f"Created correlated alert for job {alerts[0].job_id}")

    async def _check_alert_escalations(self):
        """Check for alert escalations"""
        for alert in self.alerts.values():
            if alert.status == "active":
                duration = datetime.utcnow() - alert.timestamp

                for rule in self.alert_rules.get("escalation_rules", []):
                    if self._evaluate_escalation_condition(alert, duration, rule):
                        await self._escalate_alert(alert, rule)

    def _evaluate_escalation_condition(self, alert: Alert, duration: timedelta, rule: Dict[str, Any]) -> bool:
        """Evaluate escalation condition"""
        condition = rule["condition"]

        if "alert_duration_minutes" in condition:
            threshold_minutes = condition.split(">")[1].strip()
            return duration.total_seconds() > int(threshold_minutes) * 60

        return False

    async def _escalate_alert(self, alert: Alert, rule: Dict[str, Any]):
        """Escalate an alert"""
        if rule["action"] == "escalate_severity":
            alert.severity = AlertSeverity(rule["new_severity"])
            logger.info(f"Escalated alert {alert.alert_id} to {rule['new_severity']}")

    async def _check_auto_resolution(self):
        """Check for auto-resolution of alerts"""
        for alert in self.alerts.values():
            if alert.status == "active":
                await self._check_alert_auto_resolution(alert)

    async def _check_alert_auto_resolution(self, alert: Alert):
        """Check if an alert should be auto-resolved"""
        # Check if metric has returned to normal
        key = f"{alert.job_id}_{alert.alert_type.value}"
        recent_metrics = [
            entry for entry in self.metric_history[key]
            if entry["timestamp"] > datetime.utcnow() - timedelta(minutes=30)
        ]

        if recent_metrics:
            avg_value = sum(entry["value"] for entry in recent_metrics) / len(recent_metrics)

            # Check if value is back to normal
            threshold = self.thresholds.get(alert.alert_type.value)
            if threshold and avg_value < threshold.warning_threshold:
                alert.status = "resolved"
                alert.resolution_time = datetime.utcnow()
                self.monitoring_stats["alerts_resolved"] += 1
                logger.info(f"Auto-resolved alert {alert.alert_id}")

    async def _update_baselines(self):
        """Update monitoring baselines"""
        for job_id in self.active_jobs:
            await self._update_job_baselines(job_id)

    async def _update_job_baselines(self, job_id: str):
        """Update baselines for a specific job"""
        # In a real implementation, this would calculate baselines from historical data
        pass

    async def _cleanup_old_data(self):
        """Clean up old monitoring data"""
        cutoff_time = datetime.utcnow() - timedelta(hours=self.monitoring_config["history_retention_hours"])

        # Clean up metric history
        for key in self.metric_history:
            self.metric_history[key] = deque(
                (entry for entry in self.metric_history[key] if entry["timestamp"] > cutoff_time),
                maxlen=1000
            )

        # Clean up resolved alerts
        resolved_alerts = [
            alert_id for alert_id, alert in self.alerts.items()
            if (alert.status == "resolved" and
                alert.resolution_time and
                alert.resolution_time < cutoff_time)
        ]

        for alert_id in resolved_alerts:
            del self.alerts[alert_id]

    def add_job(self, job: SparkJob):
        """Add a job to monitoring"""
        self.active_jobs[job.job_id] = job
        logger.info(f"Added job {job.job_id} to monitoring")

    def remove_job(self, job_id: str):
        """Remove a job from monitoring"""
        if job_id in self.active_jobs:
            del self.active_jobs[job_id]
        if job_id in self.health_scores:
            del self.health_scores[job_id]
        logger.info(f"Removed job {job_id} from monitoring")

    async def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status"""
        return {
            "monitoring_stats": {
                **self.monitoring_stats,
                "last_check": self.monitoring_stats["last_check"].isoformat() if self.monitoring_stats["last_check"] else None
            },
            "active_jobs": len(self.active_jobs),
            "active_alerts": len([a for a in self.alerts.values() if a.status == "active"]),
            "health_summary": {
                "healthy": len([h for h in self.health_scores.values() if h.status == HealthStatus.HEALTHY]),
                "warning": len([h for h in self.health_scores.values() if h.status == HealthStatus.WARNING]),
                "degraded": len([h for h in self.health_scores.values() if h.status == HealthStatus.DEGRADED]),
                "critical": len([h for h in self.health_scores.values() if h.status == HealthStatus.CRITICAL])
            },
            "alert_summary": {
                "critical": len([a for a in self.alerts.values() if a.severity == AlertSeverity.CRITICAL and a.status == "active"]),
                "high": len([a for a in self.alerts.values() if a.severity == AlertSeverity.HIGH and a.status == "active"]),
                "medium": len([a for a in self.alerts.values() if a.severity == AlertSeverity.MEDIUM and a.status == "active"]),
                "low": len([a for a in self.alerts.values() if a.severity == AlertSeverity.LOW and a.status == "active"])
            }
        }

    def get_job_health(self, job_id: str) -> Optional[HealthScore]:
        """Get health score for a specific job"""
        return self.health_scores.get(job_id)

    def get_active_alerts(self, severity: AlertSeverity = None, job_id: str = None) -> List[Alert]:
        """Get active alerts with optional filtering"""
        alerts = [alert for alert in self.alerts.values() if alert.status == "active"]

        if severity:
            alerts = [alert for alert in alerts if alert.severity == severity]

        if job_id:
            alerts = [alert for alert in alerts if alert.job_id == job_id]

        return sorted(alerts, key=lambda x: x.timestamp, reverse=True)

    def acknowledge_alert(self, alert_id: str, acknowledged_by: str) -> bool:
        """Acknowledge an alert"""
        if alert_id in self.alerts:
            self.alerts[alert_id].status = "acknowledged"
            self.alerts[alert_id].acknowledged_by = acknowledged_by
            return True
        return False

    def resolve_alert(self, alert_id: str) -> bool:
        """Resolve an alert"""
        if alert_id in self.alerts:
            self.alerts[alert_id].status = "resolved"
            self.alerts[alert_id].resolution_time = datetime.utcnow()
            self.monitoring_stats["alerts_resolved"] += 1
            return True
        return False