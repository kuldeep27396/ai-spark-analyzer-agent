"""
Multi-Cluster Management System
Centralized management of multiple Dataproc clusters with intelligent orchestration
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import json
from collections import defaultdict

from ..core.config import Config
from ..core.models import Cluster, SparkJob
from ..dataproc.client import DataprocClient
from ..ai.agentic_engine import AgenticAIEngine
from ..discovery.job_discovery import JobDiscoveryManager
from ..monitoring.autonomous_monitoring import AutonomousMonitoringSystem

logger = logging.getLogger(__name__)


class ClusterStatus(Enum):
    ACTIVE = "active"
    MAINTENANCE = "maintenance"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    UNKNOWN = "unknown"


class ClusterType(Enum):
    PRODUCTION = "production"
    STAGING = "staging"
    DEVELOPMENT = "development"
    TESTING = "testing"


@dataclass
class ClusterMetrics:
    cluster_name: str
    cpu_utilization: float
    memory_utilization: float
    disk_utilization: float
    active_jobs: int
    queued_jobs: int
    failed_jobs: int
    total_jobs: int
    cost_per_hour: float
    efficiency_score: float
    last_updated: datetime


@dataclass
class ClusterPolicy:
    policy_id: str
    policy_name: str
    cluster_types: List[ClusterType]
    conditions: Dict[str, Any]
    actions: List[str]
    priority: int
    enabled: bool
    created_at: datetime
    last_applied: Optional[datetime] = None
    application_count: int = 0


@dataclass
class ClusterGroup:
    group_id: str
    group_name: str
    cluster_names: List[str]
    group_type: str  # region, environment, application, custom
    policies: List[str]
    load_balancing_strategy: str
    created_at: datetime


class MultiClusterManager:
    """
    Multi-cluster management system with intelligent orchestration
    """

    def __init__(self, config: Config, ai_engine: AgenticAIEngine):
        self.config = config
        self.ai_engine = ai_engine
        self.dataproc_client = DataprocClient(config)

        # Cluster management state
        self.clusters: Dict[str, Cluster] = {}
        self.cluster_metrics: Dict[str, ClusterMetrics] = {}
        self.cluster_groups: Dict[str, ClusterGroup] = {}
        self.cluster_policies: Dict[str, ClusterPolicy] = {}
        self.cluster_jobs: Dict[str, List[SparkJob]] = defaultdict(list)

        # Subsystems
        self.job_discovery = JobDiscoveryManager(config, ai_engine)
        self.monitoring_system = AutonomousMonitoringSystem(config, ai_engine)

        # Management configuration
        self.management_config = {
            "health_check_interval_minutes": 5,
            "metrics_retention_hours": 168,  # 7 days
            "auto_scale_enabled": True,
            "load_balancing_enabled": True,
            "policy_enforcement_enabled": True,
            "cost_optimization_enabled": True,
            "max_concurrent_operations": 10
        }

        # Statistics
        self.management_stats = {
            "total_clusters": 0,
            "active_clusters": 0,
            "total_jobs": 0,
            "policies_applied": 0,
            "auto_scalings": 0,
            "last_health_check": None,
            "cluster_failures": 0
        }

        # Initialize default policies
        self._initialize_default_policies()

        logger.info("Multi-Cluster Manager initialized")

    def _initialize_default_policies(self):
        """Initialize default cluster management policies"""
        default_policies = [
            {
                "policy_id": "auto_scale_high_cpu",
                "policy_name": "Auto-scale on High CPU",
                "cluster_types": [ClusterType.PRODUCTION],
                "conditions": {"cpu_utilization": "> 80", "duration_minutes": 10},
                "actions": ["scale_up_workers", "notify_admins"],
                "priority": 8
            },
            {
                "policy_id": "auto_scale_low_cpu",
                "policy_name": "Auto-scale on Low CPU",
                "cluster_types": [ClusterType.PRODUCTION, ClusterType.STAGING],
                "conditions": {"cpu_utilization": "< 20", "duration_minutes": 30},
                "actions": ["scale_down_workers", "optimize_costs"],
                "priority": 6
            },
            {
                "policy_id": "cost_optimization",
                "policy_name": "Cost Optimization",
                "cluster_types": [ClusterType.DEVELOPMENT, ClusterType.TESTING],
                "conditions": {"cost_per_hour": "> 5.0", "efficiency_score": "< 0.6"},
                "actions": ["use_spot_instances", "rightsize_cluster", "schedule_shutdown"],
                "priority": 7
            },
            {
                "policy_id": "job_failure_response",
                "policy_name": "Job Failure Response",
                "cluster_types": [ClusterType.PRODUCTION],
                "conditions": {"failure_rate": "> 10%", "consecutive_failures": 3},
                "actions": ["restart_failed_jobs", "escalate_alert", "check_cluster_health"],
                "priority": 9
            },
            {
                "policy_id": "maintenance_window",
                "policy_name": "Maintenance Window Enforcement",
                "cluster_types": [ClusterType.PRODUCTION],
                "conditions": {"time_window": "maintenance", "critical_jobs": "none"},
                "actions": ["enable_maintenance_mode", "defer_non_critical_jobs"],
                "priority": 5
            }
        ]

        for policy_data in default_policies:
            policy = ClusterPolicy(
                policy_id=policy_data["policy_id"],
                policy_name=policy_data["policy_name"],
                cluster_types=policy_data["cluster_types"],
                conditions=policy_data["conditions"],
                actions=policy_data["actions"],
                priority=policy_data["priority"],
                enabled=True,
                created_at=datetime.utcnow()
            )
            self.cluster_policies[policy.policy_id] = policy

    async def start_management(self):
        """Start multi-cluster management"""
        logger.info("Starting multi-cluster management")

        # Start subsystems
        asyncio.create_task(self.job_discovery.start_continuous_discovery())
        asyncio.create_task(self.monitoring_system.start_monitoring())

        # Start main management loop
        while True:
            try:
                await self._management_cycle()
                await asyncio.sleep(self.management_config["health_check_interval_minutes"] * 60)
            except Exception as e:
                logger.error(f"Error in management cycle: {e}")
                await asyncio.sleep(60)

    async def _management_cycle(self):
        """Execute one management cycle"""
        logger.debug("Starting management cycle")

        # 1. Discover and update clusters
        await self._update_cluster_inventory()

        # 2. Collect cluster metrics
        await self._collect_cluster_metrics()

        # 3. Evaluate cluster health
        await self._evaluate_cluster_health()

        # 4. Apply cluster policies
        if self.management_config["policy_enforcement_enabled"]:
            await self._apply_cluster_policies()

        # 5. Optimize resource allocation
        if self.management_config["load_balancing_enabled"]:
            await self._optimize_resource_allocation()

        # 6. Cost optimization
        if self.management_config["cost_optimization_enabled"]:
            await self._perform_cost_optimization()

        # 7. Update statistics
        self._update_management_stats()

        self.management_stats["last_health_check"] = datetime.utcnow()

    async def _update_cluster_inventory(self):
        """Update cluster inventory"""
        try:
            discovered_clusters = await self.dataproc_client.list_clusters()

            for cluster in discovered_clusters:
                if cluster.cluster_name not in self.clusters:
                    # New cluster discovered
                    self.clusters[cluster.cluster_name] = cluster
                    logger.info(f"Discovered new cluster: {cluster.cluster_name}")

                    # Start monitoring for this cluster
                    await self._setup_cluster_monitoring(cluster)

                else:
                    # Update existing cluster
                    self.clusters[cluster.cluster_name] = cluster

            # Remove clusters that no longer exist
            current_cluster_names = {c.cluster_name for c in discovered_clusters}
            removed_clusters = set(self.clusters.keys()) - current_cluster_names
            for cluster_name in removed_clusters:
                del self.clusters[cluster_name]
                if cluster_name in self.cluster_metrics:
                    del self.cluster_metrics[cluster_name]
                logger.info(f"Removed cluster: {cluster_name}")

        except Exception as e:
            logger.error(f"Error updating cluster inventory: {e}")

    async def _setup_cluster_monitoring(self, cluster: Cluster):
        """Setup monitoring for a new cluster"""
        try:
            # Start job discovery for this cluster
            discovery_result = await self.job_discovery.manual_discovery([cluster.cluster_name])
            logger.info(f"Setup monitoring for cluster {cluster.cluster_name}: {discovery_result}")

        except Exception as e:
            logger.error(f"Error setting up monitoring for cluster {cluster.cluster_name}: {e}")

    async def _collect_cluster_metrics(self):
        """Collect metrics for all clusters"""
        for cluster_name, cluster in self.clusters.items():
            try:
                if cluster.status in ["RUNNING", "ACTIVE"]:
                    metrics = await self._collect_single_cluster_metrics(cluster_name)
                    if metrics:
                        self.cluster_metrics[cluster_name] = metrics

            except Exception as e:
                logger.error(f"Error collecting metrics for cluster {cluster_name}: {e}")

    async def _collect_single_cluster_metrics(self, cluster_name: str) -> Optional[ClusterMetrics]:
        """Collect metrics for a single cluster"""
        try:
            # In a real implementation, this would collect actual metrics from the cluster
            # For now, simulate metrics
            import random

            return ClusterMetrics(
                cluster_name=cluster_name,
                cpu_utilization=random.uniform(20, 90),
                memory_utilization=random.uniform(30, 85),
                disk_utilization=random.uniform(10, 70),
                active_jobs=random.randint(1, 20),
                queued_jobs=random.randint(0, 5),
                failed_jobs=random.randint(0, 3),
                total_jobs=random.randint(10, 100),
                cost_per_hour=random.uniform(1.0, 10.0),
                efficiency_score=random.uniform(0.5, 0.95),
                last_updated=datetime.utcnow()
            )

        except Exception as e:
            logger.error(f"Error collecting metrics for cluster {cluster_name}: {e}")
            return None

    async def _evaluate_cluster_health(self):
        """Evaluate health of all clusters"""
        for cluster_name, metrics in self.cluster_metrics.items():
            try:
                health_score = self._calculate_cluster_health_score(metrics)
                cluster = self.clusters.get(cluster_name)

                if cluster:
                    # Update cluster status based on health
                    if health_score >= 0.9:
                        new_status = ClusterStatus.ACTIVE
                    elif health_score >= 0.7:
                        new_status = ClusterStatus.DEGRADED
                    else:
                        new_status = ClusterStatus.OFFLINE

                    if cluster.status != new_status.value:
                        logger.warning(f"Cluster {cluster_name} status changed from {cluster.status} to {new_status.value}")
                        cluster.status = new_status.value

                        # Handle status change
                        await self._handle_cluster_status_change(cluster_name, cluster.status, new_status.value)

            except Exception as e:
                logger.error(f"Error evaluating health for cluster {cluster_name}: {e}")

    def _calculate_cluster_health_score(self, metrics: ClusterMetrics) -> float:
        """Calculate health score for a cluster"""
        score = 100.0

        # CPU utilization impact
        if metrics.cpu_utilization > 90:
            score -= 30
        elif metrics.cpu_utilization > 80:
            score -= 15
        elif metrics.cpu_utilization > 70:
            score -= 5

        # Memory utilization impact
        if metrics.memory_utilization > 95:
            score -= 30
        elif metrics.memory_utilization > 85:
            score -= 15
        elif metrics.memory_utilization > 75:
            score -= 5

        # Failed jobs impact
        if metrics.failed_jobs > 5:
            score -= 25
        elif metrics.failed_jobs > 2:
            score -= 10
        elif metrics.failed_jobs > 0:
            score -= 5

        # Efficiency impact
        if metrics.efficiency_score < 0.5:
            score -= 20
        elif metrics.efficiency_score < 0.7:
            score -= 10
        elif metrics.efficiency_score < 0.8:
            score -= 5

        return max(score, 0.0) / 100.0

    async def _handle_cluster_status_change(self, cluster_name: str, old_status: str, new_status: str):
        """Handle cluster status change"""
        try:
            if new_status == ClusterStatus.OFFLINE.value:
                # Handle cluster going offline
                await self._handle_cluster_offline(cluster_name)
                self.management_stats["cluster_failures"] += 1

            elif new_status == ClusterStatus.DEGRADED.value:
                # Handle degraded performance
                await self._handle_cluster_degraded(cluster_name)

            elif old_status != ClusterStatus.ACTIVE.value and new_status == ClusterStatus.ACTIVE.value:
                # Handle cluster recovery
                await self._handle_cluster_recovery(cluster_name)

        except Exception as e:
            logger.error(f"Error handling status change for cluster {cluster_name}: {e}")

    async def _handle_cluster_offline(self, cluster_name: str):
        """Handle cluster going offline"""
        logger.warning(f"Cluster {cluster_name} is offline, initiating failover procedures")

        # Reschedule jobs to other clusters
        jobs_to_reschedule = self.cluster_jobs.get(cluster_name, [])
        for job in jobs_to_reschedule:
            if job.status == "RUNNING":
                await self._reschedule_job(job, cluster_name)

    async def _handle_cluster_degraded(self, cluster_name: str):
        """Handle degraded cluster performance"""
        logger.info(f"Cluster {cluster_name} is degraded, applying optimization measures")

        # Apply performance optimization policies
        await self._apply_cluster_specific_policies(cluster_name, "performance")

    async def _handle_cluster_recovery(self, cluster_name: str):
        """Handle cluster recovery"""
        logger.info(f"Cluster {cluster_name} has recovered")

        # Restart any failed jobs
        failed_jobs = [job for job in self.cluster_jobs.get(cluster_name, []) if job.status == "ERROR"]
        for job in failed_jobs:
            await self._restart_job(job)

    async def _apply_cluster_policies(self):
        """Apply cluster management policies"""
        for policy_id, policy in self.cluster_policies.items():
            if not policy.enabled:
                continue

            try:
                applicable_clusters = await self._get_clusters_for_policy(policy)
                for cluster_name in applicable_clusters:
                    if await self._evaluate_policy_conditions(cluster_name, policy.conditions):
                        await self._execute_policy_actions(cluster_name, policy)
                        policy.last_applied = datetime.utcnow()
                        policy.application_count += 1
                        self.management_stats["policies_applied"] += 1

            except Exception as e:
                logger.error(f"Error applying policy {policy_id}: {e}")

    async def _get_clusters_for_policy(self, policy: ClusterPolicy) -> List[str]:
        """Get clusters that match policy criteria"""
        applicable_clusters = []

        for cluster_name, cluster in self.clusters.items():
            # Check cluster type
            cluster_type = self._determine_cluster_type(cluster)
            if cluster_type in policy.cluster_types:
                applicable_clusters.append(cluster_name)

        return applicable_clusters

    def _determine_cluster_type(self, cluster: Cluster) -> ClusterType:
        """Determine cluster type based on cluster properties"""
        cluster_name_lower = cluster.cluster_name.lower()

        if "prod" in cluster_name_lower or "production" in cluster_name_lower:
            return ClusterType.PRODUCTION
        elif "stage" in cluster_name_lower or "staging" in cluster_name_lower:
            return ClusterType.STAGING
        elif "dev" in cluster_name_lower or "development" in cluster_name_lower:
            return ClusterType.DEVELOPMENT
        elif "test" in cluster_name_lower or "testing" in cluster_name_lower:
            return ClusterType.TESTING
        else:
            return ClusterType.DEVELOPMENT  # Default

    async def _evaluate_policy_conditions(self, cluster_name: str, conditions: Dict[str, Any]) -> bool:
        """Evaluate if policy conditions are met for a cluster"""
        metrics = self.cluster_metrics.get(cluster_name)
        if not metrics:
            return False

        try:
            for condition, value in conditions.items():
                if condition == "cpu_utilization":
                    if not self._evaluate_numeric_condition(metrics.cpu_utilization, value):
                        return False
                elif condition == "memory_utilization":
                    if not self._evaluate_numeric_condition(metrics.memory_utilization, value):
                        return False
                elif condition == "cost_per_hour":
                    if not self._evaluate_numeric_condition(metrics.cost_per_hour, value):
                        return False
                elif condition == "efficiency_score":
                    if not self._evaluate_numeric_condition(metrics.efficiency_score, value):
                        return False
                elif condition == "failure_rate":
                    failure_rate = (metrics.failed_jobs / metrics.total_jobs * 100) if metrics.total_jobs > 0 else 0
                    if not self._evaluate_numeric_condition(failure_rate, value):
                        return False
                elif condition == "time_window":
                    # Check if current time matches the condition
                    if not self._evaluate_time_condition(value):
                        return False

            return True

        except Exception as e:
            logger.error(f"Error evaluating policy conditions for cluster {cluster_name}: {e}")
            return False

    def _evaluate_numeric_condition(self, actual_value: float, condition: str) -> bool:
        """Evaluate a numeric condition"""
        try:
            if ">" in condition:
                threshold = float(condition.split(">")[1].strip())
                return actual_value > threshold
            elif "<" in condition:
                threshold = float(condition.split("<")[1].strip())
                return actual_value < threshold
            elif ">=" in condition:
                threshold = float(condition.split(">=")[1].strip())
                return actual_value >= threshold
            elif "<=" in condition:
                threshold = float(condition.split("<=")[1].strip())
                return actual_value <= threshold
            elif "=" in condition:
                threshold = float(condition.split("=")[1].strip())
                return abs(actual_value - threshold) < 0.01
        except:
            pass
        return False

    def _evaluate_time_condition(self, condition: str) -> bool:
        """Evaluate a time-based condition"""
        current_hour = datetime.utcnow().hour

        if condition == "maintenance":
            # Example: maintenance window is 2-4 AM UTC
            return 2 <= current_hour <= 4
        elif condition == "business_hours":
            # Example: business hours are 9-17 UTC
            return 9 <= current_hour <= 17

        return False

    async def _execute_policy_actions(self, cluster_name: str, policy: ClusterPolicy):
        """Execute policy actions for a cluster"""
        try:
            for action in policy.actions:
                logger.info(f"Executing action '{action}' for cluster {cluster_name} based on policy {policy.policy_name}")

                if action == "scale_up_workers":
                    await self._scale_cluster_workers(cluster_name, "up")
                elif action == "scale_down_workers":
                    await self._scale_cluster_workers(cluster_name, "down")
                elif action == "use_spot_instances":
                    await self._enable_spot_instances(cluster_name)
                elif action == "rightsize_cluster":
                    await self._rightsize_cluster(cluster_name)
                elif action == "schedule_shutdown":
                    await self._schedule_cluster_shutdown(cluster_name)
                elif action == "notify_admins":
                    await self._notify_admins(cluster_name, policy.policy_name)
                elif action == "restart_failed_jobs":
                    await self._restart_failed_jobs(cluster_name)
                elif action == "escalate_alert":
                    await self._escalate_alert(cluster_name)
                elif action == "optimize_costs":
                    await self._optimize_cluster_costs(cluster_name)

        except Exception as e:
            logger.error(f"Error executing policy actions for cluster {cluster_name}: {e}")

    async def _scale_cluster_workers(self, cluster_name: str, direction: str):
        """Scale cluster workers up or down"""
        try:
            # In a real implementation, this would call the Dataproc API
            logger.info(f"Scaling {direction} workers for cluster {cluster_name}")
            self.management_stats["auto_scalings"] += 1

        except Exception as e:
            logger.error(f"Error scaling cluster {cluster_name}: {e}")

    async def _enable_spot_instances(self, cluster_name: str):
        """Enable spot instances for cost optimization"""
        try:
            logger.info(f"Enabling spot instances for cluster {cluster_name}")
        except Exception as e:
            logger.error(f"Error enabling spot instances for cluster {cluster_name}: {e}")

    async def _rightsize_cluster(self, cluster_name: str):
        """Rightsize cluster based on current workload"""
        try:
            logger.info(f"Rightsizing cluster {cluster_name}")
        except Exception as e:
            logger.error(f"Error rightsizing cluster {cluster_name}: {e}")

    async def _schedule_cluster_shutdown(self, cluster_name: str):
        """Schedule cluster shutdown for cost savings"""
        try:
            logger.info(f"Scheduling shutdown for cluster {cluster_name}")
        except Exception as e:
            logger.error(f"Error scheduling shutdown for cluster {cluster_name}: {e}")

    async def _notify_admins(self, cluster_name: str, policy_name: str):
        """Notify administrators about policy actions"""
        try:
            logger.info(f"Notifying admins about policy '{policy_name}' for cluster {cluster_name}")
        except Exception as e:
            logger.error(f"Error notifying admins: {e}")

    async def _restart_failed_jobs(self, cluster_name: str):
        """Restart failed jobs on a cluster"""
        try:
            failed_jobs = [job for job in self.cluster_jobs.get(cluster_name, []) if job.status == "ERROR"]
            for job in failed_jobs:
                await self._restart_job(job)
        except Exception as e:
            logger.error(f"Error restarting failed jobs on cluster {cluster_name}: {e}")

    async def _escalate_alert(self, cluster_name: str):
        """Escalate alerts for a cluster"""
        try:
            logger.warning(f"Escalating alerts for cluster {cluster_name}")
        except Exception as e:
            logger.error(f"Error escalating alerts for cluster {cluster_name}: {e}")

    async def _optimize_cluster_costs(self, cluster_name: str):
        """Optimize costs for a cluster"""
        try:
            logger.info(f"Optimizing costs for cluster {cluster_name}")
        except Exception as e:
            logger.error(f"Error optimizing costs for cluster {cluster_name}: {e}")

    async def _optimize_resource_allocation(self):
        """Optimize resource allocation across clusters"""
        try:
            # Analyze resource utilization across all clusters
            total_cpu = sum(m.cpu_utilization for m in self.cluster_metrics.values())
            total_memory = sum(m.memory_utilization for m in self.cluster_metrics.values())
            avg_cpu = total_cpu / len(self.cluster_metrics) if self.cluster_metrics else 0
            avg_memory = total_memory / len(self.cluster_metrics) if self.cluster_metrics else 0

            # Identify imbalances
            overutilized_clusters = [
                name for name, metrics in self.cluster_metrics.items()
                if metrics.cpu_utilization > 80 or metrics.memory_utilization > 85
            ]

            underutilized_clusters = [
                name for name, metrics in self.cluster_metrics.items()
                if metrics.cpu_utilization < 30 and metrics.memory_utilization < 40
            ]

            # Suggest reallocations
            if overutilized_clusters and underutilized_clusters:
                await self._suggest_load_balancing(overutilized_clusters, underutilized_clusters)

        except Exception as e:
            logger.error(f"Error optimizing resource allocation: {e}")

    async def _suggest_load_balancing(self, overutilized: List[str], underutilized: List[str]):
        """Suggest load balancing between clusters"""
        try:
            for over_cluster in overutilized:
                for under_cluster in underutilized:
                    logger.info(f"Suggesting load balancing from {over_cluster} to {under_cluster}")
                    # In a real implementation, this would trigger job migration

        except Exception as e:
            logger.error(f"Error suggesting load balancing: {e}")

    async def _perform_cost_optimization(self):
        """Perform cost optimization across all clusters"""
        try:
            for cluster_name, metrics in self.cluster_metrics.items():
                # Check for cost optimization opportunities
                if metrics.cost_per_hour > 5.0 and metrics.efficiency_score < 0.7:
                    await self._apply_cluster_specific_policies(cluster_name, "cost")

        except Exception as e:
            logger.error(f"Error performing cost optimization: {e}")

    async def _apply_cluster_specific_policies(self, cluster_name: str, policy_type: str):
        """Apply cluster-specific policies"""
        for policy_id, policy in self.cluster_policies.items():
            if policy_type in policy.policy_name.lower():
                if await self._evaluate_policy_conditions(cluster_name, policy.conditions):
                    await self._execute_policy_actions(cluster_name, policy)

    async def _reschedule_job(self, job: SparkJob, original_cluster: str):
        """Reschedule a job to another cluster"""
        try:
            # Find suitable cluster for rescheduling
            suitable_clusters = [
                name for name, cluster in self.clusters.items()
                if (cluster.status in ["RUNNING", "ACTIVE"] and
                    name != original_cluster and
                    self.cluster_metrics.get(name, ClusterMetrics("", 0, 0, 0, 0, 0, 0, 0, 0, 0, datetime.utcnow()).cpu_utilization < 70)
            ]

            if suitable_clusters:
                target_cluster = suitable_clusters[0]
                logger.info(f"Rescheduling job {job.job_id} from {original_cluster} to {target_cluster}")
                # In a real implementation, this would submit the job to the target cluster

        except Exception as e:
            logger.error(f"Error rescheduling job {job.job_id}: {e}")

    async def _restart_job(self, job: SparkJob):
        """Restart a failed job"""
        try:
            logger.info(f"Restarting job {job.job_id}")
            # In a real implementation, this would restart the job
        except Exception as e:
            logger.error(f"Error restarting job {job.job_id}: {e}")

    def _update_management_stats(self):
        """Update management statistics"""
        self.management_stats["total_clusters"] = len(self.clusters)
        self.management_stats["active_clusters"] = len([
            c for c in self.clusters.values() if c.status in ["RUNNING", "ACTIVE"]
        ])
        self.management_stats["total_jobs"] = sum(len(jobs) for jobs in self.cluster_jobs.values())

    async def get_cluster_status(self) -> Dict[str, Any]:
        """Get overall cluster status"""
        return {
            "management_stats": {
                **self.management_stats,
                "last_health_check": self.management_stats["last_health_check"].isoformat() if self.management_stats["last_health_check"] else None
            },
            "clusters": {
                name: {
                    "status": cluster.status,
                    "type": cluster.cluster_type,
                    "region": cluster.region,
                    "created_at": cluster.created_at.isoformat() if cluster.created_at else None
                }
                for name, cluster in self.clusters.items()
            },
            "cluster_groups": {
                group_id: {
                    "name": group.group_name,
                    "cluster_count": len(group.cluster_names),
                    "group_type": group.group_type
                }
                for group_id, group in self.cluster_groups.items()
            },
            "active_policies": len([p for p in self.cluster_policies.values() if p.enabled]),
            "metrics_summary": {
                "total_clusters": len(self.cluster_metrics),
                "avg_cpu_utilization": sum(m.cpu_utilization for m in self.cluster_metrics.values()) / len(self.cluster_metrics) if self.cluster_metrics else 0,
                "avg_memory_utilization": sum(m.memory_utilization for m in self.cluster_metrics.values()) / len(self.cluster_metrics) if self.cluster_metrics else 0,
                "total_cost_per_hour": sum(m.cost_per_hour for m in self.cluster_metrics.values())
            }
        }

    async def add_cluster_group(self, group_data: Dict[str, Any]) -> str:
        """Add a new cluster group"""
        group = ClusterGroup(
            group_id=group_data.get("group_id", f"group_{datetime.utcnow().isoformat()}"),
            group_name=group_data["group_name"],
            cluster_names=group_data["cluster_names"],
            group_type=group_data["group_type"],
            policies=group_data.get("policies", []),
            load_balancing_strategy=group_data.get("load_balancing_strategy", "round_robin"),
            created_at=datetime.utcnow()
        )

        self.cluster_groups[group.group_id] = group
        logger.info(f"Added cluster group: {group.group_name}")
        return group.group_id

    def get_cluster_recommendations(self) -> List[Dict[str, Any]]:
        """Get cluster optimization recommendations"""
        recommendations = []

        for cluster_name, metrics in self.cluster_metrics.items():
            if metrics.cpu_utilization > 85:
                recommendations.append({
                    "cluster": cluster_name,
                    "type": "scale_up",
                    "priority": "high",
                    "description": f"High CPU utilization ({metrics.cpu_utilization:.1f}%)",
                    "action": "Add worker nodes"
                })

            if metrics.memory_utilization > 90:
                recommendations.append({
                    "cluster": cluster_name,
                    "type": "memory_optimization",
                    "priority": "high",
                    "description": f"High memory utilization ({metrics.memory_utilization:.1f}%)",
                    "action": "Add memory or optimize memory usage"
                })

            if metrics.cost_per_hour > 8.0 and metrics.efficiency_score < 0.6:
                recommendations.append({
                    "cluster": cluster_name,
                    "type": "cost_optimization",
                    "priority": "medium",
                    "description": f"High cost (${metrics.cost_per_hour:.2f}/hr) with low efficiency ({metrics.efficiency_score:.2f})",
                    "action": "Use spot instances or rightsize cluster"
                })

        return sorted(recommendations, key=lambda x: x["priority"], reverse=True)