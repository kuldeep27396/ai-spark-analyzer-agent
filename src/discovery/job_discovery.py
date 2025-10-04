"""
Dynamic Job Discovery and Onboarding System
Autonomous discovery and onboarding of Dataproc Spark jobs across multiple clusters
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import json

from ..core.config import Config
from ..core.models import SparkJob, Cluster
from ..dataproc.client import DataprocClient
from ..ai.agentic_engine import AgenticAIEngine

logger = logging.getLogger(__name__)


class JobType(Enum):
    BATCH = "batch"
    STREAMING = "streaming"
    INTERACTIVE = "interactive"
    SCHEDULED = "scheduled"
    AD_HOC = "ad_hoc"


class OnboardingStatus(Enum):
    PENDING = "pending"
    DISCOVERED = "discovered"
    ANALYZING = "analyzing"
    ONBOARDED = "onboarded"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class JobDiscoveryResult:
    job_id: str
    cluster_name: str
    job_name: str
    job_type: JobType
    discovery_timestamp: datetime
    onboarding_status: OnboardingStatus
    priority_score: float
    business_impact: str
    resource_profile: Dict[str, Any]
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ClusterProfile:
    cluster_name: str
    cluster_type: str
    region: str
    status: str
    job_count: int
    last_discovery: datetime
    discovery_patterns: Dict[str, Any] = field(default_factory=dict)
    onboarding_history: List[str] = field(default_factory=list)


class JobDiscoveryManager:
    """
    Dynamic job discovery manager for autonomous Dataproc job onboarding
    """

    def __init__(self, config: Config, ai_engine: AgenticAIEngine):
        self.config = config
        self.ai_engine = ai_engine
        self.dataproc_client = DataprocClient(config)

        # Discovery state
        self.discovered_clusters: Dict[str, ClusterProfile] = {}
        self.discovered_jobs: Dict[str, JobDiscoveryResult] = {}
        self.onboarding_queue: asyncio.Queue = asyncio.Queue()
        self.processing_jobs: Set[str] = set()

        # Discovery configuration
        self.discovery_config = {
            "scan_interval_minutes": 30,
            "max_jobs_per_cluster": 100,
            "priority_threshold": 0.7,
            "auto_onboard_high_priority": True,
            "retention_days": 90,
            "discovery_depth": "shallow"  # shallow, medium, deep
        }

        # Statistics
        self.discovery_stats = {
            "total_discoveries": 0,
            "successful_onboardings": 0,
            "failed_onboardings": 0,
            "last_scan": None,
            "clusters_monitored": 0
        }

        logger.info("Job Discovery Manager initialized")

    async def start_continuous_discovery(self):
        """Start continuous job discovery process"""
        logger.info("Starting continuous job discovery")

        while True:
            try:
                await self._discovery_cycle()
                await asyncio.sleep(self.discovery_config["scan_interval_minutes"] * 60)
            except Exception as e:
                logger.error(f"Error in discovery cycle: {e}")
                await asyncio.sleep(60)  # Wait before retrying

    async def _discovery_cycle(self):
        """Execute one discovery cycle"""
        logger.info("Starting discovery cycle")

        # 1. Discover clusters
        await self._discover_clusters()

        # 2. Discover jobs from clusters
        await self._discover_jobs_from_clusters()

        # 3. Prioritize and queue jobs for onboarding
        await self._prioritize_and_queue_jobs()

        # 4. Process onboarding queue
        await self._process_onboarding_queue()

        # 5. Update statistics
        self._update_discovery_stats()

        logger.info(f"Discovery cycle completed. Jobs discovered: {len(self.discovered_jobs)}")

    async def _discover_clusters(self):
        """Discover and profile Dataproc clusters"""
        try:
            clusters = await self.dataproc_client.list_clusters()

            for cluster in clusters:
                if cluster.cluster_name not in self.discovered_clusters:
                    # Create cluster profile
                    profile = ClusterProfile(
                        cluster_name=cluster.cluster_name,
                        cluster_type=cluster.cluster_type or "unknown",
                        region=cluster.region or "unknown",
                        status=cluster.status,
                        job_count=0,
                        last_discovery=datetime.utcnow()
                    )

                    self.discovered_clusters[cluster.cluster_name] = profile
                    logger.info(f"Discovered new cluster: {cluster.cluster_name}")
                else:
                    # Update existing cluster profile
                    self.discovered_clusters[cluster.cluster_name].status = cluster.status
                    self.discovered_clusters[cluster.cluster_name].last_discovery = datetime.utcnow()

        except Exception as e:
            logger.error(f"Error discovering clusters: {e}")

    async def _discover_jobs_from_clusters(self):
        """Discover jobs from all monitored clusters"""
        for cluster_name, cluster_profile in self.discovered_clusters.items():
            if cluster_profile.status in ["RUNNING", "ACTIVE"]:
                await self._discover_cluster_jobs(cluster_name)

    async def _discover_cluster_jobs(self, cluster_name: str):
        """Discover jobs from a specific cluster"""
        try:
            logger.info(f"Discovering jobs from cluster: {cluster_name}")

            # Get jobs from the cluster
            jobs_data = await self.dataproc_client.get_cluster_jobs(
                cluster_name,
                days=7,  # Look back 7 days
                limit=self.discovery_config["max_jobs_per_cluster"]
            )

            for job_data in jobs_data:
                job_id = job_data.get("job_id")
                if job_id and job_id not in self.discovered_jobs:
                    # Create job discovery result
                    discovery_result = await self._create_job_discovery_result(
                        job_data, cluster_name
                    )

                    if discovery_result:
                        self.discovered_jobs[job_id] = discovery_result
                        self.discovery_stats["total_discoveries"] += 1

            # Update cluster job count
            cluster_profile.job_count = len([j for j in self.discovered_jobs.values()
                                           if j.cluster_name == cluster_name])

        except Exception as e:
            logger.error(f"Error discovering jobs from cluster {cluster_name}: {e}")

    async def _create_job_discovery_result(self, job_data: Dict[str, Any], cluster_name: str) -> Optional[JobDiscoveryResult]:
        """Create job discovery result from raw job data"""
        try:
            # Determine job type
            job_type = self._classify_job_type(job_data)

            # Calculate priority score
            priority_score = self._calculate_priority_score(job_data, job_type)

            # Assess business impact
            business_impact = self._assess_business_impact(job_data, priority_score)

            # Create resource profile
            resource_profile = self._create_resource_profile(job_data)

            # Extract metadata
            metadata = self._extract_job_metadata(job_data)

            return JobDiscoveryResult(
                job_id=job_data.get("job_id"),
                cluster_name=cluster_name,
                job_name=job_data.get("job_name", "Unknown Job"),
                job_type=job_type,
                discovery_timestamp=datetime.utcnow(),
                onboarding_status=OnboardingStatus.DISCOVERED,
                priority_score=priority_score,
                business_impact=business_impact,
                resource_profile=resource_profile,
                metadata=metadata
            )

        except Exception as e:
            logger.error(f"Error creating job discovery result: {e}")
            return None

    def _classify_job_type(self, job_data: Dict[str, Any]) -> JobType:
        """Classify job type based on job characteristics"""
        job_name = job_data.get("job_name", "").lower()
        job_type = job_data.get("type", "").lower()

        # Check for streaming jobs
        if any(keyword in job_name for keyword in ["stream", "real-time", "continuous"]) or \
           "streaming" in job_type:
            return JobType.STREAMING

        # Check for scheduled jobs
        if any(keyword in job_name for keyword in ["scheduled", "daily", "hourly", "batch"]) or \
           job_data.get("scheduled"):
            return JobType.SCHEDULED

        # Check for interactive jobs
        if any(keyword in job_name for keyword in ["interactive", "notebook", "adhoc"]) or \
           "interactive" in job_type:
            return JobType.INTERACTIVE

        # Default to batch
        return JobType.BATCH

    def _calculate_priority_score(self, job_data: Dict[str, Any], job_type: JobType) -> float:
        """Calculate priority score for job onboarding"""
        score = 0.5  # Base score

        # Job type weighting
        type_weights = {
            JobType.STREAMING: 0.9,
            JobType.SCHEDULED: 0.8,
            JobType.BATCH: 0.6,
            JobType.INTERACTIVE: 0.4,
            JobType.AD_HOC: 0.3
        }
        score += type_weights.get(job_type, 0.5) * 0.3

        # Resource usage weighting
        resource_usage = job_data.get("resource_usage", {})
        if resource_usage.get("cpu_cores", 0) > 50:
            score += 0.2
        if resource_usage.get("memory_gb", 0) > 100:
            score += 0.2

        # Frequency weighting
        if job_data.get("frequency", "").lower() in ["hourly", "continuous"]:
            score += 0.15
        elif job_data.get("frequency", "").lower() in ["daily", "weekly"]:
            score += 0.1

        # Business criticality
        if job_data.get("criticality", "").lower() == "critical":
            score += 0.25
        elif job_data.get("criticality", "").lower() == "high":
            score += 0.15

        return min(score, 1.0)

    def _assess_business_impact(self, job_data: Dict[str, Any], priority_score: float) -> str:
        """Assess business impact of the job"""
        if priority_score >= 0.8:
            return "critical"
        elif priority_score >= 0.6:
            return "high"
        elif priority_score >= 0.4:
            return "medium"
        else:
            return "low"

    def _create_resource_profile(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create resource usage profile"""
        resource_usage = job_data.get("resource_usage", {})

        return {
            "cpu_cores": resource_usage.get("cpu_cores", 0),
            "memory_gb": resource_usage.get("memory_gb", 0),
            "disk_gb": resource_usage.get("disk_gb", 0),
            "network_io": resource_usage.get("network_io", 0),
            "estimated_duration_hours": job_data.get("duration_hours", 0),
            "estimated_cost": job_data.get("cost_estimate", 0.0),
            "resource_efficiency": self._calculate_resource_efficiency(resource_usage)
        }

    def _calculate_resource_efficiency(self, resource_usage: Dict[str, Any]) -> str:
        """Calculate resource efficiency rating"""
        # Simple efficiency calculation based on resource utilization
        cpu_util = resource_usage.get("cpu_utilization", 0.5)
        memory_util = resource_usage.get("memory_utilization", 0.5)

        avg_util = (cpu_util + memory_util) / 2

        if avg_util >= 0.8:
            return "excellent"
        elif avg_util >= 0.6:
            return "good"
        elif avg_util >= 0.4:
            return "fair"
        else:
            return "poor"

    def _extract_job_metadata(self, job_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract relevant metadata from job data"""
        return {
            "submit_time": job_data.get("submit_time"),
            "user": job_data.get("user", "unknown"),
            "department": job_data.get("department", "unknown"),
            "application": job_data.get("application", "unknown"),
            "environment": job_data.get("environment", "production"),
            "tags": job_data.get("tags", []),
            "dependencies": job_data.get("dependencies", []),
            "success_rate": job_data.get("success_rate", 1.0),
            "avg_duration": job_data.get("avg_duration", 0),
            "data_volume_gb": job_data.get("data_volume_gb", 0)
        }

    async def _prioritize_and_queue_jobs(self):
        """Prioritize discovered jobs and add to onboarding queue"""
        # Filter jobs that need onboarding
        pending_jobs = [
            job for job in self.discovered_jobs.values()
            if job.onboarding_status == OnboardingStatus.DISCOVERED and
               job.job_id not in self.processing_jobs
        ]

        # Sort by priority score
        pending_jobs.sort(key=lambda x: x.priority_score, reverse=True)

        # Add high priority jobs to queue
        for job in pending_jobs:
            if job.priority_score >= self.discovery_config["priority_threshold"]:
                await self.onboarding_queue.put(job)
                job.onboarding_status = OnboardingStatus.ANALYZING
                self.processing_jobs.add(job.job_id)

                logger.info(f"Queued job for onboarding: {job.job_name} (priority: {job.priority_score:.2f})")

    async def _process_onboarding_queue(self):
        """Process jobs in the onboarding queue"""
        max_concurrent = 5  # Process up to 5 jobs concurrently
        processing_tasks = []

        while not self.onboarding_queue.empty() and len(processing_tasks) < max_concurrent:
            job = await self.onboarding_queue.get()
            task = asyncio.create_task(self._onboard_job(job))
            processing_tasks.append(task)

        # Wait for current batch to complete
        if processing_tasks:
            await asyncio.gather(*processing_tasks, return_exceptions=True)

    async def _onboard_job(self, job: JobDiscoveryResult):
        """Onboard a single job"""
        try:
            logger.info(f"Onboarding job: {job.job_name}")

            # Convert discovery result to SparkJob model
            spark_job = SparkJob(
                job_id=job.job_id,
                job_name=job.job_name,
                cluster_name=job.cluster_name,
                job_type=job.job_type.value,
                status="DISCOVERED",
                submit_time=job.discovery_timestamp,
                priority=self._priority_score_to_priority(job.priority_score),
                cost_estimate=job.resource_profile.get("estimated_cost", 0.0)
            )

            # Use AI engine to analyze the job
            analysis_result = await self.ai_engine.run_autonomous_analysis(days=1)

            if "error" not in analysis_result:
                job.onboarding_status = OnboardingStatus.ONBOARDED
                self.discovery_stats["successful_onboardings"] += 1

                # Add to cluster onboarding history
                if job.cluster_name in self.discovered_clusters:
                    self.discovered_clusters[job.cluster_name].onboarding_history.append(job.job_id)

                logger.info(f"Successfully onboarded job: {job.job_name}")
            else:
                job.onboarding_status = OnboardingStatus.FAILED
                self.discovery_stats["failed_onboardings"] += 1
                logger.error(f"Failed to onboard job: {job.job_name} - {analysis_result['error']}")

        except Exception as e:
            job.onboarding_status = OnboardingStatus.FAILED
            self.discovery_stats["failed_onboardings"] += 1
            logger.error(f"Error onboarding job {job.job_name}: {e}")

        finally:
            # Remove from processing set
            self.processing_jobs.discard(job.job_id)

    def _priority_score_to_priority(self, score: float) -> str:
        """Convert priority score to priority string"""
        if score >= 0.8:
            return "critical"
        elif score >= 0.6:
            return "high"
        elif score >= 0.4:
            return "medium"
        else:
            return "low"

    def _update_discovery_stats(self):
        """Update discovery statistics"""
        self.discovery_stats["last_scan"] = datetime.utcnow()
        self.discovery_stats["clusters_monitored"] = len(self.discovered_clusters)
        self.discovery_stats["jobs_pending_onboarding"] = len([
            job for job in self.discovered_jobs.values()
            if job.onboarding_status == OnboardingStatus.DISCOVERED
        ])

    async def manual_discovery(self, cluster_names: List[str] = None) -> Dict[str, Any]:
        """Manually trigger discovery for specific clusters"""
        logger.info("Starting manual job discovery")

        if cluster_names is None:
            # Discover from all clusters
            await self._discover_clusters()
            cluster_names = list(self.discovered_clusters.keys())

        discovery_results = {}

        for cluster_name in cluster_names:
            try:
                await self._discover_cluster_jobs(cluster_name)
                discovery_results[cluster_name] = {
                    "status": "success",
                    "jobs_discovered": len([j for j in self.discovered_jobs.values()
                                          if j.cluster_name == cluster_name])
                }
            except Exception as e:
                discovery_results[cluster_name] = {
                    "status": "error",
                    "error": str(e)
                }

        # Process immediate onboarding for high priority jobs
        await self._prioritize_and_queue_jobs()

        return {
            "discovery_results": discovery_results,
            "total_jobs_discovered": len(self.discovered_jobs),
            "high_priority_jobs": len([
                j for j in self.discovered_jobs.values()
                if j.priority_score >= self.discovery_config["priority_threshold"]
            ])
        }

    async def get_discovery_status(self) -> Dict[str, Any]:
        """Get current discovery status"""
        return {
            "discovery_stats": {
                **self.discovery_stats,
                "last_scan": self.discovery_stats["last_scan"].isoformat() if self.discovery_stats["last_scan"] else None
            },
            "clusters": {
                name: {
                    "cluster_type": profile.cluster_type,
                    "status": profile.status,
                    "job_count": profile.job_count,
                    "last_discovery": profile.last_discovery.isoformat(),
                    "onboarded_jobs": len(profile.onboarding_history)
                }
                for name, profile in self.discovered_clusters.items()
            },
            "jobs": {
                "total_discovered": len(self.discovered_jobs),
                "pending_onboarding": len([
                    j for j in self.discovered_jobs.values()
                    if j.onboarding_status == OnboardingStatus.DISCOVERED
                ]),
                "analyzing": len([
                    j for j in self.discovered_jobs.values()
                    if j.onboarding_status == OnboardingStatus.ANALYZING
                ]),
                "onboarded": len([
                    j for j in self.discovered_jobs.values()
                    if j.onboarding_status == OnboardingStatus.ONBOARDED
                ]),
                "failed": len([
                    j for j in self.discovered_jobs.values()
                    if j.onboarding_status == OnboardingStatus.FAILED
                ])
            },
            "queue_status": {
                "queue_size": self.onboarding_queue.qsize(),
                "processing_jobs": len(self.processing_jobs)
            }
        }

    def configure_discovery(self, config: Dict[str, Any]):
        """Configure discovery parameters"""
        self.discovery_config.update(config)
        logger.info(f"Discovery configuration updated: {config}")

    def get_high_priority_jobs(self, limit: int = 10) -> List[JobDiscoveryResult]:
        """Get high priority jobs for immediate attention"""
        return sorted(
            [job for job in self.discovered_jobs.values()
             if job.onboarding_status in [OnboardingStatus.DISCOVERED, OnboardingStatus.ANALYZING]],
            key=lambda x: x.priority_score,
            reverse=True
        )[:limit]