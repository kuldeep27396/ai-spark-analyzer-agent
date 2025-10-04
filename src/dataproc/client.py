"""Client for interacting with the Google Cloud Dataproc API.

This module provides a client class for listing Dataproc jobs and clusters,
retrieving job metrics, and estimating costs. It simplifies the interaction
with the `google-cloud-dataproc` library and formats the results into
the application's data models.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path

from google.cloud import dataproc_v1
from google.cloud import monitoring_v3
from google.api_core import exceptions as gcp_exceptions
from google.auth import default
from google.oauth2 import service_account

from ..core.models import (
    SparkJob, JobMetrics, ClusterInfo, JobStatus
)
from ..core.config import GoogleCloudConfig

logger = logging.getLogger(__name__)


class DataprocClient:
    """A client for interacting with the Google Cloud Dataproc API.

    This class wraps the `google-cloud-dataproc` and `google-cloud-monitoring`
    clients to provide methods for fetching and managing Dataproc resources
    like jobs and clusters.

    Attributes:
        config: The Google Cloud configuration settings.
        job_client: A `dataproc_v1.JobControllerClient` instance.
        cluster_client: A `dataproc_v1.ClusterControllerClient` instance.
        monitoring_client: A `monitoring_v3.MetricServiceClient` instance.
        project_path: The full path string for the GCP project.
        region_path: The full path string for the Dataproc region.
    """

    def __init__(self, config: GoogleCloudConfig):
        """Initializes the DataprocClient.

        Args:
            config: A `GoogleCloudConfig` object containing the GCP settings.
        """
        self.config = config

        # Initialize Google Cloud clients
        credentials = None
        if config.credentials_path:
            credentials = service_account.Credentials.from_service_account_file(
                config.credentials_path
            )

        # Dataproc clients
        self.job_client = dataproc_v1.JobControllerClient(
            credentials=credentials,
            client_options={"api_endpoint": f"{config.region}-dataproc.googleapis.com:443"}
        )
        self.cluster_client = dataproc_v1.ClusterControllerClient(
            credentials=credentials,
            client_options={"api_endpoint": f"{config.region}-dataproc.googleapis.com:443"}
        )

        # Monitoring client
        self.monitoring_client = monitoring_v3.MetricServiceClient(credentials=credentials)

        # Project path
        self.project_path = f"projects/{config.project_id}"
        self.region_path = f"projects/{config.project_id}/regions/{config.region}"

        logger.info(f"Dataproc client initialized for project {config.project_id}, region {config.region}")

    async def get_recent_jobs(self, days: int = 7) -> List[SparkJob]:
        """Fetches recent Spark jobs from Dataproc.

        This method retrieves a list of Spark jobs that have completed within
        the specified number of days.

        Args:
            days: The number of days to look back for recent jobs.

        Returns:
            A list of `SparkJob` objects representing the recent jobs.

        Raises:
            gcp_exceptions.GoogleAPICallError: If an error occurs while
                communicating with the Dataproc API.
        """
        logger.info(f"Fetching jobs from the last {days} days")

        try:
            # Calculate time range
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(days=days)

            # Create job list request
            request = dataproc_v1.ListJobsRequest(
                parent=self.region_path,
                filter=self._create_time_filter(start_time, end_time)
            )

            # Get all jobs
            jobs = []
            page_result = self.job_client.list_jobs(request=request)

            async for job in self._process_job_pages(page_result):
                if self._should_include_job(job):
                    spark_job = await self._convert_to_spark_job(job)
                    jobs.append(spark_job)

            logger.info(f"Retrieved {len(jobs)} jobs")
            return jobs

        except gcp_exceptions.GoogleAPICallError as e:
            logger.error(f"Error fetching jobs from Dataproc: {e}")
            raise

    async def get_jobs_by_time_range(self, start_time: datetime, end_time: datetime) -> List[SparkJob]:
        """Fetches Spark jobs within a specific time range.

        Args:
            start_time: The start of the time range.
            end_time: The end of the time range.

        Returns:
            A list of `SparkJob` objects that ran within the time range.
        """
        request = dataproc_v1.ListJobsRequest(
            parent=self.region_path,
            filter=self._create_time_filter(start_time, end_time)
        )

        jobs = []
        page_result = self.job_client.list_jobs(request=request)

        async for job in self._process_job_pages(page_result):
            if self._should_include_job(job):
                spark_job = await self._convert_to_spark_job(job)
                jobs.append(spark_job)

        return jobs

    async def get_job_metrics(self, job_id: str) -> List[JobMetrics]:
        """Fetches detailed metrics for a specific job.

        This method retrieves performance metrics for a given job ID, including
        stage-level information and data from the Cloud Monitoring API.

        Args:
            job_id: The ID of the job to fetch metrics for.

        Returns:
            A list of `JobMetrics` objects, or an empty list if an error occurs.
        """
        try:
            # Get job details
            job_path = f"{self.region_path}/jobs/{job_id}"
            job = self.job_client.get_job(name=job_path)

            metrics = []

            # Process stages if available
            if hasattr(job, 'driver_output_resource_uri') and job.driver_output_resource_uri:
                # Parse driver output for stage information
                stage_metrics = await self._extract_stage_metrics(job)
                metrics.extend(stage_metrics)

            # Get additional metrics from Monitoring API
            monitoring_metrics = await self._get_monitoring_metrics(job_id)
            metrics.extend(monitoring_metrics)

            return metrics

        except gcp_exceptions.GoogleAPICallError as e:
            logger.error(f"Error getting metrics for job {job_id}: {e}")
            return []

    async def get_cluster_info(self, cluster_name: str) -> ClusterInfo:
        """Fetches detailed information about a specific cluster.

        Args:
            cluster_name: The name of the cluster to fetch information for.

        Returns:
            A `ClusterInfo` object containing the cluster's details.

        Raises:
            gcp_exceptions.GoogleAPICallError: If the cluster is not found or
                an API error occurs.
        """
        try:
            cluster_path = f"{self.region_path}/clusters/{cluster_name}"
            cluster = self.cluster_client.get_cluster(name=cluster_path)

            return ClusterInfo(
                cluster_name=cluster.cluster_name,
                cluster_uuid=cluster.cluster_uuid,
                project_id=self.config.project_id,
                region=self.config.region,
                status=cluster.status.name,
                cluster_config=dict(cluster.config.__dict__) if cluster.config else None,
                num_workers=cluster.config.worker_config.num_instances if cluster.config and cluster.config.worker_config else None,
                num_secondary_workers=cluster.config.secondary_worker_config.num_instances if cluster.config and cluster.config.secondary_worker_config else None,
                machine_type=cluster.config.worker_config.machine_type_uri if cluster.config and cluster.config.worker_config else None,
                secondary_machine_type=cluster.config.secondary_worker_config.machine_type_uri if cluster.config and cluster.config.secondary_worker_config else None,
                creation_timestamp=datetime.fromisoformat(cluster.status.create_time.replace('Z', '+00:00')) if cluster.status.create_time else None,
                total_cost_estimate=None  # Would need to calculate based on usage
            )

        except gcp_exceptions.GoogleAPICallError as e:
            logger.error(f"Error getting cluster info for {cluster_name}: {e}")
            raise

    async def _process_job_pages(self, page_result):
        """Asynchronously processes pages of job results from the Dataproc API.

        Args:
            page_result: An iterator of job pages from the `list_jobs` call.

        Yields:
            Individual job objects from the pages.
        """
        for page in page_result:
            yield page

    def _create_time_filter(self, start_time: datetime, end_time: datetime) -> str:
        """Creates a time-based filter string for Dataproc job queries.

        Args:
            start_time: The start of the time range.
            end_time: The end of the time range.

        Returns:
            A filter string for the Dataproc API.
        """
        start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        end_str = end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return f"status.state = DONE AND createTime >= {start_str} AND createTime <= {end_str}"

    def _should_include_job(self, job) -> bool:
        """Determines whether a job should be included in the analysis.

        This method filters out short-running jobs that are unlikely to be
        relevant for performance analysis.

        Args:
            job: A Dataproc job object.

        Returns:
            `True` if the job should be included, `False` otherwise.
        """
        # Filter out very short jobs or test jobs
        if hasattr(job, 'spark_job') and job.spark_job:
            if hasattr(job.spark_job, 'submission_time') and job.spark_job.submission_time:
                duration = self._calculate_job_duration(job)
                if duration and duration < 30:  # Less than 30 seconds
                    return False

        return True

    def _calculate_job_duration(self, job) -> Optional[float]:
        """Calculates the duration of a job in seconds.

        Args:
            job: A Dataproc job object.

        Returns:
            The duration of the job in seconds, or `None` if it cannot be calculated.
        """
        if not hasattr(job, 'spark_job') or not job.spark_job:
            return None

        spark_job = job.spark_job
        if not hasattr(spark_job, 'submission_time') or not spark_job.submission_time:
            return None

        try:
            submission_time = datetime.fromisoformat(spark_job.submission_time.replace('Z', '+00:00'))
            if hasattr(spark_job, 'completion_time') and spark_job.completion_time:
                completion_time = datetime.fromisoformat(spark_job.completion_time.replace('Z', '+00:00'))
                return (completion_time - submission_time).total_seconds()
        except (ValueError, AttributeError):
            pass

        return None

    async def _convert_to_spark_job(self, job) -> SparkJob:
        """Converts a Dataproc job object to a `SparkJob` data model.

        Args:
            job: A Dataproc job object from the API.

        Returns:
            A `SparkJob` object with the job's information.
        """
        spark_job = job.spark_job if hasattr(job, 'spark_job') else None

        # Extract basic information
        job_id = job.reference.job_id if hasattr(job, 'reference') and job.reference else str(job.uuid)
        application_id = spark_job.application_id if spark_job and hasattr(spark_job, 'application_id') else job_id
        cluster_name = job.placement.cluster_name if hasattr(job, 'placement') and job.placement else "unknown"
        cluster_uuid = job.placement.cluster_uuid if hasattr(job, 'placement') and job.placement else None

        # Parse timing information
        submit_time = None
        start_time = None
        finish_time = None

        if spark_job:
            if hasattr(spark_job, 'submission_time') and spark_job.submission_time:
                submit_time = datetime.fromisoformat(spark_job.submission_time.replace('Z', '+00:00'))
            if hasattr(spark_job, 'completion_time') and spark_job.completion_time:
                finish_time = datetime.fromisoformat(spark_job.completion_time.replace('Z', '+00:00'))

        # Convert status
        status_str = job.status.state.name if hasattr(job, 'status') and job.status else "UNKNOWN"
        try:
            status = JobStatus(status_str.lower())
        except ValueError:
            status = JobStatus.UNKNOWN

        # Extract resource usage
        vcore_seconds = None
        memory_milliseconds = None

        if spark_job and hasattr(spark_job, 'total_resource_usage'):
            usage = spark_job.total_resource_usage
            vcore_seconds = usage.vcore_seconds if hasattr(usage, 'vcore_seconds') else None
            memory_milliseconds = usage.memory_milliseconds if hasattr(usage, 'memory_milliseconds') else None

        return SparkJob(
            job_id=job_id,
            application_id=application_id,
            cluster_name=cluster_name,
            cluster_uuid=cluster_uuid,
            status=status,
            submit_time=submit_time or datetime.utcnow(),
            start_time=start_time,
            finish_time=finish_time,
            duration_seconds=self._calculate_job_duration(job),
            vcore_seconds=vcore_seconds,
            memory_milliseconds=memory_milliseconds,
            spark_user=spark_job.user if spark_job and hasattr(spark_job, 'user') else None,
            spark_version=spark_job.version if spark_job and hasattr(spark_job, 'version') else None,
            num_tasks=spark_job.num_tasks if spark_job and hasattr(spark_job, 'num_tasks') else None,
            num_completed_tasks=spark_job.num_completed_tasks if spark_job and hasattr(spark_job, 'num_completed_tasks') else None,
            num_failed_tasks=spark_job.num_failed_tasks if spark_job and hasattr(spark_job, 'num_failed_tasks') else None,
            job_tags=dict(job.labels) if hasattr(job, 'labels') and job.labels else None
        )

    async def _extract_stage_metrics(self, job) -> List[JobMetrics]:
        """Extracts stage-level metrics from the job's driver output.

        Note: This is a placeholder for a more complex implementation that would
        involve parsing Spark event logs or UI data.

        Args:
            job: A Dataproc job object.

        Returns:
            An empty list, as this functionality is not yet implemented.
        """
        # This would typically involve parsing the Spark UI output
        # For now, return empty list - in production you'd implement this
        return []

    async def _get_monitoring_metrics(self, job_id: str) -> List[JobMetrics]:
        """Fetches additional job metrics from the Cloud Monitoring API.

        Note: This is a placeholder for a more complex implementation that
        would query for specific Spark metrics.

        Args:
            job_id: The ID of the job to fetch monitoring metrics for.

        Returns:
            An empty list, as this functionality is not yet implemented.
        """
        try:
            # Create monitoring query
            project_name = f"projects/{self.config.project_id}"

            # Example: Get CPU utilization metrics
            interval = monitoring_v3.TimeInterval(
                {
                    "end_time": {"seconds": int(datetime.utcnow().timestamp())},
                    "start_time": {"seconds": int((datetime.utcnow() - timedelta(hours=24)).timestamp())}
                }
            )

            # This is a placeholder - in production you'd create specific queries
            # for Spark metrics like executor memory, shuffle metrics, etc.
            return []

        except Exception as e:
            logger.error(f"Error getting monitoring metrics for job {job_id}: {e}")
            return []

    async def get_active_clusters(self) -> List[ClusterInfo]:
        """Fetches a list of all active (running) Dataproc clusters.

        Returns:
            A list of `ClusterInfo` objects for all active clusters, or an
            empty list if an error occurs.
        """
        try:
            request = dataproc_v1.ListClustersRequest(
                parent=self.region_path,
                filter="status.state = RUNNING"
            )

            clusters = []
            page_result = self.cluster_client.list_clusters(request=request)

            for cluster in page_result:
                cluster_info = ClusterInfo(
                    cluster_name=cluster.cluster_name,
                    cluster_uuid=cluster.cluster_uuid,
                    project_id=self.config.project_id,
                    region=self.config.region,
                    status=cluster.status.name,
                    cluster_config=dict(cluster.config.__dict__) if cluster.config else None,
                    num_workers=cluster.config.worker_config.num_instances if cluster.config and cluster.config.worker_config else None,
                    num_secondary_workers=cluster.config.secondary_worker_config.num_instances if cluster.config and cluster.config.secondary_worker_config else None,
                    machine_type=cluster.config.worker_config.machine_type_uri if cluster.config and cluster.config.worker_config else None,
                    creation_timestamp=datetime.fromisoformat(cluster.status.create_time.replace('Z', '+00:00')) if cluster.status.create_time else None
                )
                clusters.append(cluster_info)

            return clusters

        except gcp_exceptions.GoogleAPICallError as e:
            logger.error(f"Error getting active clusters: {e}")
            return []

    async def get_cluster_cost_estimate(self, cluster_name: str, days: int = 30) -> Optional[float]:
        """Estimates the cost of a cluster over a given number of days.

        Note: This provides a simplified cost estimate based on machine types
        and does not use the Cloud Billing API.

        Args:
            cluster_name: The name of the cluster.
            days: The number of days to estimate the cost for.

        Returns:
            The estimated cost in USD, or `None` if an error occurs.
        """
        try:
            # This would typically involve querying Cloud Billing API
            # For now, return a basic calculation based on machine types and duration
            cluster_info = await self.get_cluster_info(cluster_name)

            if not cluster_info.creation_timestamp:
                return None

            # Calculate duration in days
            duration = datetime.utcnow() - cluster_info.creation_timestamp
            duration_days = min(duration.total_seconds() / 86400, days)

            # Basic cost calculation (simplified)
            # In production, you'd use actual Cloud Billing data
            cost_per_day = self._estimate_cluster_daily_cost(cluster_info)
            return round(cost_per_day * duration_days, 2)

        except Exception as e:
            logger.error(f"Error estimating cluster cost: {e}")
            return None

    def _estimate_cluster_daily_cost(self, cluster_info: ClusterInfo) -> float:
        """Estimates the daily cost of a cluster based on its configuration.

        This is a simplified calculation using example pricing and does not
        reflect actual GCP costs.

        Args:
            cluster_info: A `ClusterInfo` object for the cluster.

        Returns:
            The estimated daily cost in USD.
        """
        # This is a simplified calculation
        # In production, you'd use actual GCP pricing

        # Basic machine costs (example rates)
        n1_standard_cost_per_hour = 0.0475  # n1-standard-1
        n1_highmem_cost_per_hour = 0.0950    # n1-highmem-2

        daily_cost = 0.0

        # Master node cost (assume n1-standard-1)
        daily_cost += n1_standard_cost_per_hour * 24

        # Worker nodes
        if cluster_info.num_workers and cluster_info.machine_type:
            if "highmem" in cluster_info.machine_type:
                worker_cost_per_hour = n1_highmem_cost_per_hour
            else:
                worker_cost_per_hour = n1_standard_cost_per_hour

            daily_cost += worker_cost_per_hour * cluster_info.num_workers * 24

        # Secondary workers (preemptible)
        if cluster_info.num_secondary_workers and cluster_info.secondary_machine_type:
            # Preemptible instances are typically 60-80% cheaper
            preemptible_discount = 0.7
            if "highmem" in cluster_info.secondary_machine_type:
                secondary_cost_per_hour = n1_highmem_cost_per_hour * (1 - preemptible_discount)
            else:
                secondary_cost_per_hour = n1_standard_cost_per_hour * (1 - preemptible_discount)

            daily_cost += secondary_cost_per_hour * cluster_info.num_secondary_workers * 24

        return daily_cost