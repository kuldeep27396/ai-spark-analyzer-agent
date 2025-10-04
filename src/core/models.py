"""Data models for the AI Spark Analyzer.

This module defines the Pydantic data models used throughout the application
to represent core entities such as Spark jobs, clusters, recommendations,
and analysis reports. These models ensure data consistency and provide
type validation.
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Enumeration for the status of a Spark job."""
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ERROR = "ERROR"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"


class RecommendationType(str, Enum):
    """Enumeration for the types of recommendations the system can generate."""
    COST_OPTIMIZATION = "cost_optimization"
    PERFORMANCE_IMPROVEMENT = "performance_improvement"
    RESOURCE_ALLOCATION = "resource_allocation"
    CODE_OPTIMIZATION = "code_optimization"
    CONFIGURATION_CHANGE = "configuration_change"
    SCHEDULING_OPTIMIZATION = "scheduling_optimization"


class SeverityLevel(str, Enum):
    """Enumeration for severity levels used in recommendations and alerts."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SparkJob(BaseModel):
    """Represents a single Spark job and its associated metadata.

    Attributes:
        job_id: The unique identifier for the job.
        application_id: The YARN or Spark application ID.
        cluster_name: The name of the cluster where the job ran.
        cluster_uuid: The unique identifier of the cluster.
        job_name: The name of the Spark job.
        status: The final status of the job.
        submit_time: The time the job was submitted.
        start_time: The time the job started execution.
        finish_time: The time the job finished execution.
        duration_seconds: The total duration of the job in seconds.
        vcore_seconds: The aggregated vcore-seconds consumed by the job.
        memory_milliseconds: The aggregated memory-milliseconds consumed.
        bytes_read: The total bytes read by the job.
        bytes_written: The total bytes written by the job.
        records_read: The total records read by the job.
        records_written: The total records written by the job.
        spark_user: The user who submitted the job.
        spark_version: The Spark version used.
        queue: The YARN queue the job was submitted to.
        num_tasks: The total number of tasks in the job.
        num_active_tasks: The number of currently active tasks.
        num_completed_tasks: The number of completed tasks.
        num_failed_tasks: The number of failed tasks.
        num_killed_tasks: The number of killed tasks.
        job_tags: A dictionary of tags associated with the job.
        properties: A dictionary of Spark properties for the job.
        success_rate: The calculated success rate of tasks.
        cost_estimate: The estimated cost of the job in USD.
    """

    # Basic job information
    job_id: str
    application_id: str
    cluster_name: str
    cluster_uuid: Optional[str] = None
    job_name: Optional[str] = None
    status: JobStatus

    # Timing information
    submit_time: datetime
    start_time: Optional[datetime] = None
    finish_time: Optional[datetime] = None
    duration_seconds: Optional[float] = None

    # Resource usage
    vcore_seconds: Optional[float] = None
    memory_milliseconds: Optional[float] = None
    bytes_read: Optional[int] = None
    bytes_written: Optional[int] = None
    records_read: Optional[int] = None
    records_written: Optional[int] = None

    # Spark specific
    spark_user: Optional[str] = None
    spark_version: Optional[str] = None
    queue: Optional[str] = None

    # Performance metrics
    num_tasks: Optional[int] = None
    num_active_tasks: Optional[int] = None
    num_completed_tasks: Optional[int] = None
    num_failed_tasks: Optional[int] = None
    num_killed_tasks: Optional[int] = None

    # Additional metadata
    job_tags: Optional[Dict[str, Any]] = None
    properties: Optional[Dict[str, Any]] = None

    # Calculated fields
    success_rate: Optional[float] = Field(None, description="Task success rate")
    cost_estimate: Optional[float] = Field(None, description="Estimated cost in USD")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class JobMetrics(BaseModel):
    """Represents detailed, stage-level metrics for a Spark job.

    Attributes:
        stage_id: The ID of the Spark stage.
        stage_attempt_id: The attempt ID of the stage.
        stage_name: The name of the stage.
        num_tasks: The number of tasks in the stage.
        num_active_tasks: The number of active tasks in the stage.
        num_completed_tasks: The number of completed tasks in the stage.
        num_failed_tasks: The number of failed tasks in the stage.
        num_killed_tasks: The number of killed tasks in the stage.
        executor_run_time: The total executor runtime in milliseconds.
        executor_cpu_time: The total executor CPU time in nanoseconds.
        executor_deserialize_cpu_time: CPU time spent on deserialization.
        executor_deserialize_time: Time spent on deserialization.
        executor_runtime: The total executor runtime.
        shuffle_read_bytes: Bytes read during shuffle operations.
        shuffle_read_records: Records read during shuffle operations.
        shuffle_write_bytes: Bytes written during shuffle operations.
        shuffle_write_records: Records written during shuffle operations.
        input_bytes: Bytes read from input sources.
        input_records: Records read from input sources.
        output_bytes: Bytes written to output sinks.
        output_records: Records written to output sinks.
        memory_bytes_spilled: Bytes spilled to memory.
        disk_bytes_spilled: Bytes spilled to disk.
        submission_time: The time the stage was submitted.
        completion_time: The time the stage was completed.
        stage_duration: The duration of the stage in seconds.
    """

    # Stage metrics
    stage_id: int
    stage_attempt_id: int
    stage_name: str
    num_tasks: int
    num_active_tasks: int
    num_completed_tasks: int
    num_failed_tasks: int
    num_killed_tasks: int

    # Resource usage per stage
    executor_run_time: Optional[int] = None
    executor_cpu_time: Optional[int] = None
    executor_deserialize_cpu_time: Optional[int] = None
    executor_deserialize_time: Optional[int] = None
    executor_runtime: Optional[int] = None

    # Shuffle metrics
    shuffle_read_bytes: Optional[int] = None
    shuffle_read_records: Optional[int] = None
    shuffle_write_bytes: Optional[int] = None
    shuffle_write_records: Optional[int] = None

    # Input/output metrics
    input_bytes: Optional[int] = None
    input_records: Optional[int] = None
    output_bytes: Optional[int] = None
    output_records: Optional[int] = None

    # Memory metrics
    memory_bytes_spilled: Optional[int] = None
    disk_bytes_spilled: Optional[int] = None

    # Timing
    submission_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None
    stage_duration: Optional[float] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class ClusterInfo(BaseModel):
    """Represents information about a Dataproc cluster.

    Attributes:
        cluster_name: The name of the cluster.
        cluster_uuid: The unique identifier for the cluster.
        project_id: The GCP project ID where the cluster resides.
        region: The region of the cluster.
        status: The current status of the cluster.
        cluster_config: A dictionary containing the cluster's configuration.
        num_workers: The number of primary worker nodes.
        num_secondary_workers: The number of secondary (preemptible) worker nodes.
        machine_type: The machine type of the primary workers.
        secondary_machine_type: The machine type of the secondary workers.
        creation_timestamp: The time the cluster was created.
        deletion_timestamp: The time the cluster was deleted.
        total_cost_estimate: The estimated total cost of the cluster.
    """

    cluster_name: str
    cluster_uuid: str
    project_id: str
    region: str
    status: str

    # Configuration
    cluster_config: Optional[Dict[str, Any]] = None

    # Resource allocation
    num_workers: Optional[int] = None
    num_secondary_workers: Optional[int] = None
    machine_type: Optional[str] = None
    secondary_machine_type: Optional[str] = None

    # Cost information
    creation_timestamp: Optional[datetime] = None
    deletion_timestamp: Optional[datetime] = None
    total_cost_estimate: Optional[float] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class Pattern(BaseModel):
    """Represents a performance or cost pattern identified by the AI.

    Attributes:
        pattern_id: The unique identifier for the pattern.
        pattern_type: The type of pattern (e.g., "data_skew", "inefficient_join").
        description: A description of the pattern.
        confidence_score: The AI's confidence in the identified pattern.
        frequency: The number of times this pattern was observed.
        job_ids: A list of job IDs where this pattern was found.
        clusters: A list of clusters where this pattern was observed.
        time_period: The time period over which the pattern was observed.
        root_cause: The AI's analysis of the root cause.
        impact_assessment: The AI's assessment of the pattern's impact.
        created_at: The timestamp when the pattern was first identified.
    """

    pattern_id: str
    pattern_type: str
    description: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    frequency: int = Field(description="Number of times this pattern was observed")

    # Pattern details
    job_ids: List[str] = Field(default_factory=list)
    clusters: List[str] = Field(default_factory=list)
    time_period: Optional[Dict[str, datetime]] = None

    # AI analysis
    root_cause: Optional[str] = None
    impact_assessment: Optional[str] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class Recommendation(BaseModel):
    """Represents an AI-generated optimization recommendation.

    Attributes:
        recommendation_id: The unique identifier for the recommendation.
        type: The type of recommendation.
        title: A short title for the recommendation.
        description: A detailed description of the recommendation.
        severity: The severity level of the issue being addressed.
        estimated_cost_savings: The estimated cost savings in USD.
        estimated_performance_improvement: The estimated performance improvement percentage.
        implementation_difficulty: The estimated difficulty of implementation.
        target_jobs: A list of job IDs this recommendation applies to.
        target_clusters: A list of clusters this recommendation applies to.
        target_users: A list of users this recommendation is relevant for.
        implementation_steps: A list of steps to implement the recommendation.
        code_changes: A dictionary of suggested code changes.
        configuration_changes: A dictionary of suggested configuration changes.
        reasoning: The AI's reasoning behind the recommendation.
        confidence_score: The AI's confidence in the recommendation's effectiveness.
        supporting_evidence: A list of evidence supporting the recommendation.
        created_at: The timestamp when the recommendation was created.
        expires_at: The timestamp when the recommendation expires.
        status: The current status of the recommendation.
    """

    recommendation_id: str
    type: RecommendationType
    title: str
    description: str
    severity: SeverityLevel

    # Impact assessment
    estimated_cost_savings: Optional[float] = None
    estimated_performance_improvement: Optional[float] = None
    implementation_difficulty: Optional[str] = None

    # Target information
    target_jobs: List[str] = Field(default_factory=list)
    target_clusters: List[str] = Field(default_factory=list)
    target_users: List[str] = Field(default_factory=list)

    # Implementation details
    implementation_steps: List[str] = Field(default_factory=list)
    code_changes: Optional[Dict[str, Any]] = None
    configuration_changes: Optional[Dict[str, Any]] = None

    # AI analysis
    reasoning: str
    confidence_score: float = Field(ge=0.0, le=1.0)
    supporting_evidence: List[str] = Field(default_factory=list)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: Optional[datetime] = None
    status: str = Field(default="pending", description="pending, accepted, rejected, implemented")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class MemoryEntry(BaseModel):
    """Represents an entry in the system's long-term memory.

    Attributes:
        entry_id: The unique identifier for the memory entry.
        entry_type: The type of the memory entry (e.g., "job", "pattern").
        content: The content of the memory entry.
        context: Additional context associated with the entry.
        importance_score: The importance score of the entry.
        relevance_score: The relevance score of the entry for a given query.
        access_count: The number of times the entry has been accessed.
        last_accessed: The timestamp of the last access.
        created_at: The timestamp when the entry was created.
        updated_at: The timestamp when the entry was last updated.
        embedding: The vector embedding for similarity searches.
    """

    entry_id: str
    entry_type: str
    content: Dict[str, Any]
    context: Optional[Dict[str, Any]] = None

    # Memory metadata
    importance_score: float = Field(ge=0.0, le=1.0)
    relevance_score: float = Field(ge=0.0, le=1.0)
    access_count: int = Field(default=0)
    last_accessed: Optional[datetime] = None

    # Temporal information
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = None

    # Vector embedding for similarity search
    embedding: Optional[List[float]] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class OptimizationPlan(BaseModel):
    """Represents a 30-day optimization plan.

    Attributes:
        plan_id: The unique identifier for the plan.
        plan_name: The name of the optimization plan.
        description: A description of the plan's goals.
        period_start: The start date of the plan.
        period_end: The end date of the plan.
        total_recommendations: The total number of recommendations in the plan.
        estimated_cost_savings: The total estimated cost savings from the plan.
        estimated_performance_improvement: The total estimated performance improvement.
        recommendations: A list of recommendations included in the plan.
        phases: A list of implementation phases for the plan.
        success_metrics: A dictionary of metrics to measure the plan's success.
        risk_assessment: The AI's assessment of risks associated with the plan.
        dependencies: A list of dependencies for the plan.
        created_at: The timestamp when the plan was created.
        last_updated: The timestamp when the plan was last updated.
        status: The current status of the plan.
    """

    plan_id: str
    plan_name: str
    description: str
    period_start: datetime
    period_end: datetime

    # Plan metrics
    total_recommendations: int
    estimated_cost_savings: float
    estimated_performance_improvement: float

    # Recommendations included
    recommendations: List[Recommendation] = Field(default_factory=list)

    # Implementation phases
    phases: List[Dict[str, Any]] = Field(default_factory=list)

    # Success criteria
    success_metrics: Dict[str, Any] = Field(default_factory=dict)

    # AI analysis
    risk_assessment: Optional[str] = None
    dependencies: List[str] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=datetime.utcnow)
    last_updated: Optional[datetime] = None
    status: str = Field(default="draft", description="draft, active, completed, cancelled")

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class AnalysisReport(BaseModel):
    """Represents a daily analysis report.

    Attributes:
        report_id: The unique identifier for the report.
        report_date: The date the report was generated for.
        analysis_period: The time period covered by the analysis.
        total_jobs_analyzed: The total number of jobs analyzed.
        successful_jobs: The number of successful jobs.
        failed_jobs: The number of failed jobs.
        total_cost_estimate: The total estimated cost of the analyzed jobs.
        patterns_identified: A list of patterns identified in the report.
        recommendations_generated: A list of recommendations generated.
        average_job_duration: The average duration of jobs in the period.
        resource_utilization_metrics: A dictionary of key resource utilization metrics.
        anomalies: A list of anomalies detected during the analysis.
        summary_insights: A high-level summary of the AI's insights.
        key_opportunities: A list of key optimization opportunities.
        created_at: The timestamp when the report was created.
    """

    report_id: str
    report_date: datetime
    analysis_period: Dict[str, datetime]

    # Summary statistics
    total_jobs_analyzed: int
    successful_jobs: int
    failed_jobs: int
    total_cost_estimate: float

    # Key findings
    patterns_identified: List[Pattern] = Field(default_factory=list)
    recommendations_generated: List[Recommendation] = Field(default_factory=list)

    # Performance metrics
    average_job_duration: Optional[float] = None
    resource_utilization_metrics: Dict[str, float] = Field(default_factory=dict)

    # Anomalies detected
    anomalies: List[Dict[str, Any]] = Field(default_factory=list)

    # AI insights
    summary_insights: str
    key_opportunities: List[str] = Field(default_factory=list)

    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }


class Alert(BaseModel):
    """Represents a system alert or notification.

    Attributes:
        alert_id: The unique identifier for the alert.
        alert_type: The type of alert (e.g., "performance_degradation").
        severity: The severity level of the alert.
        title: A title for the alert.
        message: The alert message.
        target_jobs: A list of job IDs related to the alert.
        target_clusters: A list of clusters related to the alert.
        source: The system component that generated the alert.
        category: The category of the alert (e.g., "cost", "performance").
        resolved: A boolean indicating if the alert has been resolved.
        resolved_at: The timestamp when the alert was resolved.
        created_at: The timestamp when the alert was created.
    """

    alert_id: str
    alert_type: str
    severity: SeverityLevel
    title: str
    message: str

    # Target information
    target_jobs: List[str] = Field(default_factory=list)
    target_clusters: List[str] = Field(default_factory=list)

    # Alert metadata
    source: str
    category: str
    resolved: bool = Field(default=False)
    resolved_at: Optional[datetime] = None

    created_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat() if v else None
        }