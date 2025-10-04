"""
Data models for AI Spark Analyzer
"""

from datetime import datetime
from typing import Optional, List, Dict, Any
from enum import Enum
from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Spark job status enumeration"""
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    DONE = "DONE"
    ERROR = "ERROR"
    KILLED = "KILLED"
    UNKNOWN = "UNKNOWN"


class RecommendationType(str, Enum):
    """Types of recommendations"""
    COST_OPTIMIZATION = "cost_optimization"
    PERFORMANCE_IMPROVEMENT = "performance_improvement"
    RESOURCE_ALLOCATION = "resource_allocation"
    CODE_OPTIMIZATION = "code_optimization"
    CONFIGURATION_CHANGE = "configuration_change"
    SCHEDULING_OPTIMIZATION = "scheduling_optimization"


class SeverityLevel(str, Enum):
    """Severity levels for recommendations and alerts"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class SparkJob(BaseModel):
    """Spark job data model"""

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
    """Detailed job metrics"""

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
    """Dataproc cluster information"""

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
    """Performance or cost pattern identified by AI"""

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
    """AI-generated optimization recommendation"""

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
    """Long-term memory entry"""

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
    """30-day optimization plan"""

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
    """Daily analysis report"""

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
    """System alert or notification"""

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