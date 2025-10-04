"""
Configuration management for AI Spark Analyzer
"""

import os
import json
from typing import Optional, List, Dict, Any
from pydantic import BaseSettings, Field
from pydantic_settings import BaseSettings


class GoogleCloudConfig(BaseSettings):
    """Google Cloud configuration settings"""

    project_id: str = Field(..., env="GOOGLE_CLOUD_PROJECT")
    region: str = Field("us-central1", env="DATAPROC_REGION")
    credentials_path: Optional[str] = Field(None, env="GOOGLE_APPLICATION_CREDENTIALS")
    dataproc_cluster_label: str = Field("env", env="DATAPROC_CLUSTER_LABEL")

    # BigQuery configuration
    bigquery_dataset: str = Field("spark_analyzer_memory", env="BIGQUERY_DATASET")
    bigquery_location: str = Field("US", env="BIGQUERY_LOCATION")

    # GCS configuration
    gcs_bucket: str = Field("spark-analyzer-reports", env="GCS_BUCKET")
    gcs_reports_prefix: str = Field("reports/", env="GCS_REPORTS_PREFIX")

    class Config:
        env_prefix = "GCLOUD_"


class AIConfig(BaseSettings):
    """AI and ML configuration settings"""

    openai_api_key: str = Field(..., env="OPENAI_API_KEY")
    model: str = Field("gpt-4o", env="OPENAI_MODEL")
    temperature: float = Field(0.1, env="AI_TEMPERATURE")
    max_tokens: int = Field(4000, env="AI_MAX_TOKENS")
    timeout: int = Field(60, env="AI_TIMEOUT")
    retry_attempts: int = Field(3, env="AI_RETRY_ATTEMPTS")
    retry_delay: int = Field(5, env="AI_RETRY_DELAY")

    # LangGraph settings
    langgraph_api_key: Optional[str] = Field(None, env="LANGGRAPH_API_KEY")
    langgraph: Dict[str, Any] = Field(default_factory=dict)
    memory_retention_days: int = Field(90, env="MEMORY_RETENTION_DAYS")

    class Config:
        env_prefix = "AI_"


class DatabaseConfig(BaseSettings):
    """Database configuration settings"""

    database_url: str = Field("postgresql://localhost/spark_analyzer", env="DATABASE_URL")
    redis_url: str = Field("redis://localhost:6379", env="REDIS_URL")

    # Vector database for memory
    vector_db_path: str = Field("./data/vector_db", env="VECTOR_DB_PATH")
    vector_collection_name: str = Field("spark_memory", env="VECTOR_COLLECTION_NAME")

    class Config:
        env_prefix = "DB_"


class EmailConfig(BaseSettings):
    """Email configuration for notifications and summaries"""

    # SMTP configuration
    smtp_server: str = Field("smtp.gmail.com", env="SMTP_SERVER")
    smtp_port: int = Field(587, env="SMTP_PORT")
    use_tls: bool = Field(True, env="SMTP_USE_TLS")

    # Authentication
    username: str = Field(..., env="EMAIL_USERNAME")
    password: str = Field(..., env="EMAIL_PASSWORD")
    from_email: str = Field(..., env="EMAIL_FROM")

    # Recipients
    admin_emails: List[str] = Field(default_factory=list, env="EMAIL_ADMINS")
    notification_emails: List[str] = Field(default_factory=list, env="EMAIL_NOTIFICATIONS")

    # Email scheduling
    send_monthly_summaries: bool = Field(True, env="SEND_MONTHLY_SUMMARIES")
    monthly_summary_day: int = Field(1, env="MONTHLY_SUMMARY_DAY")  # 1st of each month

    # Email templates
    email_template_dir: str = Field("./templates/email", env="EMAIL_TEMPLATE_DIR")

    class Config:
        env_prefix = "EMAIL_"


class MonitoringConfig(BaseSettings):
    """Monitoring and alerting configuration"""

    prometheus_port: int = Field(8000, env="PROMETHEUS_PORT")

    # Slack notifications
    slack_webhook_url: Optional[str] = Field(None, env="SLACK_WEBHOOK_URL")
    slack_channel: str = Field("#spark-alerts", env="SLACK_CHANNEL")

    # Airflow configuration
    airflow_url: Optional[str] = Field(None, env="AIRFLOW_URL")
    airflow_dag_id: str = Field("ai_spark_analyzer_daily", env="AIRFLOW_DAG_ID")

    class Config:
        env_prefix = "MONITORING_"


class AnalysisConfig(BaseSettings):
    """Analysis configuration settings"""

    # Analysis frequency
    daily_analysis_hour: int = Field(9, env="DAILY_ANALYSIS_HOUR")
    recommendation_frequency_days: int = Field(30, env="RECOMMENDATION_FREQUENCY_DAYS")

    # Job filtering
    min_job_duration_seconds: int = Field(60, env="MIN_JOB_DURATION_SECONDS")
    failed_jobs_analysis: bool = Field(True, env="ANALYZE_FAILED_JOBS")

    # Cost analysis
    cost_analysis_enabled: bool = Field(True, env="COST_ANALYSIS_ENABLED")
    currency: str = Field("USD", env="CURRENCY")

    # Performance thresholds
    cpu_usage_threshold: float = Field(80.0, env="CPU_USAGE_THRESHOLD")
    memory_usage_threshold: float = Field(85.0, env="MEMORY_USAGE_THRESHOLD")
    disk_usage_threshold: float = Field(90.0, env="DISK_USAGE_THRESHOLD")

    class Config:
        env_prefix = "ANALYSIS_"


class DashboardConfig(BaseSettings):
    """Dashboard configuration settings"""

    host: str = Field("0.0.0.0", env="DASHBOARD_HOST")
    port: int = Field(8080, env="DASHBOARD_PORT")
    debug: bool = Field(False, env="DASHBOARD_DEBUG")
    secret_key: str = Field("your-secret-key-here", env="DASHBOARD_SECRET_KEY")

    class Config:
        env_prefix = "DASHBOARD_"


class Config(BaseSettings):
    """Main configuration class"""

    # Environment
    environment: str = Field("development", env="ENVIRONMENT")
    log_level: str = Field("INFO", env="LOG_LEVEL")
    name: str = Field("AI Spark Analyzer", env="SYSTEM_NAME")
    version: str = Field("2.0.0", env="SYSTEM_VERSION")
    max_workers: int = Field(10, env="MAX_WORKERS")

    # Sub-configurations
    google_cloud: GoogleCloudConfig = Field(default_factory=GoogleCloudConfig)
    gcp: GoogleCloudConfig = Field(default_factory=GoogleCloudConfig, alias="google_cloud")  # Alias for backward compatibility
    ai: AIConfig = Field(default_factory=AIConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    email: EmailConfig = Field(default_factory=EmailConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)
    analysis: AnalysisConfig = Field(default_factory=AnalysisConfig)
    dashboard: DashboardConfig = Field(default_factory=DashboardConfig)

    # Additional configs from JSON
    gcp_config: Dict[str, Any] = Field(default_factory=dict)
    analysis_config: Dict[str, Any] = Field(default_factory=dict)
    monitoring_config: Dict[str, Any] = Field(default_factory=dict)
    storage_config: Dict[str, Any] = Field(default_factory=dict)
    retention_config: Dict[str, Any] = Field(default_factory=dict)
    agents_config: Dict[str, Any] = Field(default_factory=dict)
    api_config: Dict[str, Any] = Field(default_factory=dict)
    cost_optimization_config: Dict[str, Any] = Field(default_factory=dict)

    # Data paths
    data_dir: str = Field("./data", env="DATA_DIR")
    log_dir: str = Field("./logs", env="LOG_DIR")
    output_dir: str = Field("./output", env="OUTPUT_DIR")

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._load_json_config()
        self._ensure_directories()

    def _load_json_config(self):
        """Load configuration from JSON file"""
        config_path = os.path.join(os.path.dirname(__file__), "../../config.json")
        if os.path.exists(config_path):
            try:
                with open(config_path, 'r') as f:
                    json_config = json.load(f)

                # Update configuration with JSON values
                if "system" in json_config:
                    self.name = json_config["system"].get("name", self.name)
                    self.version = json_config["system"].get("version", self.version)
                    self.environment = json_config["system"].get("environment", self.environment)
                    self.log_level = json_config["system"].get("log_level", self.log_level)
                    self.max_workers = json_config["system"].get("max_workers", self.max_workers)

                if "gcp" in json_config:
                    self.gcp_config = json_config["gcp"]

                if "ai" in json_config:
                    ai_config = json_config["ai"]
                    self.ai.model = ai_config.get("model", self.ai.model)
                    self.ai.max_tokens = ai_config.get("max_tokens", self.ai.max_tokens)
                    self.ai.temperature = ai_config.get("temperature", self.ai.temperature)
                    self.ai.timeout = ai_config.get("timeout", self.ai.timeout)
                    self.ai.retry_attempts = ai_config.get("retry_attempts", self.ai.retry_attempts)
                    self.ai.retry_delay = ai_config.get("retry_delay", self.ai.retry_delay)
                    self.ai.openai_api_key = ai_config.get("openai_api_key", self.ai.openai_api_key)
                    self.ai.langgraph = ai_config.get("langgraph", {})

                if "analysis" in json_config:
                    self.analysis_config = json_config["analysis"]

                if "monitoring" in json_config:
                    self.monitoring_config = json_config["monitoring"]

                if "storage" in json_config:
                    self.storage_config = json_config["storage"]

                if "retention" in json_config:
                    self.retention_config = json_config["retention"]

                if "agents" in json_config:
                    self.agents_config = json_config["agents"]

                if "api" in json_config:
                    self.api_config = json_config["api"]

                if "cost_optimization" in json_config:
                    self.cost_optimization_config = json_config["cost_optimization"]

                print(f"✅ Configuration loaded from {config_path}")

            except Exception as e:
                print(f"⚠️  Error loading JSON config: {e}")

    def _ensure_directories(self):
        """Ensure required directories exist"""
        directories = [
            self.data_dir,
            self.log_dir,
            self.output_dir,
            self.database.vector_db_path,
        ]

        for directory in directories:
            os.makedirs(directory, exist_ok=True)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Global configuration instance
config = Config()