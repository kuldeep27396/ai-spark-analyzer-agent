"""Configuration management for the AI Spark Analyzer.

This module defines the configuration structure for the application using
Pydantic's BaseSettings. It allows loading settings from environment
variables, a .env file, and a JSON configuration file, providing a
centralized and type-safe way to manage application settings.
"""

import os
import json
from typing import Optional, List, Dict, Any
from pydantic import BaseSettings, Field
from pydantic_settings import BaseSettings


class GoogleCloudConfig(BaseSettings):
    """Defines Google Cloud Platform (GCP) configuration settings.

    Attributes:
        project_id: The GCP project ID.
        region: The Dataproc region.
        credentials_path: The path to the GCP service account credentials file.
        dataproc_cluster_label: The label used to identify Dataproc clusters.
        bigquery_dataset: The BigQuery dataset for storing memory and analysis results.
        bigquery_location: The location of the BigQuery dataset.
        gcs_bucket: The GCS bucket for storing reports.
        gcs_reports_prefix: The prefix for report objects in the GCS bucket.
    """

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
    """Defines AI and Machine Learning (ML) configuration settings.

    Attributes:
        openai_api_key: The API key for OpenAI services.
        model: The model to use for AI-powered analysis (e.g., "gpt-4o").
        temperature: The sampling temperature for the AI model.
        max_tokens: The maximum number of tokens for AI model responses.
        timeout: The timeout for AI API requests in seconds.
        retry_attempts: The number of retry attempts for failed AI API requests.
        retry_delay: The delay between retry attempts in seconds.
        langgraph_api_key: The API key for LangGraph services.
        langgraph: A dictionary for LangGraph-specific settings.
        memory_retention_days: The number of days to retain AI memory.
    """

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
    """Defines database configuration settings.

    Attributes:
        database_url: The URL for the primary relational database (e.g., PostgreSQL).
        redis_url: The URL for the Redis cache.
        vector_db_path: The file path for the local vector database.
        vector_collection_name: The name of the collection in the vector database.
    """

    database_url: str = Field("postgresql://localhost/spark_analyzer", env="DATABASE_URL")
    redis_url: str = Field("redis://localhost:6379", env="REDIS_URL")

    # Vector database for memory
    vector_db_path: str = Field("./data/vector_db", env="VECTOR_DB_PATH")
    vector_collection_name: str = Field("spark_memory", env="VECTOR_COLLECTION_NAME")

    class Config:
        env_prefix = "DB_"


class EmailConfig(BaseSettings):
    """Defines email configuration for notifications and summaries.

    Attributes:
        smtp_server: The SMTP server hostname.
        smtp_port: The SMTP server port.
        use_tls: Whether to use TLS for the SMTP connection.
        username: The username for SMTP authentication.
        password: The password for SMTP authentication.
        from_email: The email address to send notifications from.
        admin_emails: A list of email addresses for admin notifications.
        notification_emails: A list of email addresses for general notifications.
        send_monthly_summaries: Whether to send monthly summary emails.
        monthly_summary_day: The day of the month to send summaries.
        email_template_dir: The directory containing email templates.
    """

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
    """Defines monitoring and alerting configuration.

    Attributes:
        prometheus_port: The port for the Prometheus metrics endpoint.
        slack_webhook_url: The webhook URL for Slack notifications.
        slack_channel: The Slack channel for sending alerts.
        airflow_url: The URL for the Airflow webserver.
        airflow_dag_id: The ID of the Airflow DAG for the analyzer.
    """

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
    """Defines analysis configuration settings.

    Attributes:
        daily_analysis_hour: The hour of the day to run daily analysis.
        recommendation_frequency_days: The frequency in days for generating new recommendations.
        min_job_duration_seconds: The minimum duration for a job to be analyzed.
        failed_jobs_analysis: Whether to analyze failed Spark jobs.
        cost_analysis_enabled: Whether to enable cost analysis features.
        currency: The currency for cost analysis (eg., "USD").
        cpu_usage_threshold: The CPU usage threshold for performance alerts.
        memory_usage_threshold: The memory usage threshold for performance alerts.
        disk_usage_threshold: The disk usage threshold for performance alerts.
    """

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
    """Defines dashboard configuration settings.

    Attributes:
        host: The hostname to bind the dashboard server to.
        port: The port to run the dashboard server on.
        debug: Whether to run the dashboard in debug mode.
        secret_key: The secret key for the dashboard application.
    """

    host: str = Field("0.0.0.0", env="DASHBOARD_HOST")
    port: int = Field(8080, env="DASHBOARD_PORT")
    debug: bool = Field(False, env="DASHBOARD_DEBUG")
    secret_key: str = Field("your-secret-key-here", env="DASHBOARD_SECRET_KEY")

    class Config:
        env_prefix = "DASHBOARD_"


class Config(BaseSettings):
    """The main configuration class for the AI Spark Analyzer.

    This class aggregates all other configuration classes and provides
    a single point of access for all settings. It also loads additional
    configuration from a JSON file and ensures necessary directories exist.

    Attributes:
        environment: The application environment (e.g., "development", "production").
        log_level: The logging level (e.g., "INFO", "DEBUG").
        name: The name of the system.
        version: The version of the system.
        max_workers: The maximum number of worker threads for concurrent tasks.
        google_cloud: The Google Cloud Platform configuration.
        gcp: An alias for the Google Cloud Platform configuration.
        ai: The AI and ML configuration.
        database: The database configuration.
        email: The email configuration.
        monitoring: The monitoring configuration.
        analysis: The analysis configuration.
        dashboard: The dashboard configuration.
        gcp_config: Additional GCP settings from the JSON file.
        analysis_config: Additional analysis settings from the JSON file.
        monitoring_config: Additional monitoring settings from the JSON file.
        storage_config: Additional storage settings from the JSON file.
        retention_config: Additional retention settings from the JSON file.
        agents_config: Additional agents settings from the JSON file.
        api_config: Additional API settings from the JSON file.
        cost_optimization_config: Additional cost optimization settings from the JSON file.
        data_dir: The base directory for data files.
        log_dir: The directory for log files.
        output_dir: The directory for output files.
    """

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
        """Loads configuration settings from a `config.json` file.

        This method reads the JSON configuration file, if it exists, and
        updates the application's settings with the values found. This
        allows for easy customization without changing environment variables.
        """
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
        """Ensures that all required data and log directories exist.

        This method checks for the existence of directories specified in the
        configuration and creates them if they are missing.
        """
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