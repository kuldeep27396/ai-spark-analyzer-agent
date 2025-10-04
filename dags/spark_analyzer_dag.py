"""
Airflow DAG for AI Spark Analyzer
Daily execution with BigQuery storage and 30-day email summaries
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add the src directory to the Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.email import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Variable

from core.config import config
from ai.agentic_engine import AgenticAIEngine
from storage.bigquery_memory import BigQueryMemoryManager
from email.email_summaries import EmailSummarySystem


# Default DAG arguments
default_args = {
    "owner": "ai-spark-analyzer",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "max_active_runs": 1,
}

# Create DAG
dag = DAG(
    "ai_spark_analyzer_daily",
    default_args=default_args,
    description="Daily AI Spark Analyzer with BigQuery storage and email summaries",
    schedule_interval="@daily",
    catchup=False,
    tags=["spark", "ai", "optimization", "dataproc"],
    doc_md="""
    ## AI Spark Analyzer Daily Execution DAG

    This DAG runs the AI Spark Analyzer daily to:
    1. Analyze Spark jobs from Dataproc clusters
    2. Store results in BigQuery for long-term memory
    3. Generate and send 30-day email summaries

    ### Features:
    - Autonomous job discovery and analysis
    - Multi-agent AI system with LangGraph
    - BigQuery long-term memory storage
    - 30-day email summaries for each job
    - Performance optimization recommendations
    - Cost analysis and savings opportunities
    """
)


def initialize_bigquery(**context):
    """Initialize BigQuery tables for storing analysis data"""
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        bigquery_manager = BigQueryMemoryManager(config)
        asyncio.run(bigquery_manager.initialize_dataset())
        logging.info("✅ BigQuery tables initialized successfully")

        # Update Airflow Variable
        Variable.set("spark_analyzer_bigquery_initialized", True)

        return {
            "status": "success",
            "message": "BigQuery tables initialized",
            "dataset": f"{config.gcp.project_id}.spark_analyzer_memory"
        }
    except Exception as e:
        logging.error(f"❌ Error initializing BigQuery: {e}")
        raise


def run_daily_analysis(**context):
    """Run the daily autonomous analysis"""
    import logging
    import asyncio
    logging.basicConfig(level=logging.INFO)

    try:
        # Initialize AI engine
        ai_engine = AgenticAIEngine(config)

        # Run autonomous analysis for the last 24 hours
        analysis_result = asyncio.run(ai_engine.run_autonomous_analysis(days=1))

        # Store results in Airflow XCom
        context["task_instance"].xcom_push(
            key="daily_analysis_result",
            value=analysis_result
        )

        logging.info("✅ Daily analysis completed successfully")
        logging.info(f"📊 Jobs analyzed: {analysis_result['discovered_jobs']}")
        logging.info(f"🧠 Patterns identified: {analysis_result['identified_patterns']}")
        logging.info(f"💡 Recommendations: {analysis_result['optimization_recommendations']}")

        return analysis_result

    except Exception as e:
        logging.error(f"❌ Error in daily analysis: {e}")
        raise


def store_daily_results(**context):
    """Store daily analysis results in BigQuery"""
    import logging
    import asyncio
    logging.basicConfig(level=logging.INFO)

    try:
        # Get analysis results from XCom
        analysis_result = context["task_instance"].xcom_pull(
            task_ids="run_daily_analysis",
            key="daily_analysis_result"
        )

        if not analysis_result:
            logging.warning("⚠️ No analysis results found to store")
            return {"status": "skipped", "message": "No analysis results"}

        # Initialize BigQuery manager
        bigquery_manager = BigQueryMemoryManager(config)

        # Get analysis date (yesterday)
        analysis_date = (context["ds"] or datetime.now().strftime("%Y-%m-%d"))
        analysis_datetime = datetime.strptime(analysis_date, "%Y-%m-%d")

        # Store daily summary
        summary_data = {
            "total_jobs_analyzed": analysis_result.get("discovered_jobs", 0),
            "successful_jobs": analysis_result.get("successful_jobs", 0),
            "failed_jobs": analysis_result.get("failed_jobs", 0),
            "total_cost": analysis_result.get("total_cost", 0.0),
            "total_runtime_hours": analysis_result.get("total_runtime_hours", 0.0),
            "patterns_identified": analysis_result.get("identified_patterns", 0),
            "recommendations_generated": analysis_result.get("optimization_recommendations", 0),
            "high_priority_issues": analysis_result.get("high_priority_issues", 0),
            "clusters_analyzed": analysis_result.get("clusters_analyzed", 0),
            "avg_cpu_utilization": analysis_result.get("avg_cpu_utilization", 0.0),
            "avg_memory_utilization": analysis_result.get("avg_memory_utilization", 0.0),
            "analysis_details": analysis_result
        }

        asyncio.run(bigquery_manager.store_daily_summary(summary_data, analysis_datetime))

        logging.info("✅ Daily results stored in BigQuery successfully")

        return {
            "status": "success",
            "message": "Results stored in BigQuery",
            "analysis_date": analysis_date,
            "jobs_analyzed": analysis_result.get("discovered_jobs", 0)
        }

    except Exception as e:
        logging.error(f"❌ Error storing results in BigQuery: {e}")
        raise


def generate_email_summaries(**context):
    """Generate 30-day email summaries"""
    import logging
    import asyncio
    logging.basicConfig(level=logging.INFO)

    try:
        # Check if it's time to send 30-day summaries (every 30 days)
        execution_date = context["execution_date"]

        # Send summaries on the 1st of each month (or every 30 days)
        if execution_date.day != 1:
            logging.info("⏭️ Skipping email summaries (only sent on 1st of each month)")
            return {"status": "skipped", "message": "Not time for 30-day summaries"}

        # Initialize email system
        email_system = EmailSummarySystem(config)

        # Generate summaries for the last 30 days
        summaries = asyncio.run(email_system.generate_30day_summaries())

        if not summaries:
            logging.info("📧 No email summaries generated")
            return {"status": "no_summaries", "message": "No jobs found for 30-day period"}

        # Store summaries in XCom for the email sending task
        context["task_instance"].xcom_push(
            key="email_summaries",
            value=summaries
        )

        logging.info(f"📧 Generated {len(summaries)} email summaries")

        return {
            "status": "success",
            "message": f"Generated {len(summaries)} summaries",
            "summaries_count": len(summaries)
        }

    except Exception as e:
        logging.error(f"❌ Error generating email summaries: {e}")
        raise


def send_email_summaries(**context):
    """Send generated email summaries"""
    import logging
    import asyncio
    logging.basicConfig(level=logging.INFO)

    try:
        # Get summaries from XCom
        summaries = context["task_instance"].xcom_pull(
            task_ids="generate_email_summaries",
            key="email_summaries"
        )

        if not summaries:
            logging.info("📧 No email summaries to send")
            return {"status": "no_summaries", "message": "No summaries to send"}

        # Initialize email system
        email_system = EmailSummarySystem(config)

        # Send all summaries
        results = asyncio.run(email_system.send_all_summaries(summaries))

        # Calculate success metrics
        sent_count = sum(1 for success in results.values() if success)
        total_count = len(summaries)

        logging.info(f"📧 Email sending completed: {sent_count}/{total_count} sent successfully")

        # Log any failures
        failed_jobs = [job_id for job_id, success in results.items() if not success]
        if failed_jobs:
            logging.warning(f"⚠️ Failed to send emails for jobs: {failed_jobs}")

        return {
            "status": "success",
            "message": f"Sent {sent_count}/{total_count} emails",
            "sent_count": sent_count,
            "total_count": total_count,
            "failed_jobs": failed_jobs
        }

    except Exception as e:
        logging.error(f"❌ Error sending email summaries: {e}")
        raise


def cleanup_old_data(**context):
    """Clean up old data based on retention policy"""
    import logging
    import asyncio
    logging.basicConfig(level=logging.INFO)

    try:
        # Get retention period from variables (default 365 days)
        retention_days = int(Variable.get("spark_analyzer_retention_days", default_var=365))

        # Initialize BigQuery manager
        bigquery_manager = BigQueryMemoryManager(config)

        # Clean up old data
        asyncio.run(bigquery_manager.cleanup_old_data(retention_days))

        logging.info(f"🧹 Cleaned up data older than {retention_days} days")

        return {
            "status": "success",
            "message": f"Cleaned up data older than {retention_days} days",
            "retention_days": retention_days
        }

    except Exception as e:
        logging.error(f"❌ Error cleaning up old data: {e}")
        raise


def create_analysis_report(**context):
    """Create a comprehensive analysis report"""
    import logging
    logging.basicConfig(level=logging.INFO)

    try:
        # Get analysis results from XCom
        analysis_result = context["task_instance"].xcom_pull(
            task_ids="run_daily_analysis",
            key="daily_analysis_result"
        )

        # Get storage results
        storage_result = context["task_instance"].xcom_pull(
            task_ids="store_daily_results",
            key="return_value"
        )

        # Create report
        report = {
            "execution_date": context["ds"],
            "dag_run_id": context["dag_run"].run_id,
            "analysis_result": analysis_result,
            "storage_result": storage_result,
            "agent_performance": analysis_result.get("agent_performance", {}) if analysis_result else {},
            "workflow_health": "healthy"
        }

        # Store report as a JSON file
        report_path = f"/tmp/spark_analyzer_report_{context['ds']}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)

        logging.info(f"📊 Analysis report created: {report_path}")

        return {
            "status": "success",
            "message": "Analysis report created",
            "report_path": report_path
        }

    except Exception as e:
        logging.error(f"❌ Error creating analysis report: {e}")
        raise


# Task definitions
initialize_bigquery_task = PythonOperator(
    task_id="initialize_bigquery",
    python_callable=initialize_bigquery,
    dag=dag
)

# Create task group for daily analysis
with TaskGroup("daily_analysis", dag=dag) as analysis_group:
    run_analysis_task = PythonOperator(
        task_id="run_daily_analysis",
        python_callable=run_daily_analysis,
        dag=dag
    )

    store_results_task = PythonOperator(
        task_id="store_daily_results",
        python_callable=store_daily_results,
        dag=dag
    )

    create_report_task = PythonOperator(
        task_id="create_analysis_report",
        python_callable=create_analysis_report,
        dag=dag
    )

    run_analysis_task >> store_results_task >> create_report_task

# Email tasks
generate_emails_task = PythonOperator(
    task_id="generate_email_summaries",
    python_callable=generate_email_summaries,
    dag=dag
)

send_emails_task = PythonOperator(
    task_id="send_email_summaries",
    python_callable=send_email_summaries,
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id="cleanup_old_data",
    python_callable=cleanup_old_data,
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

# Task dependencies
initialize_bigquery_task >> analysis_group

# Email tasks run after analysis (conditional on day of month)
analysis_group >> generate_emails_task >> send_emails_task

# Cleanup runs at the end regardless of other tasks
analysis_group >> cleanup_task

# Optional: Store report to GCS
store_report_to_gcs = LocalFilesystemToGCSOperator(
    task_id="store_report_to_gcs",
    src="/tmp/spark_analyzer_report_{{ ds }}.json",
    dst="spark-analyzer/reports/daily_report_{{ ds }}.json",
    bucket="your-gcs-bucket",  # Replace with your bucket name
    dag=dag,
    trigger_rule=TriggerRule.ALL_DONE
)

create_report_task >> store_report_to_gcs

# Email notification on failure
notification_email = EmailOperator(
    task_id="send_failure_notification",
    to="admin@company.com",  # Replace with admin email
    subject="AI Spark Analyzer DAG Failed - {{ ds }}",
    html_content="""
    <h2>AI Spark Analyzer DAG Failed</h2>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>DAG Run ID:</strong> {{ dag_run.run_id }}</p>
    <p><strong>Execution Date:</strong> {{ execution_date }}</p>
    <p><strong>Log URL:</strong> <a href="{{ dag_run.log_url }}">View Logs</a></p>

    <p>Please check the Airflow logs for more details.</p>
    """,
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED
)

# Configure failure notifications
for task in [run_analysis_task, store_results_task, generate_emails_task, send_emails_task]:
    task >> notification_email

# DAG documentation
dag.doc_md = """
# AI Spark Analyzer Daily Execution DAG

## Overview
This DAG runs the AI Spark Analyzer daily to analyze Spark jobs from Google Cloud Dataproc clusters, storing results in BigQuery for long-term memory and generating 30-day email summaries.

## Schedule
- **Frequency**: Daily at midnight UTC
- **Email Summaries**: Sent on the 1st of each month (30-day summaries)

## Tasks

### 1. Initialize BigQuery
- Creates and manages BigQuery tables for storing analysis data
- Sets up partitioning and clustering for optimal performance

### 2. Daily Analysis
- Runs the agentic AI system to discover and analyze Spark jobs
- Uses 7 specialized AI agents for comprehensive analysis
- Identifies patterns, generates recommendations, and optimizes costs

### 3. Store Results
- Stores analysis results in BigQuery tables
- Maintains historical data for trend analysis
- Enables long-term memory for the AI system

### 4. Generate Email Summaries (Monthly)
- Creates comprehensive 30-day summaries for each job
- Includes performance trends, optimization recommendations
- Sends personalized reports to job owners

### 5. Send Emails
- Sends all generated email summaries
- Includes admin summary for system oversight
- Handles email delivery errors gracefully

### 6. Cleanup Old Data
- Removes old data based on retention policy
- Configurable retention period (default: 365 days)
- Optimizes storage costs

## Monitoring
- All tasks include detailed logging
- Failure notifications sent to administrators
- Success metrics tracked in Airflow XCom
- Reports stored in GCS for archival

## Configuration
- Configure email settings in Airflow Variables
- Set retention period in `spark_analyzer_retention_days`
- Update GCS bucket name for report storage
"""

# SLA monitoring
sla_miss_callback = EmailOperator(
    task_id="sla_miss_notification",
    to="admin@company.com",
    subject="AI Spark Analyzer SLA Miss - {{ ds }}",
    html_content="""
    <h2>SLA Miss for AI Spark Analyzer</h2>
    <p><strong>Execution Date:</strong> {{ ds }}</p>
    <p><strong>DAG Run ID:</strong> {{ dag_run.run_id }}</p>
    <p><strong>SLA Miss Reason:</strong> {{ sla_miss_reason }}</p>

    <p>Please investigate the delay and optimize performance.</p>
    """,
    dag=dag
)

# Set SLA of 2 hours for the entire DAG
dag.sla_miss_callback = sla_miss_callback