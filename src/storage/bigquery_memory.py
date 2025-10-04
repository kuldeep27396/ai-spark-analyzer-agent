"""
BigQuery Long-Term Memory Storage
Managed tables for storing historical analysis data and patterns
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, asdict
import json
from pathlib import Path

from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError

from ..core.config import Config
from ..core.models import SparkJob, Pattern, Recommendation, OptimizationPlan

logger = logging.getLogger(__name__)


@dataclass
class BigQueryTableConfig:
    dataset_id: str
    table_id: str
    schema: List[dict]
    partition_field: Optional[str] = None
    cluster_fields: Optional[List[str]] = None


class BigQueryMemoryManager:
    """
    BigQuery-based long-term memory storage for AI Spark Analyzer
    """

    def __init__(self, config: Config):
        self.config = config
        self.client = bigquery.Client(project=config.gcp.project_id)

        # Table configurations
        self.tables = self._initialize_table_configs()

        # Dataset configuration
        self.dataset_id = f"{config.gcp.project_id}.spark_analyzer_memory"

        logger.info("BigQuery Memory Manager initialized")

    def _initialize_table_configs(self) -> Dict[str, BigQueryTableConfig]:
        """Initialize BigQuery table configurations"""

        return {
            "jobs": BigQueryTableConfig(
                dataset_id="spark_analyzer_memory",
                table_id="job_analysis",
                schema=[
                    {"name": "job_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "job_name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "cluster_name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "job_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "submit_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "start_time", "type": "TIMESTAMP"},
                    {"name": "finish_time", "type": "TIMESTAMP"},
                    {"name": "duration_seconds", "type": "INTEGER"},
                    {"name": "priority", "type": "STRING"},
                    {"name": "cost_estimate", "type": "FLOAT"},
                    {"name": "vcore_seconds", "type": "INTEGER"},
                    {"name": "memory_milliseconds", "type": "INTEGER"},
                    {"name": "input_bytes", "type": "INTEGER"},
                    {"name": "output_bytes", "type": "INTEGER"},
                    {"name": "cpu_utilization_avg", "type": "FLOAT"},
                    {"name": "memory_utilization_avg", "type": "FLOAT"},
                    {"name": "disk_utilization_avg", "type": "FLOAT"},
                    {"name": "success_rate", "type": "FLOAT"},
                    {"name": "error_count", "type": "INTEGER"},
                    {"name": "analysis_date", "type": "DATE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "metadata", "type": "JSON"}
                ],
                partition_field="analysis_date",
                cluster_fields=["cluster_name", "job_type"]
            ),

            "patterns": BigQueryTableConfig(
                dataset_id="spark_analyzer_memory",
                table_id="patterns",
                schema=[
                    {"name": "pattern_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "pattern_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "confidence_score", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "frequency", "type": "INTEGER", "mode": "REQUIRED"},
                    {"name": "severity", "type": "STRING"},
                    {"name": "impact_score", "type": "FLOAT"},
                    {"name": "job_ids", "type": "STRING", "mode": "REPEATED"},
                    {"name": "clusters", "type": "STRING", "mode": "REPEATED"},
                    {"name": "time_period_start", "type": "TIMESTAMP"},
                    {"name": "time_period_end", "type": "TIMESTAMP"},
                    {"name": "pattern_data", "type": "JSON"},
                    {"name": "analysis_date", "type": "DATE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ],
                partition_field="analysis_date",
                cluster_fields=["pattern_type", "severity"]
            ),

            "recommendations": BigQueryTableConfig(
                dataset_id="spark_analyzer_memory",
                table_id="recommendations",
                schema=[
                    {"name": "recommendation_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "job_ids", "type": "STRING", "mode": "REPEATED"},
                    {"name": "recommendation_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "title", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "impact_score", "type": "FLOAT", "mode": "REQUIRED"},
                    {"name": "implementation_effort", "type": "STRING"},
                    {"name": "priority", "type": "STRING"},
                    {"name": "severity", "type": "STRING"},
                    {"name": "cost_savings", "type": "FLOAT"},
                    {"name": "performance_improvement", "type": "FLOAT"},
                    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "applied_at", "type": "TIMESTAMP"},
                    {"name": "feedback_score", "type": "FLOAT"},
                    {"name": "feedback_comments", "type": "STRING"},
                    {"name": "recommendation_data", "type": "JSON"},
                    {"name": "analysis_date", "type": "DATE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ],
                partition_field="analysis_date",
                cluster_fields=["recommendation_type", "priority", "status"]
            ),

            "optimization_plans": BigQueryTableConfig(
                dataset_id="spark_analyzer_memory",
                table_id="optimization_plans",
                schema=[
                    {"name": "plan_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "plan_name", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "plan_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "description", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "period_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "period_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "total_jobs_analyzed", "type": "INTEGER"},
                    {"name": "estimated_cost_savings", "type": "FLOAT"},
                    {"name": "estimated_performance_improvement", "type": "FLOAT"},
                    {"name": "implementation_priority", "type": "STRING"},
                    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "plan_data", "type": "JSON"},
                    {"name": "analysis_date", "type": "DATE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    {"name": "updated_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ],
                partition_field="analysis_date",
                cluster_fields=["plan_type", "status"]
            ),

            "daily_summaries": BigQueryTableConfig(
                dataset_id="spark_analyzer_memory",
                table_id="daily_summaries",
                schema=[
                    {"name": "summary_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "summary_date", "type": "DATE", "mode": "REQUIRED"},
                    {"name": "total_jobs_analyzed", "type": "INTEGER"},
                    {"name": "successful_jobs", "type": "INTEGER"},
                    {"name": "failed_jobs", "type": "INTEGER"},
                    {"name": "total_cost", "type": "FLOAT"},
                    {"name": "total_runtime_hours", "type": "FLOAT"},
                    {"name": "patterns_identified", "type": "INTEGER"},
                    {"name": "recommendations_generated", "type": "INTEGER"},
                    {"name": "high_priority_issues", "type": "INTEGER"},
                    {"name": "clusters_analyzed", "type": "INTEGER"},
                    {"name": "avg_cpu_utilization", "type": "FLOAT"},
                    {"name": "avg_memory_utilization", "type": "FLOAT"},
                    {"name": "summary_data", "type": "JSON"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ],
                partition_field="summary_date"
            ),

            "job_feedback": BigQueryTableConfig(
                dataset_id="spark_analyzer_memory",
                table_id="job_feedback",
                schema=[
                    {"name": "feedback_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "job_id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "recommendation_id", "type": "STRING"},
                    {"name": "user_id", "type": "STRING"},
                    {"name": "feedback_type", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "feedback_score", "type": "FLOAT"},
                    {"name": "comments", "type": "STRING"},
                    {"name": "actual_outcome", "type": "JSON"},
                    {"name": "expected_outcome", "type": "JSON"},
                    {"name": "feedback_date", "type": "DATE"},
                    {"name": "created_at", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ],
                partition_field="feedback_date",
                cluster_fields=["feedback_type", "job_id"]
            )
        }

    async def initialize_dataset(self):
        """Initialize BigQuery dataset and tables"""
        try:
            # Create dataset if it doesn't exist
            dataset_ref = bigquery.DatasetReference(self.config.gcp.project_id, "spark_analyzer_memory")

            try:
                self.client.create_dataset(dataset_ref, exists_ok=True)
                logger.info("BigQuery dataset created/verified")
            except GoogleAPICallError as e:
                logger.error(f"Error creating dataset: {e}")
                raise

            # Create tables
            for table_name, table_config in self.tables.items():
                await self._create_table(table_config)

            logger.info("BigQuery tables initialized successfully")

        except Exception as e:
            logger.error(f"Error initializing BigQuery dataset: {e}")
            raise

    async def _create_table(self, table_config: BigQueryTableConfig):
        """Create a BigQuery table"""
        try:
            table_ref = f"{table_config.dataset_id}.{table_config.table_id}"

            # Build table reference
            table = bigquery.Table(table_ref, schema=table_config.schema)

            # Add partitioning
            if table_config.partition_field:
                table.time_partitioning = bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=table_config.partition_field
                )

            # Add clustering
            if table_config.cluster_fields:
                table.clustering_fields = table_config.cluster_fields

            # Create table
            self.client.create_table(table, exists_ok=True)
            logger.info(f"Table {table_config.table_id} created/verified")

        except Exception as e:
            logger.error(f"Error creating table {table_config.table_id}: {e}")
            raise

    async def store_jobs(self, jobs: List[SparkJob], analysis_date: datetime = None):
        """Store job analysis data in BigQuery"""
        if not jobs:
            return

        try:
            analysis_date = analysis_date or datetime.utcnow().date()

            # Prepare rows for insertion
            rows = []
            for job in jobs:
                row = {
                    "job_id": job.job_id,
                    "job_name": job.job_name,
                    "cluster_name": job.cluster_name,
                    "job_type": job.job_type,
                    "status": job.status,
                    "submit_time": job.submit_time,
                    "start_time": getattr(job, 'start_time', None),
                    "finish_time": getattr(job, 'finish_time', None),
                    "duration_seconds": job.duration_seconds,
                    "priority": job.priority,
                    "cost_estimate": job.cost_estimate,
                    "vcore_seconds": getattr(job, 'vcore_seconds', None),
                    "memory_milliseconds": getattr(job, 'memory_milliseconds', None),
                    "input_bytes": getattr(job, 'input_bytes', None),
                    "output_bytes": getattr(job, 'output_bytes', None),
                    "cpu_utilization_avg": getattr(job, 'cpu_utilization_avg', None),
                    "memory_utilization_avg": getattr(job, 'memory_utilization_avg', None),
                    "disk_utilization_avg": getattr(job, 'disk_utilization_avg', None),
                    "success_rate": getattr(job, 'success_rate', None),
                    "error_count": getattr(job, 'error_count', 0),
                    "analysis_date": analysis_date,
                    "created_at": datetime.utcnow(),
                    "updated_at": datetime.utcnow(),
                    "metadata": json.dumps(getattr(job, 'metadata', {}))
                }
                rows.append(row)

            # Insert into BigQuery
            table_ref = f"{self.tables['jobs'].dataset_id}.{self.tables['jobs'].table_id}"

            errors = self.client.insert_rows_json(table_ref, rows)
            if errors:
                logger.error(f"Errors inserting jobs: {errors}")
                raise Exception(f"BigQuery insertion errors: {errors}")

            logger.info(f"Successfully stored {len(jobs)} jobs in BigQuery")

        except Exception as e:
            logger.error(f"Error storing jobs in BigQuery: {e}")
            raise

    async def store_patterns(self, patterns: List[Pattern], analysis_date: datetime = None):
        """Store pattern analysis data in BigQuery"""
        if not patterns:
            return

        try:
            analysis_date = analysis_date or datetime.utcnow().date()

            rows = []
            for pattern in patterns:
                row = {
                    "pattern_id": pattern.pattern_id,
                    "pattern_type": pattern.pattern_type,
                    "description": pattern.description,
                    "confidence_score": pattern.confidence_score,
                    "frequency": pattern.frequency,
                    "severity": getattr(pattern, 'severity', None),
                    "impact_score": getattr(pattern, 'impact_score', None),
                    "job_ids": pattern.job_ids,
                    "clusters": pattern.clusters,
                    "time_period_start": getattr(pattern, 'time_period_start', None),
                    "time_period_end": getattr(pattern, 'time_period_end', None),
                    "pattern_data": json.dumps(getattr(pattern, 'pattern_data', {})),
                    "analysis_date": analysis_date,
                    "created_at": pattern.created_at,
                    "updated_at": datetime.utcnow()
                }
                rows.append(row)

            table_ref = f"{self.tables['patterns'].dataset_id}.{self.tables['patterns'].table_id}"

            errors = self.client.insert_rows_json(table_ref, rows)
            if errors:
                logger.error(f"Errors inserting patterns: {errors}")
                raise Exception(f"BigQuery insertion errors: {errors}")

            logger.info(f"Successfully stored {len(patterns)} patterns in BigQuery")

        except Exception as e:
            logger.error(f"Error storing patterns in BigQuery: {e}")
            raise

    async def store_recommendations(self, recommendations: List[Recommendation], analysis_date: datetime = None):
        """Store recommendation data in BigQuery"""
        if not recommendations:
            return

        try:
            analysis_date = analysis_date or datetime.utcnow().date()

            rows = []
            for rec in recommendations:
                row = {
                    "recommendation_id": rec.recommendation_id,
                    "job_ids": rec.job_ids,
                    "recommendation_type": rec.recommendation_type,
                    "title": rec.title,
                    "description": rec.description,
                    "impact_score": rec.impact_score,
                    "implementation_effort": rec.implementation_effort,
                    "priority": rec.priority,
                    "severity": rec.severity,
                    "cost_savings": rec.cost_savings,
                    "performance_improvement": getattr(rec, 'performance_improvement', None),
                    "status": rec.status,
                    "applied_at": getattr(rec, 'applied_at', None),
                    "feedback_score": getattr(rec, 'feedback_score', None),
                    "feedback_comments": getattr(rec, 'feedback_comments', None),
                    "recommendation_data": json.dumps(getattr(rec, 'recommendation_data', {})),
                    "analysis_date": analysis_date,
                    "created_at": rec.created_at,
                    "updated_at": datetime.utcnow()
                }
                rows.append(row)

            table_ref = f"{self.tables['recommendations'].dataset_id}.{self.tables['recommendations'].table_id}"

            errors = self.client.insert_rows_json(table_ref, rows)
            if errors:
                logger.error(f"Errors inserting recommendations: {errors}")
                raise Exception(f"BigQuery insertion errors: {errors}")

            logger.info(f"Successfully stored {len(recommendations)} recommendations in BigQuery")

        except Exception as e:
            logger.error(f"Error storing recommendations in BigQuery: {e}")
            raise

    async def store_daily_summary(self, summary_data: Dict[str, Any], summary_date: datetime = None):
        """Store daily analysis summary in BigQuery"""
        try:
            summary_date = summary_date or datetime.utcnow().date()

            row = {
                "summary_id": f"summary_{summary_date.isoformat()}",
                "summary_date": summary_date,
                "total_jobs_analyzed": summary_data.get("total_jobs_analyzed", 0),
                "successful_jobs": summary_data.get("successful_jobs", 0),
                "failed_jobs": summary_data.get("failed_jobs", 0),
                "total_cost": summary_data.get("total_cost", 0.0),
                "total_runtime_hours": summary_data.get("total_runtime_hours", 0.0),
                "patterns_identified": summary_data.get("patterns_identified", 0),
                "recommendations_generated": summary_data.get("recommendations_generated", 0),
                "high_priority_issues": summary_data.get("high_priority_issues", 0),
                "clusters_analyzed": summary_data.get("clusters_analyzed", 0),
                "avg_cpu_utilization": summary_data.get("avg_cpu_utilization", 0.0),
                "avg_memory_utilization": summary_data.get("avg_memory_utilization", 0.0),
                "summary_data": json.dumps(summary_data),
                "created_at": datetime.utcnow()
            }

            table_ref = f"{self.tables['daily_summaries'].dataset_id}.{self.tables['daily_summaries'].table_id}"

            errors = self.client.insert_rows_json(table_ref, [row])
            if errors:
                logger.error(f"Errors inserting daily summary: {errors}")
                raise Exception(f"BigQuery insertion errors: {errors}")

            logger.info(f"Successfully stored daily summary for {summary_date}")

        except Exception as e:
            logger.error(f"Error storing daily summary in BigQuery: {e}")
            raise

    async def store_feedback(self, feedback_data: Dict[str, Any]):
        """Store user feedback in BigQuery"""
        try:
            feedback_date = datetime.utcnow().date()

            row = {
                "feedback_id": feedback_data.get("feedback_id", f"feedback_{datetime.utcnow().isoformat()}"),
                "job_id": feedback_data["job_id"],
                "recommendation_id": feedback_data.get("recommendation_id"),
                "user_id": feedback_data.get("user_id"),
                "feedback_type": feedback_data["feedback_type"],
                "feedback_score": feedback_data.get("feedback_score"),
                "comments": feedback_data.get("comments"),
                "actual_outcome": json.dumps(feedback_data.get("actual_outcome", {})),
                "expected_outcome": json.dumps(feedback_data.get("expected_outcome", {})),
                "feedback_date": feedback_date,
                "created_at": datetime.utcnow()
            }

            table_ref = f"{self.tables['job_feedback'].dataset_id}.{self.tables['job_feedback'].table_id}"

            errors = self.client.insert_rows_json(table_ref, [row])
            if errors:
                logger.error(f"Errors inserting feedback: {errors}")
                raise Exception(f"BigQuery insertion errors: {errors}")

            logger.info(f"Successfully stored feedback for job {feedback_data['job_id']}")

        except Exception as e:
            logger.error(f"Error storing feedback in BigQuery: {e}")
            raise

    async def get_job_history(self, job_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get historical data for a specific job"""
        try:
            table_ref = f"{self.tables['jobs'].dataset_id}.{self.tables['jobs'].table_id}"

            query = f"""
                SELECT *
                FROM `{table_ref}`
                WHERE job_id = @job_id
                AND analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
                ORDER BY analysis_date DESC
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                    bigquery.ScalarQueryParameter("days", "INT64", days)
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting job history for {job_id}: {e}")
            raise

    async def get_30day_job_summary(self, job_id: str) -> Dict[str, Any]:
        """Get 30-day summary for a specific job"""
        try:
            # Get job history
            job_history = await self.get_job_history(job_id, days=30)

            if not job_history:
                return {"error": f"No data found for job {job_id} in the last 30 days"}

            # Get recommendations for this job
            recommendations = await self._get_job_recommendations(job_id, days=30)

            # Get patterns affecting this job
            patterns = await self._get_job_patterns(job_id, days=30)

            # Get feedback for this job
            feedback = await self._get_job_feedback(job_id, days=30)

            # Calculate summary statistics
            summary = {
                "job_id": job_id,
                "job_name": job_history[0].get("job_name", "Unknown"),
                "cluster_name": job_history[0].get("cluster_name", "Unknown"),
                "analysis_period": {
                    "start_date": (datetime.utcnow() - timedelta(days=30)).date().isoformat(),
                    "end_date": datetime.utcnow().date().isoformat()
                },
                "execution_summary": {
                    "total_executions": len(job_history),
                    "successful_executions": len([j for j in job_history if j.get("status") == "DONE"]),
                    "failed_executions": len([j for j in job_history if j.get("status") == "ERROR"]),
                    "success_rate": len([j for j in job_history if j.get("status") == "DONE"]) / len(job_history) * 100
                },
                "performance_summary": {
                    "avg_duration_seconds": sum(j.get("duration_seconds", 0) for j in job_history) / len(job_history),
                    "total_cost": sum(j.get("cost_estimate", 0) for j in job_history),
                    "avg_cpu_utilization": sum(j.get("cpu_utilization_avg", 0) for j in job_history if j.get("cpu_utilization_avg")) / len([j for j in job_history if j.get("cpu_utilization_avg")]) or 0,
                    "avg_memory_utilization": sum(j.get("memory_utilization_avg", 0) for j in job_history if j.get("memory_utilization_avg")) / len([j for j in job_history if j.get("memory_utilization_avg")]) or 0
                },
                "recommendations": {
                    "total_recommendations": len(recommendations),
                    "applied_recommendations": len([r for r in recommendations if r.get("status") == "applied"]),
                    "pending_recommendations": len([r for r in recommendations if r.get("status") == "pending"]),
                    "high_priority_recommendations": len([r for r in recommendations if r.get("priority") == "high"]),
                    "estimated_savings": sum(r.get("cost_savings", 0) for r in recommendations)
                },
                "patterns": {
                    "total_patterns": len(patterns),
                    "high_confidence_patterns": len([p for p in patterns if p.get("confidence_score", 0) > 0.8]),
                    "pattern_types": list(set(p.get("pattern_type") for p in patterns))
                },
                "feedback": {
                    "total_feedback": len(feedback),
                    "positive_feedback": len([f for f in feedback if f.get("feedback_score", 0) > 0]),
                    "negative_feedback": len([f for f in feedback if f.get("feedback_score", 0) < 0]),
                    "average_feedback_score": sum(f.get("feedback_score", 0) for f in feedback) / len(feedback) if feedback else 0
                },
                "trend_analysis": {
                    "cost_trend": self._calculate_trend([j.get("cost_estimate", 0) for j in job_history]),
                    "duration_trend": self._calculate_trend([j.get("duration_seconds", 0) for j in job_history]),
                    "success_rate_trend": self._calculate_success_rate_trend(job_history)
                },
                "recommendations_summary": recommendations[:5],  # Top 5 recommendations
                "generated_at": datetime.utcnow().isoformat()
            }

            return summary

        except Exception as e:
            logger.error(f"Error generating 30-day summary for job {job_id}: {e}")
            raise

    async def _get_job_recommendations(self, job_id: str, days: int) -> List[Dict[str, Any]]:
        """Get recommendations for a job"""
        try:
            table_ref = f"{self.tables['recommendations'].dataset_id}.{self.tables['recommendations'].table_id}"

            query = f"""
                SELECT *
                FROM `{table_ref}`
                WHERE @job_id IN UNNEST(job_ids)
                AND analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
                ORDER BY created_at DESC
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                    bigquery.ScalarQueryParameter("days", "INT64", days)
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting recommendations for job {job_id}: {e}")
            return []

    async def _get_job_patterns(self, job_id: str, days: int) -> List[Dict[str, Any]]:
        """Get patterns affecting a job"""
        try:
            table_ref = f"{self.tables['patterns'].dataset_id}.{self.tables['patterns'].table_id}"

            query = f"""
                SELECT *
                FROM `{table_ref}`
                WHERE @job_id IN UNNEST(job_ids)
                AND analysis_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
                ORDER BY confidence_score DESC
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                    bigquery.ScalarQueryParameter("days", "INT64", days)
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting patterns for job {job_id}: {e}")
            return []

    async def _get_job_feedback(self, job_id: str, days: int) -> List[Dict[str, Any]]:
        """Get feedback for a job"""
        try:
            table_ref = f"{self.tables['job_feedback'].dataset_id}.{self.tables['job_feedback'].table_id}"

            query = f"""
                SELECT *
                FROM `{table_ref}`
                WHERE job_id = @job_id
                AND feedback_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY)
                ORDER BY created_at DESC
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("job_id", "STRING", job_id),
                    bigquery.ScalarQueryParameter("days", "INT64", days)
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            return [dict(row) for row in results]

        except Exception as e:
            logger.error(f"Error getting feedback for job {job_id}: {e}")
            return []

    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend from values"""
        if len(values) < 2:
            return "insufficient_data"

        # Simple linear regression to determine trend
        n = len(values)
        x = list(range(n))

        # Calculate slope
        sum_x = sum(x)
        sum_y = sum(values)
        sum_xy = sum(x[i] * values[i] for i in range(n))
        sum_x2 = sum(x[i] ** 2 for i in range(n))

        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)

        if slope > 0.1:
            return "increasing"
        elif slope < -0.1:
            return "decreasing"
        else:
            return "stable"

    def _calculate_success_rate_trend(self, job_history: List[Dict[str, Any]]) -> str:
        """Calculate success rate trend over time"""
        if len(job_history) < 2:
            return "insufficient_data"

        # Sort by date
        sorted_history = sorted(job_history, key=lambda x: x.get("analysis_date"))

        # Calculate success rates in time windows
        window_size = max(1, len(sorted_history) // 3)
        success_rates = []

        for i in range(0, len(sorted_history), window_size):
            window = sorted_history[i:i + window_size]
            success_rate = len([j for j in window if j.get("status") == "DONE"]) / len(window) * 100
            success_rates.append(success_rate)

        return self._calculate_trend(success_rates)

    async def get_all_jobs_for_period(self, start_date: datetime, end_date: datetime) -> List[str]:
        """Get all unique job IDs for a period"""
        try:
            table_ref = f"{self.tables['jobs'].dataset_id}.{self.tables['jobs'].table_id}"

            query = f"""
                SELECT DISTINCT job_id
                FROM `{table_ref}`
                WHERE analysis_date BETWEEN @start_date AND @end_date
            """

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("start_date", "DATE", start_date.date()),
                    bigquery.ScalarQueryParameter("end_date", "DATE", end_date.date())
                ]
            )

            query_job = self.client.query(query, job_config=job_config)
            results = query_job.result()

            return [row["job_id"] for row in results]

        except Exception as e:
            logger.error(f"Error getting jobs for period: {e}")
            return []

    async def cleanup_old_data(self, retention_days: int = 365):
        """Clean up old data based on retention policy"""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)

            tables_to_cleanup = ["jobs", "patterns", "recommendations", "job_feedback"]

            for table_name in tables_to_cleanup:
                table_config = self.tables[table_name]
                table_ref = f"{table_config.dataset_id}.{table_config.table_id}"

                query = f"""
                    DELETE FROM `{table_ref}`
                    WHERE analysis_date < @cutoff_date
                """

                job_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("cutoff_date", "DATE", cutoff_date.date())
                    ]
                )

                query_job = self.client.query(query, job_config=job_config)
                query_job.result()

                logger.info(f"Cleaned up old data from {table_name} table")

        except Exception as e:
            logger.error(f"Error cleaning up old data: {e}")
            raise