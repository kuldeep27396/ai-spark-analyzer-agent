"""
Email Summary System for 30-Day Job Analysis
Generates and sends comprehensive email summaries for each job every 30 days
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import json
import io

from ..core.config import Config
from ..storage.bigquery_memory import BigQueryMemoryManager

logger = logging.getLogger(__name__)


@dataclass
class EmailConfig:
    smtp_server: str
    smtp_port: int
    username: str
    password: str
    use_tls: bool = True
    from_email: str = None
    admin_emails: List[str] = None


@dataclass
class JobSummaryEmail:
    job_id: str
    recipient_email: str
    subject: str
    html_content: str
    text_content: str
    attachments: List[Dict[str, Any]] = None
    cc_emails: List[str] = None
    bcc_emails: List[str] = None


class EmailSummarySystem:
    """
    Email summary system for 30-day job analysis reports
    """

    def __init__(self, config: Config):
        self.config = config
        self.bigquery_manager = BigQueryMemoryManager(config)
        self.email_config = self._initialize_email_config()

        # Email templates
        self.templates = self._initialize_templates()

        logger.info("Email Summary System initialized")

    def _initialize_email_config(self) -> EmailConfig:
        """Initialize email configuration"""
        return EmailConfig(
            smtp_server=getattr(config.email, 'smtp_server', 'smtp.gmail.com'),
            smtp_port=getattr(config.email, 'smtp_port', 587),
            username=getattr(config.email, 'username', ''),
            password=getattr(config.email, 'password', ''),
            use_tls=getattr(config.email, 'use_tls', True),
            from_email=getattr(config.email, 'from_email', 'ai-spark-analyzer@company.com'),
            admin_emails=getattr(config.email, 'admin_emails', ['admin@company.com'])
        )

    def _initialize_templates(self) -> Dict[str, str]:
        """Initialize email templates"""
        return {
            "job_summary_html": """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>30-Day Job Analysis Summary</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
                    .container { max-width: 800px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .header { text-align: center; margin-bottom: 30px; padding-bottom: 20px; border-bottom: 2px solid #4285f4; }
                    .title { color: #333; font-size: 24px; margin: 0; }
                    .subtitle { color: #666; font-size: 16px; margin: 10px 0 0 0; }
                    .section { margin-bottom: 25px; }
                    .section-title { color: #4285f4; font-size: 18px; margin-bottom: 15px; border-left: 4px solid #4285f4; padding-left: 10px; }
                    .metric-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 15px; }
                    .metric-card { background-color: #f8f9fa; padding: 15px; border-radius: 6px; text-align: center; }
                    .metric-value { font-size: 24px; font-weight: bold; color: #333; margin: 0; }
                    .metric-label { font-size: 14px; color: #666; margin: 5px 0 0 0; }
                    .trend { font-size: 12px; padding: 2px 6px; border-radius: 3px; }
                    .trend-positive { background-color: #d4edda; color: #155724; }
                    .trend-negative { background-color: #f8d7da; color: #721c24; }
                    .trend-stable { background-color: #fff3cd; color: #856404; }
                    .recommendation-list { list-style: none; padding: 0; }
                    .recommendation-item { background-color: #e8f0fe; padding: 12px; margin-bottom: 10px; border-radius: 6px; border-left: 3px solid #4285f4; }
                    .recommendation-title { font-weight: bold; color: #4285f4; margin: 0 0 5px 0; }
                    .recommendation-desc { color: #333; margin: 0; font-size: 14px; }
                    .recommendation-impact { color: #666; font-size: 12px; margin: 5px 0 0 0; }
                    .footer { text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; color: #666; font-size: 12px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1 class="title">30-Day Job Analysis Summary</h1>
                        <p class="subtitle">Comprehensive performance and optimization insights for your Spark job</p>
                    </div>

                    <div class="section">
                        <h2 class="section-title">Job Information</h2>
                        <div class="metric-grid">
                            <div class="metric-card">
                                <p class="metric-value">{job_name}</p>
                                <p class="metric-label">Job Name</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{cluster_name}</p>
                                <p class="metric-label">Cluster</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{total_executions}</p>
                                <p class="metric-label">Total Executions</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{success_rate:.1f}%</p>
                                <p class="metric-label">Success Rate</p>
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h2 class="section-title">Performance Summary</h2>
                        <div class="metric-grid">
                            <div class="metric-card">
                                <p class="metric-value">${total_cost:.2f}</p>
                                <p class="metric-label">Total Cost (30 Days)</p>
                                <span class="trend trend-{cost_trend_class}">{cost_trend_icon} {cost_trend}</span>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{avg_duration:.0f}s</p>
                                <p class="metric-label">Avg Duration</p>
                                <span class="trend trend-{duration_trend_class}">{duration_trend_icon} {duration_trend}</span>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{avg_cpu_utilization:.0f}%</p>
                                <p class="metric-label">Avg CPU Usage</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{avg_memory_utilization:.0f}%</p>
                                <p class="metric-label">Avg Memory Usage</p>
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h2 class="section-title">Optimization Opportunities</h2>
                        <div class="metric-grid">
                            <div class="metric-card">
                                <p class="metric-value">{total_recommendations}</p>
                                <p class="metric-label">Total Recommendations</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">${estimated_savings:.2f}</p>
                                <p class="metric-label">Estimated Savings</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{applied_recommendations}</p>
                                <p class="metric-label">Applied Recommendations</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{high_priority_count}</p>
                                <p class="metric-label">High Priority</p>
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h2 class="section-title">Top Recommendations</h2>
                        {top_recommendations}
                    </div>

                    <div class="section">
                        <h2 class="section-title">Patterns & Insights</h2>
                        <p><strong>Total Patterns Identified:</strong> {total_patterns}</p>
                        <p><strong>High Confidence Patterns:</strong> {high_confidence_patterns}</p>
                        <p><strong>Pattern Types:</strong> {pattern_types_list}</p>
                    </div>

                    <div class="section">
                        <h2 class="section-title">User Feedback</h2>
                        <div class="metric-grid">
                            <div class="metric-card">
                                <p class="metric-value">{total_feedback}</p>
                                <p class="metric-label">Total Feedback</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{positive_feedback_count}</p>
                                <p class="metric-label">Positive Feedback</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{negative_feedback_count}</p>
                                <p class="metric-label">Negative Feedback</p>
                            </div>
                            <div class="metric-card">
                                <p class="metric-value">{avg_feedback_score:.1f}</p>
                                <p class="metric-label">Avg Feedback Score</p>
                            </div>
                        </div>
                    </div>

                    <div class="footer">
                        <p>Generated by AI Spark Analyzer on {generated_date}</p>
                        <p>Analysis Period: {period_start} to {period_end}</p>
                        <p>Questions? Contact your Spark optimization team</p>
                    </div>
                </div>
            </body>
            </html>
            """,

            "job_summary_text": """
            30-DAY JOB ANALYSIS SUMMARY
            =============================

            Job Information:
            - Job ID: {job_id}
            - Job Name: {job_name}
            - Cluster: {cluster_name}
            - Analysis Period: {period_start} to {period_end}

            Execution Summary:
            - Total Executions: {total_executions}
            - Successful: {successful_executions}
            - Failed: {failed_executions}
            - Success Rate: {success_rate:.1f}%

            Performance Metrics:
            - Total Cost: ${total_cost:.2f}
            - Average Duration: {avg_duration:.0f} seconds
            - Average CPU Utilization: {avg_cpu_utilization:.0f}%
            - Average Memory Utilization: {avg_memory_utilization:.0f}%

            Trends:
            - Cost Trend: {cost_trend} {cost_trend_icon}
            - Duration Trend: {duration_trend} {duration_trend_icon}
            - Success Rate Trend: {success_rate_trend}

            Optimization Summary:
            - Total Recommendations: {total_recommendations}
            - Applied Recommendations: {applied_recommendations}
            - High Priority Recommendations: {high_priority_count}
            - Estimated Potential Savings: ${estimated_savings:.2f}

            Top Recommendations:
            {top_recommendations_text}

            Patterns & Insights:
            - Total Patterns Identified: {total_patterns}
            - High Confidence Patterns: {high_confidence_patterns}
            - Pattern Types: {pattern_types_list}

            User Feedback:
            - Total Feedback: {total_feedback}
            - Positive Feedback: {positive_feedback_count}
            - Negative Feedback: {negative_feedback_count}
            - Average Feedback Score: {avg_feedback_score:.1f}

            Generated by AI Spark Analyzer on {generated_date}
            """,

            "admin_summary_html": """
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="utf-8">
                <title>30-Day Analysis Summary - Admin Report</title>
                <style>
                    body { font-family: Arial, sans-serif; margin: 0; padding: 20px; background-color: #f5f5f5; }
                    .container { max-width: 1000px; margin: 0 auto; background-color: white; padding: 30px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
                    .header { text-align: center; margin-bottom: 30px; }
                    .title { color: #333; font-size: 28px; margin: 0; }
                    .subtitle { color: #666; font-size: 16px; margin: 10px 0 0 0; }
                    .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 20px 0; }
                    .stat-card { background-color: #f8f9fa; padding: 20px; border-radius: 8px; text-align: center; }
                    .stat-value { font-size: 32px; font-weight: bold; color: #4285f4; margin: 0; }
                    .stat-label { font-size: 14px; color: #666; margin: 5px 0 0 0; }
                    .table-container { overflow-x: auto; margin: 20px 0; }
                    table { width: 100%; border-collapse: collapse; background-color: white; }
                    th, td { padding: 12px; text-align: left; border-bottom: 1px solid #ddd; }
                    th { background-color: #f8f9fa; font-weight: bold; color: #333; }
                    tr:hover { background-color: #f5f5f5; }
                    .footer { text-align: center; margin-top: 30px; padding-top: 20px; border-top: 1px solid #eee; color: #666; font-size: 12px; }
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1 class="title">30-Day Analysis Summary - Admin Report</h1>
                        <p class="subtitle">System-wide performance and optimization insights</p>
                    </div>

                    <div class="stats-grid">
                        <div class="stat-card">
                            <p class="stat-value">{total_jobs_analyzed}</p>
                            <p class="stat-label">Jobs Analyzed</p>
                        </div>
                        <div class="stat-card">
                            <p class="stat-value">{total_executions}</p>
                            <p class="stat-label">Total Executions</p>
                        </div>
                        <div class="stat-card">
                            <p class="stat-value">${total_cost:.2f}</p>
                            <p class="stat-label">Total Cost</p>
                        </div>
                        <div class="stat-card">
                            <p class="stat-value">${total_savings:.2f}</p>
                            <p class="stat-label">Potential Savings</p>
                        </div>
                        <div class="stat-card">
                            <p class="stat-value">{total_recommendations}</p>
                            <p class="stat-label">Recommendations</p>
                        </div>
                        <div class="stat-card">
                            <p class="stat-value">{applied_recommendations}</p>
                            <p class="stat-label">Applied</p>
                        </div>
                    </div>

                    <div class="table-container">
                        <h3>Top Performing Jobs</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Job Name</th>
                                    <th>Cluster</th>
                                    <th>Success Rate</th>
                                    <th>Avg Cost</th>
                                    <th>Avg Duration</th>
                                    <th>Savings</th>
                                </tr>
                            </thead>
                            <tbody>
                                {top_performing_jobs}
                            </tbody>
                        </table>
                    </div>

                    <div class="table-container">
                        <h3>Jobs Needing Attention</h3>
                        <table>
                            <thead>
                                <tr>
                                    <th>Job Name</th>
                                    <th>Cluster</th>
                                    <th>Success Rate</th>
                                    <th>Avg Cost</th>
                                    <th>Issues</th>
                                    <th>Priority</th>
                                </tr>
                            </thead>
                            <tbody>
                                {attention_jobs}
                            </tbody>
                        </table>
                    </div>

                    <div class="footer">
                        <p>Generated by AI Spark Analyzer on {generated_date}</p>
                        <p>Analysis Period: {period_start} to {period_end}</p>
                    </div>
                </div>
            </body>
            </html>
            """
        }

    async def generate_30day_summaries(self, end_date: datetime = None) -> List[JobSummaryEmail]:
        """Generate 30-day summaries for all jobs"""
        try:
            end_date = end_date or datetime.utcnow()
            start_date = end_date - timedelta(days=30)

            logger.info(f"Generating 30-day summaries for period {start_date.date()} to {end_date.date()}")

            # Get all jobs for the period
            job_ids = await self.bigquery_manager.get_all_jobs_for_period(start_date, end_date)

            if not job_ids:
                logger.warning("No jobs found for the 30-day period")
                return []

            summaries = []

            # Generate summary for each job
            for job_id in job_ids:
                try:
                    job_summary = await self.bigquery_manager.get_30day_job_summary(job_id)

                    if "error" not in job_summary:
                        # Create email summary
                        email_summary = await self._create_job_summary_email(job_summary)
                        summaries.append(email_summary)
                        logger.info(f"Generated summary for job {job_id}")
                    else:
                        logger.warning(f"Error generating summary for job {job_id}: {job_summary['error']}")

                except Exception as e:
                    logger.error(f"Error generating summary for job {job_id}: {e}")

            # Also generate admin summary
            admin_summary = await self._generate_admin_summary(job_ids, start_date, end_date)
            if admin_summary:
                summaries.append(admin_summary)

            logger.info(f"Generated {len(summaries)} email summaries")
            return summaries

        except Exception as e:
            logger.error(f"Error generating 30-day summaries: {e}")
            raise

    async def _create_job_summary_email(self, job_summary: Dict[str, Any]) -> JobSummaryEmail:
        """Create email summary for a specific job"""
        try:
            # Get recipient email (would be based on job owner/team)
            recipient_email = self._get_job_recipient_email(job_summary)

            # Create email content
            subject = f"30-Day Analysis Summary - Job {job_summary['job_name']}"

            # Prepare top recommendations
            top_recommendations = self._format_recommendations(job_summary.get("recommendations_summary", []))

            # Prepare trend icons
            trend_data = self._prepare_trend_data(job_summary.get("trend_analysis", {}))

            # Fill HTML template
            html_content = self.templates["job_summary_html"].format(
                job_id=job_summary["job_id"],
                job_name=job_summary["job_name"],
                cluster_name=job_summary["cluster_name"],
                total_executions=job_summary["execution_summary"]["total_executions"],
                success_rate=job_summary["execution_summary"]["success_rate"],
                total_cost=job_summary["performance_summary"]["total_cost"],
                avg_duration=job_summary["performance_summary"]["avg_duration_seconds"],
                avg_cpu_utilization=job_summary["performance_summary"]["avg_cpu_utilization"],
                avg_memory_utilization=job_summary["performance_summary"]["avg_memory_utilization"],
                total_recommendations=job_summary["recommendations"]["total_recommendations"],
                estimated_savings=job_summary["recommendations"]["estimated_savings"],
                applied_recommendations=job_summary["recommendations"]["applied_recommendations"],
                high_priority_count=job_summary["recommendations"]["high_priority_recommendations"],
                top_recommendations=top_recommendations,
                total_patterns=job_summary["patterns"]["total_patterns"],
                high_confidence_patterns=job_summary["patterns"]["high_confidence_patterns"],
                pattern_types_list=", ".join(job_summary["patterns"]["pattern_types"]),
                total_feedback=job_summary["feedback"]["total_feedback"],
                positive_feedback_count=job_summary["feedback"]["positive_feedback"],
                negative_feedback_count=job_summary["feedback"]["negative_feedback"],
                avg_feedback_score=job_summary["feedback"]["average_feedback_score"],
                generated_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                period_start=job_summary["analysis_period"]["start_date"],
                period_end=job_summary["analysis_period"]["end_date"],
                **trend_data
            )

            # Fill text template
            text_content = self.templates["job_summary_text"].format(
                job_id=job_summary["job_id"],
                job_name=job_summary["job_name"],
                cluster_name=job_summary["cluster_name"],
                period_start=job_summary["analysis_period"]["start_date"],
                period_end=job_summary["analysis_period"]["end_date"],
                total_executions=job_summary["execution_summary"]["total_executions"],
                successful_executions=job_summary["execution_summary"]["successful_executions"],
                failed_executions=job_summary["execution_summary"]["failed_executions"],
                success_rate=job_summary["execution_summary"]["success_rate"],
                total_cost=job_summary["performance_summary"]["total_cost"],
                avg_duration=job_summary["performance_summary"]["avg_duration_seconds"],
                avg_cpu_utilization=job_summary["performance_summary"]["avg_cpu_utilization"],
                avg_memory_utilization=job_summary["performance_summary"]["avg_memory_utilization"],
                cost_trend=trend_data["cost_trend"],
                cost_trend_icon=trend_data["cost_trend_icon"],
                duration_trend=trend_data["duration_trend"],
                duration_trend_icon=trend_data["duration_trend_icon"],
                success_rate_trend=trend_data["success_rate_trend"],
                total_recommendations=job_summary["recommendations"]["total_recommendations"],
                applied_recommendations=job_summary["recommendations"]["applied_recommendations"],
                high_priority_count=job_summary["recommendations"]["high_priority_recommendations"],
                estimated_savings=job_summary["recommendations"]["estimated_savings"],
                top_recommendations_text=self._format_recommendations_text(job_summary.get("recommendations_summary", [])),
                total_patterns=job_summary["patterns"]["total_patterns"],
                high_confidence_patterns=job_summary["patterns"]["high_confidence_patterns"],
                pattern_types_list=", ".join(job_summary["patterns"]["pattern_types"]),
                total_feedback=job_summary["feedback"]["total_feedback"],
                positive_feedback_count=job_summary["feedback"]["positive_feedback"],
                negative_feedback_count=job_summary["feedback"]["negative_feedback"],
                avg_feedback_score=job_summary["feedback"]["average_feedback_score"],
                generated_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            )

            return JobSummaryEmail(
                job_id=job_summary["job_id"],
                recipient_email=recipient_email,
                subject=subject,
                html_content=html_content,
                text_content=text_content,
                cc_emails=self.email_config.admin_emails
            )

        except Exception as e:
            logger.error(f"Error creating job summary email: {e}")
            raise

    async def _generate_admin_summary(self, job_ids: List[str], start_date: datetime, end_date: datetime) -> Optional[JobSummaryEmail]:
        """Generate admin summary email"""
        try:
            # Get summaries for all jobs
            all_summaries = []
            for job_id in job_ids:
                try:
                    summary = await self.bigquery_manager.get_30day_job_summary(job_id)
                    if "error" not in summary:
                        all_summaries.append(summary)
                except Exception as e:
                    logger.error(f"Error getting summary for admin report: {e}")

            if not all_summaries:
                return None

            # Calculate aggregate statistics
            total_jobs_analyzed = len(all_summaries)
            total_executions = sum(s["execution_summary"]["total_executions"] for s in all_summaries)
            total_cost = sum(s["performance_summary"]["total_cost"] for s in all_summaries)
            total_savings = sum(s["recommendations"]["estimated_savings"] for s in all_summaries)
            total_recommendations = sum(s["recommendations"]["total_recommendations"] for s in all_summaries)
            applied_recommendations = sum(s["recommendations"]["applied_recommendations"] for s in all_summaries)

            # Get top performing jobs
            top_performing = sorted(
                all_summaries,
                key=lambda x: (x["execution_summary"]["success_rate"], -x["performance_summary"]["total_cost"]),
                reverse=True
            )[:5]

            # Get jobs needing attention
            attention_jobs = [
                s for s in all_summaries
                if (s["execution_summary"]["success_rate"] < 80 or
                    s["recommendations"]["high_priority_recommendations"] > 2)
            ][:5]

            # Format tables
            top_performing_rows = self._format_admin_job_rows(top_performing)
            attention_job_rows = self._format_admin_job_rows(attention_jobs)

            # Fill admin template
            html_content = self.templates["admin_summary_html"].format(
                total_jobs_analyzed=total_jobs_analyzed,
                total_executions=total_executions,
                total_cost=total_cost,
                total_savings=total_savings,
                total_recommendations=total_recommendations,
                applied_recommendations=applied_recommendations,
                top_performing_jobs=top_performing_rows,
                attention_jobs=attention_job_rows,
                generated_date=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                period_start=start_date.date().isoformat(),
                period_end=end_date.date().isoformat()
            )

            return JobSummaryEmail(
                job_id="admin_summary",
                recipient_email=", ".join(self.email_config.admin_emails),
                subject=f"30-Day Analysis Summary - Admin Report ({start_date.date()} to {end_date.date()})",
                html_content=html_content,
                text_content=f"Admin summary for {total_jobs_analyzed} jobs analyzed"
            )

        except Exception as e:
            logger.error(f"Error generating admin summary: {e}")
            return None

    def _format_recommendations(self, recommendations: List[Dict[str, Any]]) -> str:
        """Format recommendations for HTML display"""
        if not recommendations:
            return "<p>No recommendations available.</p>"

        formatted = ""
        for i, rec in enumerate(recommendations[:5], 1):
            impact_score = rec.get("impact_score", 0)
            cost_savings = rec.get("cost_savings", 0)

            formatted += f"""
            <div class="recommendation-item">
                <div class="recommendation-title">{i}. {rec.get('title', 'No Title')}</div>
                <div class="recommendation-desc">{rec.get('description', 'No Description')}</div>
                <div class="recommendation-impact">
                    Impact Score: {impact_score:.1f} | Potential Savings: ${cost_savings:.2f}
                </div>
            </div>
            """

        return formatted

    def _format_recommendations_text(self, recommendations: List[Dict[str, Any]]) -> str:
        """Format recommendations for text display"""
        if not recommendations:
            return "No recommendations available."

        formatted = ""
        for i, rec in enumerate(recommendations[:5], 1):
            formatted += f"{i}. {rec.get('title', 'No Title')}\n"
            formatted += f"   {rec.get('description', 'No Description')}\n"
            formatted += f"   Impact: {rec.get('impact_score', 0):.1f} | Savings: ${rec.get('cost_savings', 0):.2f}\n\n"

        return formatted

    def _prepare_trend_data(self, trend_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare trend data for templates"""
        trends = {}

        for metric in ["cost", "duration"]:
            trend = trend_analysis.get(f"{metric}_trend", "stable")
            trends[f"{metric}_trend"] = trend
            trends[f"{metric}_trend_class"] = {
                "increasing": "negative",
                "decreasing": "positive",
                "stable": "stable"
            }.get(trend, "stable")
            trends[f"{metric}_trend_icon"] = {
                "increasing": "📈",
                "decreasing": "📉",
                "stable": "➡️"
            }.get(trend, "❓")

        trends["success_rate_trend"] = trend_analysis.get("success_rate_trend", "stable")

        return trends

    def _get_job_recipient_email(self, job_summary: Dict[str, Any]) -> str:
        """Get recipient email for a job"""
        # In a real implementation, this would map jobs to owners/teams
        # For now, return admin email
        return self.email_config.admin_emails[0]

    def _format_admin_job_rows(self, jobs: List[Dict[str, Any]]) -> str:
        """Format job rows for admin tables"""
        rows = ""
        for job in jobs:
            row = f"""
            <tr>
                <td>{job['job_name']}</td>
                <td>{job['cluster_name']}</td>
                <td>{job['execution_summary']['success_rate']:.1f}%</td>
                <td>${job['performance_summary']['total_cost']:.2f}</td>
                <td>{job['performance_summary']['avg_duration_seconds']:.0f}s</td>
                <td>${job['recommendations']['estimated_savings']:.2f}</td>
            </tr>
            """
            rows += row

        return rows

    async def send_email_summary(self, email_summary: JobSummaryEmail) -> bool:
        """Send email summary"""
        try:
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = email_summary.subject
            msg['From'] = self.email_config.from_email
            msg['To'] = email_summary.recipient_email

            if email_summary.cc_emails:
                msg['Cc'] = ", ".join(email_summary.cc_emails)

            # Attach text and HTML content
            text_part = MIMEText(email_summary.text_content, 'plain')
            html_part = MIMEText(email_summary.html_content, 'html')

            msg.attach(text_part)
            msg.attach(html_part)

            # Send email
            context = ssl.create_default_context()

            with smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port) as server:
                if self.email_config.use_tls:
                    server.starttls(context=context)
                server.login(self.email_config.username, self.email_config.password)

                recipients = [email_summary.recipient_email]
                if email_summary.cc_emails:
                    recipients.extend(email_summary.cc_emails)
                if email_summary.bcc_emails:
                    recipients.extend(email_summary.bcc_emails)

                server.sendmail(self.email_config.from_email, recipients, msg.as_string())

            logger.info(f"Email sent successfully to {email_summary.recipient_email}")
            return True

        except Exception as e:
            logger.error(f"Error sending email: {e}")
            return False

    async def send_all_summaries(self, summaries: List[JobSummaryEmail]) -> Dict[str, bool]:
        """Send all email summaries"""
        results = {}

        for summary in summaries:
            try:
                success = await self.send_email_summary(summary)
                results[summary.job_id] = success
                await asyncio.sleep(1)  # Avoid overwhelming the email server
            except Exception as e:
                logger.error(f"Error sending summary for job {summary.job_id}: {e}")
                results[summary.job_id] = False

        sent_count = sum(1 for success in results.values() if success)
        logger.info(f"Successfully sent {sent_count}/{len(summaries)} email summaries")

        return results

    def test_email_configuration(self) -> bool:
        """Test email configuration"""
        try:
            context = ssl.create_default_context()

            with smtplib.SMTP(self.email_config.smtp_server, self.email_config.smtp_port) as server:
                if self.email_config.use_tls:
                    server.starttls(context=context)
                server.login(self.email_config.username, self.email_config.password)

            logger.info("Email configuration test successful")
            return True

        except Exception as e:
            logger.error(f"Email configuration test failed: {e}")
            return False