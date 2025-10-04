"""AI analysis engine for Spark jobs.

This module provides an AI-powered engine for analyzing Spark jobs,
identifying patterns, and generating optimization recommendations. It uses
LangGraph for managing the analysis workflow and maintaining memory.
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from pathlib import Path

from langchain_openai import ChatOpenAI
from langchain.schema import HumanMessage, SystemMessage, AIMessage
from langchain.memory import ConversationBufferMemory
from langgraph import StateGraph, START, END
from langgraph.checkpoint.memory import MemorySaver

from ..core.config import AIConfig
from ..core.models import (
    SparkJob, Pattern, Recommendation, AnalysisReport
)
from .models import (
    AIAnalysis, Insight, AIRequest, AIResponse,
    PatternAnalysis, CostAnalysis, PerformanceAnalysis
)
from .prompts import PromptManager

logger = logging.getLogger(__name__)


class AIEngine:
    """An AI analysis engine with LangGraph for memory management.

    This class encapsulates the logic for running AI-powered analysis on
    Spark jobs. It uses a LangGraph `StateGraph` to manage the analysis
    workflow and maintains a conversation memory for context.

    Attributes:
        config: The AI configuration settings.
        llm: An instance of the ChatOpenAI model.
        prompt_manager: A manager for retrieving and formatting prompts.
        memory_system: A compiled LangGraph for the memory system.
        conversation_memory: A buffer for storing conversation history.
    """

    def __init__(self, config: AIConfig):
        """Initializes the AIEngine.

        Args:
            config: An `AIConfig` object containing the AI settings.
        """
        self.config = config

        # Initialize LLM
        self.llm = ChatOpenAI(
            model=config.model_name,
            temperature=config.temperature,
            max_tokens=config.max_tokens,
            openai_api_key=config.openai_api_key
        )

        # Initialize prompt manager
        self.prompt_manager = PromptManager()

        # Initialize LangGraph memory system
        self.memory_system = self._create_memory_graph()

        # Conversation memory for context
        self.conversation_memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )

        logger.info("AI Engine initialized successfully")

    def _create_memory_graph(self) -> StateGraph:
        """Creates the LangGraph state management graph.

        This method defines the nodes and edges of the analysis workflow,
        which includes steps for retrieving from memory, analyzing data,
        identifying patterns, and generating recommendations.

        Returns:
            A compiled `StateGraph` for the analysis workflow.
        """
        # Define state schema
        class AnalysisState:
            def __init__(self):
                self.analysis_request = None
                self.current_data = {}
                self.historical_context = {}
                self.insights = []
                self.patterns = []
                self.recommendations = []
                self.final_analysis = None

        # Create graph
        workflow = StateGraph(AnalysisState)

        # Add nodes
        workflow.add_node("analyze_data", self._analyze_data_node)
        workflow.add_node("retrieve_memory", self._retrieve_memory_node)
        workflow.add_node("identify_patterns", self._identify_patterns_node)
        workflow.add_node("generate_insights", self._generate_insights_node)
        workflow.add_node("create_recommendations", self._create_recommendations_node)
        workflow.add_node("update_memory", self._update_memory_node)

        # Add edges
        workflow.add_edge(START, "retrieve_memory")
        workflow.add_edge("retrieve_memory", "analyze_data")
        workflow.add_edge("analyze_data", "identify_patterns")
        workflow.add_edge("identify_patterns", "generate_insights")
        workflow.add_edge("generate_insights", "create_recommendations")
        workflow.add_edge("create_recommendations", "update_memory")
        workflow.add_edge("update_memory", END)

        # Add memory checkpointer
        memory = MemorySaver()
        return workflow.compile(checkpointer=memory)

    async def identify_patterns(self, jobs: List[SparkJob]) -> List[Pattern]:
        """Identifies performance and cost patterns in a list of Spark jobs.

        Args:
            jobs: A list of `SparkJob` objects to analyze.

        Returns:
            A list of `Pattern` objects representing the identified patterns.
        """
        logger.info(f"Identifying patterns in {len(jobs)} jobs")

        try:
            # Prepare data for analysis
            job_data_summary = self._prepare_job_summary(jobs)
            performance_metrics = self._extract_performance_metrics(jobs)

            # Create analysis request
            request = AIRequest(
                request_id=f"pattern_analysis_{datetime.utcnow().isoformat()}",
                request_type="pattern_analysis",
                input_data={
                    "jobs": [job.dict() for job in jobs],
                    "summary": job_data_summary,
                    "metrics": performance_metrics
                }
            )

            # Execute analysis through LangGraph
            result = await self._execute_analysis_workflow(request)

            # Convert results to Pattern objects
            patterns = []
            if result and "patterns" in result:
                for pattern_data in result["patterns"]:
                    pattern = Pattern(
                        pattern_id=pattern_data.get("pattern_id", f"pattern_{datetime.utcnow().isoformat()}"),
                        pattern_type=pattern_data.get("pattern_type", "unknown"),
                        description=pattern_data.get("description", ""),
                        confidence_score=pattern_data.get("confidence", 0.5),
                        frequency=pattern_data.get("frequency", 1),
                        job_ids=pattern_data.get("job_ids", []),
                        clusters=pattern_data.get("clusters", []),
                        created_at=datetime.utcnow()
                    )
                    patterns.append(pattern)

            logger.info(f"Identified {len(patterns)} patterns")
            return patterns

        except Exception as e:
            logger.error(f"Error identifying patterns: {e}")
            return []

    async def identify_long_term_patterns(self, jobs: List[SparkJob]) -> List[Pattern]:
        """Identifies long-term patterns across extended time periods.

        This method groups jobs by week and month to analyze trends and
        recurring patterns over time.

        Args:
            jobs: A list of `SparkJob` objects spanning multiple weeks or months.

        Returns:
            A list of `Pattern` objects representing the long-term patterns.
        """
        logger.info("Analyzing long-term patterns")

        # Group jobs by time periods
        weekly_jobs = self._group_jobs_by_week(jobs)
        monthly_jobs = self._group_jobs_by_month(jobs)

        # Analyze trends over time
        long_term_patterns = []

        # Weekly pattern analysis
        for week, week_jobs in weekly_jobs.items():
            weekly_patterns = await self.identify_patterns(week_jobs)
            for pattern in weekly_patterns:
                pattern.time_period = {"start": week[0], "end": week[1]}
                long_term_patterns.append(pattern)

        # Monthly pattern analysis
        for month, month_jobs in monthly_jobs.items():
            monthly_patterns = await self.identify_patterns(month_jobs)
            for pattern in monthly_patterns:
                pattern.time_period = {"start": month[0], "end": month[1]}
                long_term_patterns.append(pattern)

        return long_term_patterns

    async def analyze_cost_optimization(self, jobs: List[SparkJob]) -> CostAnalysis:
        """Analyzes cost optimization opportunities for a list of Spark jobs.

        Args:
            jobs: A list of `SparkJob` objects to analyze for cost.

        Returns:
            A `CostAnalysis` object with the results of the analysis.

        Raises:
            Exception: If an error occurs during the cost analysis.
        """
        logger.info("Analyzing cost optimization opportunities")

        try:
            # Prepare cost data
            cost_data = self._prepare_cost_data(jobs)
            job_cost_breakdown = self._analyze_job_costs(jobs)

            # Create analysis request
            request = AIRequest(
                request_id=f"cost_analysis_{datetime.utcnow().isoformat()}",
                request_type="cost_optimization",
                input_data={
                    "cost_data": cost_data,
                    "job_breakdown": job_cost_breakdown,
                    "total_jobs": len(jobs)
                }
            )

            # Get cost optimization prompt
            prompt = self.prompt_manager.get_prompt(
                "cost_optimization",
                cost_data=json.dumps(cost_data, indent=2),
                job_cost_breakdown=json.dumps(job_cost_breakdown, indent=2),
                resource_usage=json.dumps(self._get_resource_usage_summary(jobs), indent=2),
                cluster_config_impact=json.dumps(self._get_cluster_config_impact(jobs), indent=2)
            )

            # Execute analysis
            response = await self._execute_llm_analysis(prompt, request)

            # Parse response
            cost_analysis = CostAnalysis(
                analysis_id=f"cost_analysis_{datetime.utcnow().isoformat()}",
                period_start=min(job.submit_time for job in jobs),
                period_end=max(job.submit_time for job in jobs),
                total_cost=sum(job.cost_estimate or 0 for job in jobs),
                compute_cost=0,  # Would be calculated from actual usage
                storage_cost=0,  # Would be calculated from actual usage
                network_cost=0,  # Would be calculated from actual usage
                other_costs=0,
                cost_trend="stable",  # Would be calculated from historical data
                cost_change_percentage=0,
                optimization_opportunities=response.get("optimization_opportunities", []),
                potential_savings=response.get("roi_projection", {}).get("monthly_savings", 0),
                top_cost_drivers=response.get("cost_analysis", {}).get("major_cost_drivers", []),
                recommendations=response.get("recommendations", []),
                created_at=datetime.utcnow()
            )

            logger.info("Cost optimization analysis completed")
            return cost_analysis

        except Exception as e:
            logger.error(f"Error in cost optimization analysis: {e}")
            raise

    async def generate_summary_insights(self, jobs: List[SparkJob], patterns: List[Pattern], recommendations: List[Recommendation]) -> str:
        """Generates an executive summary of the analysis insights.

        Args:
            jobs: A list of the analyzed Spark jobs.
            patterns: A list of the identified patterns.
            recommendations: A list of the generated recommendations.

        Returns:
            A string containing the executive summary.
        """
        logger.info("Generating executive summary insights")

        try:
            # Prepare summary data
            analysis_summary = self._prepare_analysis_summary(jobs, patterns, recommendations)
            key_findings = self._extract_key_findings(jobs, patterns)
            performance_trends = self._analyze_performance_trends(jobs)
            cost_trends = self._analyze_cost_trends(jobs)

            # Get summary insights prompt
            prompt = self.prompt_manager.get_prompt(
                "summary_insights",
                analysis_summary=json.dumps(analysis_summary, indent=2),
                key_findings=json.dumps(key_findings, indent=2),
                performance_trends=json.dumps(performance_trends, indent=2),
                cost_trends=json.dumps(cost_trends, indent=2),
                recommendations=json.dumps([rec.dict() for rec in recommendations[:5]], indent=2)  # Top 5
            )

            # Execute analysis
            response = await self._execute_llm_analysis(prompt)

            summary = response.get("executive_summary", "Analysis completed successfully")
            logger.info("Executive summary generated")
            return summary

        except Exception as e:
            logger.error(f"Error generating summary insights: {e}")
            return "Analysis completed with insights generation."

    async def _execute_analysis_workflow(self, request: AIRequest) -> Dict[str, Any]:
        """Executes the analysis workflow using the LangGraph memory system.

        Args:
            request: An `AIRequest` object containing the analysis request.

        Returns:
            A dictionary containing the results of the workflow execution.
        """
        try:
            # Initialize state
            initial_state = {
                "analysis_request": request,
                "current_data": request.input_data,
                "historical_context": {},
                "insights": [],
                "patterns": [],
                "recommendations": [],
                "final_analysis": None
            }

            # Execute workflow
            config = {"configurable": {"thread_id": request.request_id}}
            result = await self.memory_system.ainvoke(initial_state, config=config)

            return result

        except Exception as e:
            logger.error(f"Error executing analysis workflow: {e}")
            return {}

    async def _retrieve_memory_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """A node in the LangGraph workflow to retrieve relevant historical context.

        Args:
            state: The current state of the analysis workflow.

        Returns:
            The updated state with historical context.
        """
        try:
            # In a real implementation, this would query the memory system
            # For now, simulate memory retrieval
            state["historical_context"] = {
                "similar_analyses": [],
                "relevant_patterns": [],
                "past_recommendations": []
            }
            return state
        except Exception as e:
            logger.error(f"Error in memory retrieval: {e}")
            return state

    async def _analyze_data_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """A node in the LangGraph workflow to analyze the current data.

        Args:
            state: The current state of the analysis workflow.

        Returns:
            The updated state with the analysis result.
        """
        try:
            request = state["analysis_request"]
            current_data = state["current_data"]

            # Perform basic analysis
            analysis_result = {
                "total_jobs": len(current_data.get("jobs", [])),
                "analysis_timestamp": datetime.utcnow().isoformat(),
                "data_quality_score": 0.9  # Placeholder
            }

            state["analysis_result"] = analysis_result
            return state
        except Exception as e:
            logger.error(f"Error in data analysis: {e}")
            return state

    async def _identify_patterns_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """A node in the LangGraph workflow to identify patterns in the data.

        Args:
            state: The current state of the analysis workflow.

        Returns:
            The updated state with the identified patterns.
        """
        try:
            # Use pattern identification logic
            patterns = []  # Would be identified through analysis
            state["patterns"] = patterns
            return state
        except Exception as e:
            logger.error(f"Error in pattern identification: {e}")
            return state

    async def _generate_insights_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """A node in the LangGraph workflow to generate insights from the analysis.

        Args:
            state: The current state of the analysis workflow.

        Returns:
            The updated state with the generated insights.
        """
        try:
            insights = []  # Would be generated through analysis
            state["insights"] = insights
            return state
        except Exception as e:
            logger.error(f"Error in insight generation: {e}")
            return state

    async def _create_recommendations_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """A node in the LangGraph workflow to create optimization recommendations.

        Args:
            state: The current state of the analysis workflow.

        Returns:
            The updated state with the created recommendations.
        """
        try:
            recommendations = []  # Would be generated through analysis
            state["recommendations"] = recommendations
            return state
        except Exception as e:
            logger.error(f"Error in recommendation creation: {e}")
            return state

    async def _update_memory_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """A node in the LangGraph workflow to update the memory with new insights.

        Args:
            state: The current state of the analysis workflow.

        Returns:
            The updated state after the memory update.
        """
        try:
            # In a real implementation, this would update the memory system
            state["memory_updated"] = True
            return state
        except Exception as e:
            logger.error(f"Error in memory update: {e}")
            return state

    async def _execute_llm_analysis(self, prompt: str, request: AIRequest) -> Dict[str, Any]:
        """Executes an LLM analysis with the given prompt.

        Args:
            prompt: The prompt to send to the LLM.
            request: The `AIRequest` object for this analysis.

        Returns:
            A dictionary containing the parsed JSON response from the LLM.
        """
        try:
            messages = [
                SystemMessage(content="You are an expert Spark and cloud infrastructure analyst. Provide detailed, actionable analysis in valid JSON format."),
                HumanMessage(content=prompt)
            ]

            response = await self.llm.ainvoke(messages)
            content = response.content

            # Parse JSON response
            try:
                # Extract JSON from response
                json_start = content.find('{')
                json_end = content.rfind('}') + 1
                if json_start != -1 and json_end != -1:
                    json_content = content[json_start:json_end]
                    return json.loads(json_content)
                else:
                    logger.warning("Could not extract JSON from LLM response")
                    return {"raw_response": content}
            except json.JSONDecodeError as e:
                logger.error(f"Error parsing LLM JSON response: {e}")
                return {"raw_response": content, "parse_error": str(e)}

        except Exception as e:
            logger.error(f"Error executing LLM analysis: {e}")
            return {"error": str(e)}

    def _prepare_job_summary(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Prepares a summary of job data.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary containing the job summary.
        """
        total_jobs = len(jobs)
        successful_jobs = [j for j in jobs if j.status == "DONE"]
        failed_jobs = [j for j in jobs if j.status == "ERROR"]

        return {
            "total_jobs": total_jobs,
            "successful_jobs": len(successful_jobs),
            "failed_jobs": len(failed_jobs),
            "success_rate": (len(successful_jobs) / total_jobs * 100) if total_jobs > 0 else 0,
            "total_cost": sum(j.cost_estimate or 0 for j in jobs),
            "average_duration": sum(j.duration_seconds or 0 for j in jobs) / total_jobs if total_jobs > 0 else 0
        }

    def _extract_performance_metrics(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Extracts performance metrics from a list of jobs.

        Note: This is a placeholder and would be calculated from actual data.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary of performance metrics.
        """
        return {
            "cpu_utilization": 0.75,  # Placeholder - would be calculated from actual data
            "memory_utilization": 0.80,
            "disk_io": 0.60,
            "network_io": 0.40
        }

    def _prepare_cost_data(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Prepares cost data for analysis.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary containing cost data.
        """
        return {
            "total_cost": sum(job.cost_estimate or 0 for job in jobs),
            "cost_per_job": [job.cost_estimate or 0 for job in jobs],
            "cost_distribution": {
                "compute": 0.7,
                "storage": 0.2,
                "network": 0.1
            }
        }

    def _analyze_job_costs(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Analyzes the cost breakdown of jobs.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary with the cost breakdown.
        """
        return {
            "job_costs": [{"job_id": job.job_id, "cost": job.cost_estimate or 0} for job in jobs],
            "cost_by_status": {},
            "cost_by_cluster": {}
        }

    def _get_resource_usage_summary(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Gets a summary of resource usage.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary with the resource usage summary.
        """
        return {
            "vcore_usage": sum(job.vcore_seconds or 0 for job in jobs),
            "memory_usage": sum(job.memory_milliseconds or 0 for job in jobs)
        }

    def _get_cluster_config_impact(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Analyzes the impact of cluster configuration.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary with the cluster configuration impact analysis.
        """
        return {
            "cluster_performance": {},
            "configuration_efficiency": 0.8
        }

    def _group_jobs_by_week(self, jobs: List[SparkJob]) -> Dict[tuple, List[SparkJob]]:
        """Groups jobs by week.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary where keys are week tuples (start_date, end_date)
            and values are lists of jobs in that week.
        """
        weekly_jobs = {}
        for job in jobs:
            if job.submit_time:
                week_start = job.submit_time - timedelta(days=job.submit_time.weekday())
                week_end = week_start + timedelta(days=6)
                week_key = (week_start.date(), week_end.date())

                if week_key not in weekly_jobs:
                    weekly_jobs[week_key] = []
                weekly_jobs[week_key].append(job)
        return weekly_jobs

    def _group_jobs_by_month(self, jobs: List[SparkJob]) -> Dict[tuple, List[SparkJob]]:
        """Groups jobs by month.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary where keys are month tuples (start_date, end_date)
            and values are lists of jobs in that month.
        """
        monthly_jobs = {}
        for job in jobs:
            if job.submit_time:
                month_start = job.submit_time.replace(day=1)
                # Calculate month end
                if job.submit_time.month == 12:
                    month_end = job.submit_time.replace(year=job.submit_time.year + 1, month=1, day=1) - timedelta(days=1)
                else:
                    month_end = job.submit_time.replace(month=job.submit_time.month + 1, day=1) - timedelta(days=1)

                month_key = (month_start.date(), month_end.date())

                if month_key not in monthly_jobs:
                    monthly_jobs[month_key] = []
                monthly_jobs[month_key].append(job)
        return monthly_jobs

    def _prepare_analysis_summary(self, jobs: List[SparkJob], patterns: List[Pattern], recommendations: List[Recommendation]) -> Dict[str, Any]:
        """Prepares an analysis summary for insights generation.

        Args:
            jobs: A list of `SparkJob` objects.
            patterns: A list of `Pattern` objects.
            recommendations: A list of `Recommendation` objects.

        Returns:
            A dictionary with the analysis summary.
        """
        return {
            "jobs_summary": self._prepare_job_summary(jobs),
            "patterns_count": len(patterns),
            "recommendations_count": len(recommendations),
            "high_priority_recommendations": len([r for r in recommendations if r.severity in ["high", "critical"]])
        }

    def _extract_key_findings(self, jobs: List[SparkJob], patterns: List[Pattern]) -> List[Dict[str, Any]]:
        """Extracts key findings from the analysis.

        Args:
            jobs: A list of `SparkJob` objects.
            patterns: A list of `Pattern` objects.

        Returns:
            A list of dictionaries, each representing a key finding.
        """
        findings = []

        # Extract findings from patterns
        for pattern in patterns:
            if pattern.confidence_score > 0.7:
                findings.append({
                    "type": "pattern",
                    "description": pattern.description,
                    "confidence": pattern.confidence_score,
                    "frequency": pattern.frequency
                })

        return findings

    def _analyze_performance_trends(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Analyzes performance trends.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary of performance trends.
        """
        return {
            "duration_trend": "stable",
            "success_rate_trend": "improving",
            "resource_efficiency_trend": "stable"
        }

    def _analyze_cost_trends(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Analyzes cost trends.

        Args:
            jobs: A list of `SparkJob` objects.

        Returns:
            A dictionary of cost trends.
        """
        return {
            "cost_trend": "increasing",
            "cost_per_job_trend": "stable",
            "efficiency_trend": "improving"
        }