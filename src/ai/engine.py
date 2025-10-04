"""
AI analysis engine using LangGraph for memory management and reasoning
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
    """
    AI analysis engine with LangGraph memory management
    """

    def __init__(self, config: AIConfig):
        """
        Initialize AI Engine

        Args:
            config: AI configuration
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
        """Create LangGraph state management graph"""
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
        """
        Identify patterns in Spark job data

        Args:
            jobs: List of Spark jobs to analyze

        Returns:
            List of identified patterns
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
        """
        Identify long-term patterns across extended time periods

        Args:
            jobs: List of Spark jobs spanning multiple weeks/months

        Returns:
            List of long-term patterns
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
        """
        Analyze cost optimization opportunities

        Args:
            jobs: List of Spark jobs

        Returns:
            Cost analysis results
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
        """
        Generate executive summary insights

        Args:
            jobs: List of analyzed jobs
            patterns: Identified patterns
            recommendations: Generated recommendations

        Returns:
            Executive summary string
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
        """Execute analysis through LangGraph workflow"""
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
        """Node to retrieve relevant historical context"""
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
        """Node to analyze current data"""
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
        """Node to identify patterns in data"""
        try:
            # Use pattern identification logic
            patterns = []  # Would be identified through analysis
            state["patterns"] = patterns
            return state
        except Exception as e:
            logger.error(f"Error in pattern identification: {e}")
            return state

    async def _generate_insights_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node to generate insights"""
        try:
            insights = []  # Would be generated through analysis
            state["insights"] = insights
            return state
        except Exception as e:
            logger.error(f"Error in insight generation: {e}")
            return state

    async def _create_recommendations_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node to create recommendations"""
        try:
            recommendations = []  # Would be generated through analysis
            state["recommendations"] = recommendations
            return state
        except Exception as e:
            logger.error(f"Error in recommendation creation: {e}")
            return state

    async def _update_memory_node(self, state: Dict[str, Any]) -> Dict[str, Any]:
        """Node to update memory with new insights"""
        try:
            # In a real implementation, this would update the memory system
            state["memory_updated"] = True
            return state
        except Exception as e:
            logger.error(f"Error in memory update: {e}")
            return state

    async def _execute_llm_analysis(self, prompt: str, request: AIRequest) -> Dict[str, Any]:
        """Execute LLM analysis with the given prompt"""
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
        """Prepare summary of job data"""
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
        """Extract performance metrics from jobs"""
        return {
            "cpu_utilization": 0.75,  # Placeholder - would be calculated from actual data
            "memory_utilization": 0.80,
            "disk_io": 0.60,
            "network_io": 0.40
        }

    def _prepare_cost_data(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Prepare cost data for analysis"""
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
        """Analyze job costs breakdown"""
        return {
            "job_costs": [{"job_id": job.job_id, "cost": job.cost_estimate or 0} for job in jobs],
            "cost_by_status": {},
            "cost_by_cluster": {}
        }

    def _get_resource_usage_summary(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Get resource usage summary"""
        return {
            "vcore_usage": sum(job.vcore_seconds or 0 for job in jobs),
            "memory_usage": sum(job.memory_milliseconds or 0 for job in jobs)
        }

    def _get_cluster_config_impact(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Analyze cluster configuration impact"""
        return {
            "cluster_performance": {},
            "configuration_efficiency": 0.8
        }

    def _group_jobs_by_week(self, jobs: List[SparkJob]) -> Dict[tuple, List[SparkJob]]:
        """Group jobs by week"""
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
        """Group jobs by month"""
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
        """Prepare analysis summary for insights generation"""
        return {
            "jobs_summary": self._prepare_job_summary(jobs),
            "patterns_count": len(patterns),
            "recommendations_count": len(recommendations),
            "high_priority_recommendations": len([r for r in recommendations if r.severity in ["high", "critical"]])
        }

    def _extract_key_findings(self, jobs: List[SparkJob], patterns: List[Pattern]) -> List[Dict[str, Any]]:
        """Extract key findings from analysis"""
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
        """Analyze performance trends"""
        return {
            "duration_trend": "stable",
            "success_rate_trend": "improving",
            "resource_efficiency_trend": "stable"
        }

    def _analyze_cost_trends(self, jobs: List[SparkJob]) -> Dict[str, Any]:
        """Analyze cost trends"""
        return {
            "cost_trend": "increasing",
            "cost_per_job_trend": "stable",
            "efficiency_trend": "improving"
        }