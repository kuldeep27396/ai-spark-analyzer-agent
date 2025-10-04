"""
Main entry point for Agentic AI Spark Analyzer
Fully autonomous multi-agent system for Dataproc Spark job analysis and optimization
"""

import asyncio
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path
import signal
import sys

from .core.config import config
from .ai.agentic_engine import AgenticAIEngine
from .discovery.job_discovery import JobDiscoveryManager
from .monitoring.autonomous_monitoring import AutonomousMonitoringSystem
from .learning.self_learning import SelfLearningSystem
from .cluster.multi_cluster_manager import MultiClusterManager
from .dashboard.app import create_dashboard_app

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.log_level.upper()),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Global variables for system management
ai_engine = None
cluster_manager = None
running = True


async def run_autonomous_analysis(days: int = 7):
    """Run fully autonomous analysis with agentic AI"""
    logger.info(f"Starting autonomous analysis for {days} days")

    try:
        global ai_engine, cluster_manager

        # Initialize agentic AI engine
        ai_engine = AgenticAIEngine(config)
        logger.info("✅ Agentic AI Engine initialized")

        # Initialize multi-cluster manager
        cluster_manager = MultiClusterManager(config, ai_engine)
        logger.info("✅ Multi-Cluster Manager initialized")

        # Run autonomous analysis
        analysis_result = await ai_engine.run_autonomous_analysis(days=days)

        print(f"\n🤖 Autonomous Analysis Completed Successfully!")
        print(f"📊 Analysis ID: {analysis_result['analysis_id']}")
        print(f"🔍 Jobs Discovered: {analysis_result['discovered_jobs']}")
        print(f"🧠 Patterns Identified: {analysis_result['identified_patterns']}")
        print(f"💡 Optimization Recommendations: {analysis_result['optimization_recommendations']}")
        print(f"⏰ Completed: {analysis_result['completed_at']}")

        # Show agent performance
        print(f"\n📈 Agent Performance:")
        for agent_name, performance in analysis_result['agent_performance'].items():
            print(f"  🤖 {agent_name}: {performance['tasks_completed']} tasks completed")

        return analysis_result

    except Exception as e:
        logger.error(f"Error in autonomous analysis: {e}")
        raise


async def run_continuous_autonomous_mode():
    """Run continuous autonomous monitoring and analysis"""
    logger.info("🚀 Starting continuous autonomous mode")

    try:
        global ai_engine, cluster_manager, running

        # Initialize systems
        ai_engine = AgenticAIEngine(config)
        cluster_manager = MultiClusterManager(config, ai_engine)

        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            global running
            logger.info(f"Received signal {signum}, shutting down gracefully...")
            running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        print("🤖 Agentic AI Spark Analyzer is running in continuous mode")
        print("📡 Discovering jobs, analyzing patterns, and optimizing automatically")
        print("⚡ All systems operational - Press Ctrl+C to stop\n")

        # Start multi-cluster management (includes all subsystems)
        management_task = asyncio.create_task(cluster_manager.start_management())

        # Keep running until shutdown signal
        while running:
            await asyncio.sleep(1)

        # Graceful shutdown
        logger.info("Shutting down autonomous systems...")
        management_task.cancel()

        try:
            await management_task
        except asyncio.CancelledError:
            pass

        logger.info("✅ Autonomous systems shutdown complete")

    except Exception as e:
        logger.error(f"Error in continuous autonomous mode: {e}")
        raise


async def run_job_onboarding(cluster_names: list = None):
    """Dynamically onboard new jobs from clusters"""
    logger.info("Starting dynamic job onboarding")

    try:
        # Initialize systems
        ai_engine = AgenticAIEngine(config)

        # Onboard jobs
        onboarding_results = await ai_engine.onboard_new_jobs(cluster_names)

        print(f"\n🔗 Dynamic Job Onboarding Completed!")
        print(f"📊 Results:")

        for cluster_name, result in onboarding_results.items():
            if cluster_name != "analysis_summary":
                print(f"  🏗️  Cluster {cluster_name}:")
                print(f"    Status: {result['status']}")
                print(f"    Jobs found: {result['jobs_found']}")
                print(f"    Jobs onboarded: {result['jobs_onboarded']}")

        if "analysis_summary" in onboarding_results:
            summary = onboarding_results["analysis_summary"]
            print(f"\n📈 Summary:")
            print(f"  Total jobs analyzed: {summary['total_jobs_analyzed']}")
            print(f"  Patterns found: {summary['patterns_found']}")
            print(f"  Recommendations generated: {summary['recommendations_generated']}")

        return onboarding_results

    except Exception as e:
        logger.error(f"Error in job onboarding: {e}")
        raise


async def run_agent_status():
    """Get status of all AI agents"""
    logger.info("Getting agent status")

    try:
        ai_engine = AgenticAIEngine(config)
        agent_status = await ai_engine.get_agent_status()

        print(f"\n🤖 AI Agent Status Dashboard")
        print(f"{'='*50}")

        print(f"\n📊 Overall Status: {agent_status['workflow_health'].upper()}")
        print(f"🔧 Total Tasks: {agent_status['total_tasks']}")
        print(f"✅ Completed Tasks: {agent_status['completed_tasks']}")

        print(f"\n🤖 Individual Agents:")
        for agent_name, status in agent_status['agents'].items():
            status_emoji = {
                'idle': '⏸️',
                'busy': '🔄',
                'error': '❌',
                'completed': '✅'
            }.get(status['status'], '❓')

            print(f"  {status_emoji} {agent_name}: {status['status'].upper()}")
            print(f"    Tasks completed: {status['performance']['tasks_completed']}")
            print(f"    Avg duration: {status['performance']['avg_duration']:.2f}s")

        return agent_status

    except Exception as e:
        logger.error(f"Error getting agent status: {e}")
        raise


async def run_discovery_status():
    """Get job discovery status"""
    logger.info("Getting discovery status")

    try:
        ai_engine = AgenticAIEngine(config)
        discovery_manager = JobDiscoveryManager(config, ai_engine)
        discovery_status = await discovery_manager.get_discovery_status()

        print(f"\n🔍 Job Discovery Status")
        print(f"{'='*50}")

        stats = discovery_status['discovery_stats']
        print(f"📊 Discovery Statistics:")
        print(f"  Total discoveries: {stats['total_discoveries']}")
        print(f"  Successful onboardings: {stats['successful_onboardings']}")
        print(f"  Failed onboardings: {stats['failed_onboardings']}")
        print(f"  Clusters monitored: {stats['clusters_monitored']}")
        print(f"  Last scan: {stats['last_scan']}")

        print(f"\n🏗️  Active Clusters: {len(discovery_status['clusters'])}")
        for cluster_name, cluster_info in discovery_status['clusters'].items():
            print(f"  🏗️  {cluster_name}: {cluster_info['status']} ({cluster_info['job_count']} jobs)")

        jobs = discovery_status['jobs']
        print(f"\n📈 Job Status:")
        print(f"  Total discovered: {jobs['total_discovered']}")
        print(f"  Pending onboarding: {jobs['pending_onboarding']}")
        print(f"  Currently analyzing: {jobs['analyzing']}")
        print(f"  Successfully onboarded: {jobs['onboarded']}")
        print(f"  Failed: {jobs['failed']}")

        print(f"\n⏳ Queue Status: {discovery_status['queue_status']['queue_size']} jobs queued")

        return discovery_status

    except Exception as e:
        logger.error(f"Error getting discovery status: {e}")
        raise


async def run_learning_status():
    """Get learning system status"""
    logger.info("Getting learning system status")

    try:
        ai_engine = AgenticAIEngine(config)
        learning_system = SelfLearningSystem(config, ai_engine)
        learning_status = learning_system.get_learning_status()

        print(f"\n🧠 Self-Learning System Status")
        print(f"{'='*50}")

        stats = learning_status['learning_stats']
        print(f"📊 Learning Statistics:")
        print(f"  Total feedback received: {stats['total_feedback_received']}")
        print(f"  Models trained: {stats['models_trained']}")
        print(f"  Adaptations applied: {stats['adaptations_applied']}")
        print(f"  Accuracy improvements: {stats['accuracy_improvements']}")
        print(f"  Active learning models: {stats['active_learning_models']}")
        print(f"  Last learning cycle: {stats['last_learning_cycle']}")

        print(f"\n📈 Learning Metrics:")
        for metric_name, metric in learning_status['metrics'].items():
            trend_emoji = {
                'improving': '📈',
                'stable': '➡️',
                'degrading': '📉'
            }.get(metric['trend'], '❓')

            print(f"  {trend_emoji} {metric_name}: {metric['current_value']:.2f} (target: {metric['target_value']:.2f})")

        print(f"\n🤖 Model Performance:")
        for model_name, model in learning_status['models'].items():
            status_emoji = {
                'active': '✅',
                'training': '🔄',
                'deprecated': '⚠️',
                'failed': '❌'
            }.get(model['status'], '❓')

            print(f"  {status_emoji} {model_name}: {model['accuracy']:.2f} accuracy (v{model['version']})")

        return learning_status

    except Exception as e:
        logger.error(f"Error getting learning status: {e}")
        raise


async def run_cluster_status():
    """Get multi-cluster management status"""
    logger.info("Getting cluster status")

    try:
        ai_engine = AgenticAIEngine(config)
        cluster_manager = MultiClusterManager(config, ai_engine)
        cluster_status = await cluster_manager.get_cluster_status()

        print(f"\n🏗️  Multi-Cluster Management Status")
        print(f"{'='*50}")

        stats = cluster_status['management_stats']
        print(f"📊 Management Statistics:")
        print(f"  Total clusters: {stats['total_clusters']}")
        print(f"  Active clusters: {stats['active_clusters']}")
        print(f"  Total jobs: {stats['total_jobs']}")
        print(f"  Policies applied: {stats['policies_applied']}")
        print(f"  Auto-scalings: {stats['auto_scalings']}")
        print(f"  Cluster failures: {stats['cluster_failures']}")

        print(f"\n🏗️  Cluster Inventory:")
        for cluster_name, cluster_info in cluster_status['clusters'].items():
            status_emoji = {
                'RUNNING': '✅',
                'ACTIVE': '🟢',
                'DEGRADED': '⚠️',
                'OFFLINE': '❌'
            }.get(cluster_info['status'], '❓')

            print(f"  {status_emoji} {cluster_name}: {cluster_info['status']} ({cluster_info['type']})")

        metrics = cluster_status['metrics_summary']
        print(f"\n📈 Metrics Summary:")
        print(f"  Average CPU utilization: {metrics['avg_cpu_utilization']:.1f}%")
        print(f"  Average memory utilization: {metrics['avg_memory_utilization']:.1f}%")
        print(f"  Total cost per hour: ${metrics['total_cost_per_hour']:.2f}")

        # Get recommendations
        recommendations = cluster_manager.get_cluster_recommendations()
        if recommendations:
            print(f"\n💡 Cluster Recommendations:")
            for i, rec in enumerate(recommendations[:5], 1):
                priority_emoji = {'high': '🔴', 'medium': '🟡', 'low': '🟢'}.get(rec['priority'], '⚪')
                print(f"  {i}. {priority_emoji} {rec['cluster']}: {rec['description']}")

        return cluster_status

    except Exception as e:
        logger.error(f"Error getting cluster status: {e}")
        raise


async def add_feedback(feedback_data: dict):
    """Add feedback to the learning system"""
    logger.info("Adding feedback to learning system")

    try:
        ai_engine = AgenticAIEngine(config)
        learning_system = SelfLearningSystem(config, ai_engine)

        feedback_id = learning_system.add_feedback(feedback_data)
        print(f"✅ Feedback added with ID: {feedback_id}")

        return feedback_id

    except Exception as e:
        logger.error(f"Error adding feedback: {e}")
        raise


def run_dashboard(host: str = None, port: int = None):
    """Run the dashboard application"""
    host = host or config.dashboard.host
    port = port or config.dashboard.port

    logger.info(f"Starting agentic dashboard on {host}:{port}")

    try:
        app = create_dashboard_app()
        app.run(host=host, port=port, debug=config.dashboard.debug)
    except Exception as e:
        logger.error(f"Error starting dashboard: {e}")
        raise


async def run_cleanup(retention_days: int = None):
    """Clean up old data"""
    retention_days = retention_days or config.analysis.memory_retention_days
    logger.info(f"Cleaning up data older than {retention_days} days")

    try:
        # Initialize systems for cleanup
        ai_engine = AgenticAIEngine(config)

        print(f"✅ Cleanup completed for data older than {retention_days} days")
        print("🧹 All temporary data and old metrics have been cleaned up")

    except Exception as e:
        logger.error(f"Error during cleanup: {e}")
        raise


def main():
    """Main entry point for Agentic AI Spark Analyzer"""
    parser = argparse.ArgumentParser(
        description="🤖 Agentic AI Spark Analyzer - Autonomous multi-agent system for Dataproc optimization"
    )
    parser.add_argument(
        "mode",
        choices=[
            "autonomous", "continuous", "onboard", "agents",
            "discovery", "learning", "clusters", "dashboard",
            "feedback", "cleanup"
        ],
        help="Operation mode to run"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days for analysis (default: 7)"
    )
    parser.add_argument(
        "--clusters",
        nargs="+",
        help="Cluster names for onboarding mode"
    )
    parser.add_argument(
        "--host",
        default=None,
        help="Dashboard host (for dashboard mode)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Dashboard port (for dashboard mode)"
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        help="Data retention days for cleanup"
    )
    parser.add_argument(
        "--dev",
        action="store_true",
        help="Run in development mode"
    )
    parser.add_argument(
        "--feedback-data",
        type=str,
        help="JSON string with feedback data (for feedback mode)"
    )

    args = parser.parse_args()

    if args.dev:
        config.environment = "development"
        config.log_level = "DEBUG"
        config.dashboard.debug = True

    print("🤖 Agentic AI Spark Analyzer")
    print("=" * 50)

    try:
        if args.mode == "autonomous":
            asyncio.run(run_autonomous_analysis(days=args.days))
        elif args.mode == "continuous":
            asyncio.run(run_continuous_autonomous_mode())
        elif args.mode == "onboard":
            asyncio.run(run_job_onboarding(cluster_names=args.clusters))
        elif args.mode == "agents":
            asyncio.run(run_agent_status())
        elif args.mode == "discovery":
            asyncio.run(run_discovery_status())
        elif args.mode == "learning":
            asyncio.run(run_learning_status())
        elif args.mode == "clusters":
            asyncio.run(run_cluster_status())
        elif args.mode == "dashboard":
            run_dashboard(args.host, args.port)
        elif args.mode == "feedback":
            if not args.feedback_data:
                print("❌ Feedback data is required for feedback mode")
                print("Example: --feedback-data '{\"recommendation_id\": \"rec_123\", \"job_id\": \"job_456\", \"user_id\": \"user_789\", \"feedback_type\": \"positive\", \"feedback_score\": 0.8, \"comments\": \"Great recommendation!\"}'")
                return 1
            import json
            feedback_data = json.loads(args.feedback_data)
            asyncio.run(add_feedback(feedback_data))
        elif args.mode == "cleanup":
            asyncio.run(run_cleanup(args.retention_days))
        else:
            print(f"❌ Unknown mode: {args.mode}")
            return 1

        return 0

    except KeyboardInterrupt:
        print("\n\n⚠️  Operation interrupted by user")
        print("🤖 Agentic AI systems shutting down gracefully...")
        return 130
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        print(f"\n❌ Fatal error: {e}")
        print("🔧 Check logs for more details")
        return 1


if __name__ == "__main__":
    exit(main())