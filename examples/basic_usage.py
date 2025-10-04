"""
Basic usage example for AI Spark Analyzer
"""

import asyncio
import logging
from datetime import datetime, timedelta

# Add src to path
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from core.config import config
from core.analyzer import SparkAnalyzer
from core.models import SparkJob, JobStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def basic_analysis_example():
    """Basic analysis example"""
    print("🚀 AI Spark Analyzer - Basic Usage Example")
    print("=" * 50)

    try:
        # Initialize analyzer
        analyzer = SparkAnalyzer()
        print("✅ Spark Analyzer initialized")

        # Run daily analysis
        print("\n📊 Running daily analysis...")
        jobs = await analyzer.analyze_recent_jobs(days=7)
        print(f"✅ Analyzed {len(jobs)} jobs")

        # Generate patterns
        print("\n🔍 Identifying patterns...")
        patterns = await analyzer.ai_engine.identify_patterns(jobs)
        print(f"✅ Identified {len(patterns)} patterns")

        # Generate recommendations
        print("\n💡 Generating recommendations...")
        recommendations = await analyzer.recommendation_engine.generate_recommendations(jobs, patterns)
        print(f"✅ Generated {len(recommendations)} recommendations")

        # Display summary
        print("\n📋 Analysis Summary:")
        print(f"   Jobs analyzed: {len(jobs)}")
        print(f"   Patterns found: {len(patterns)}")
        print(f"   Recommendations: {len(recommendations)}")

        if jobs:
            total_cost = sum(job.cost_estimate or 0 for job in jobs)
            successful_jobs = [j for j in jobs if j.status == JobStatus.DONE]
            success_rate = (len(successful_jobs) / len(jobs)) * 100

            print(f"   Total cost: ${total_cost:.2f}")
            print(f"   Success rate: {success_rate:.1f}%")

        # Display top recommendations
        if recommendations:
            print("\n🎯 Top Recommendations:")
            for i, rec in enumerate(recommendations[:3], 1):
                print(f"   {i}. {rec.title}")
                print(f"      Type: {rec.type.value}")
                print(f"      Severity: {rec.severity.value}")
                if rec.estimated_cost_savings:
                    print(f"      Potential savings: ${rec.estimated_cost_savings:.2f}")
                if rec.estimated_performance_improvement:
                    print(f"      Performance gain: {rec.estimated_performance_improvement:.1f}%")
                print()

        # Store results in memory
        print("💾 Storing analysis in memory...")
        await analyzer.memory_system.store_jobs(jobs)
        await analyzer.memory_system.store_patterns(patterns)
        await analyzer.memory_system.store_recommendations(recommendations)
        print("✅ Analysis stored in memory")

    except Exception as e:
        logger.error(f"Error in basic analysis: {e}")
        print(f"❌ Error: {e}")


async def cost_optimization_example():
    """Cost optimization example"""
    print("💰 AI Spark Analyzer - Cost Optimization Example")
    print("=" * 50)

    try:
        # Initialize analyzer
        analyzer = SparkAnalyzer()
        print("✅ Spark Analyzer initialized")

        # Analyze cost optimization
        print("\n💸 Analyzing cost optimization opportunities...")
        cost_analysis = await analyzer.ai_engine.analyze_cost_optimization([])
        print(f"✅ Cost analysis completed")

        print(f"\n💰 Cost Analysis Summary:")
        print(f"   Total cost: ${cost_analysis.total_cost:.2f}")
        print(f"   Cost trend: {cost_analysis.cost_trend}")
        print(f"   Optimization opportunities: {len(cost_analysis.optimization_opportunities)}")
        print(f"   Potential savings: ${cost_analysis.potential_savings:.2f}")

        # Display top optimization opportunities
        if cost_analysis.optimization_opportunities:
            print("\n🎯 Top Optimization Opportunities:")
            for i, opp in enumerate(cost_analysis.optimization_opportunities[:3], 1):
                print(f"   {i}. {opp.get('opportunity', 'Unknown opportunity')}")
                print(f"      Estimated savings: ${opp.get('estimated_savings', 0):.2f}")
                print(f"      Effort: {opp.get('implementation_effort', 'medium')}")
                print(f"      Risk level: {opp.get('risk_level', 'low')}")
                print()

    except Exception as e:
        logger.error(f"Error in cost optimization analysis: {e}")
        print(f"❌ Error: {e}")


async def memory_insights_example():
    """Memory insights example"""
    print("🧠 AI Spark Analyzer - Memory Insights Example")
    print("=" * 50)

    try:
        # Initialize analyzer
        analyzer = SparkAnalyzer()
        print("✅ Spark Analyzer initialized")

        # Search memory for insights
        queries = [
            "cost optimization opportunities",
            "performance bottlenecks",
            "cluster configuration issues"
        ]

        for query in queries:
            print(f"\n🔍 Searching memory for: '{query}'")
            insights = await analyzer.get_memory_insights(query, limit=5)

            if insights:
                print(f"✅ Found {len(insights)} relevant insights:")
                for i, insight in enumerate(insights[:3], 1):
                    print(f"   {i}. {insight.get('content', 'No content')}")
                    print(f"      Relevance: {insight.get('relevance_score', 0):.1%}")
                    print(f"      Type: {insight.get('type', 'unknown')}")
            else:
                print("ℹ️  No relevant insights found")

    except Exception as e:
        logger.error(f"Error in memory insights example: {e}")
        print(f"❌ Error: {e}")


async def dashboard_example():
    """Dashboard example"""
    print("📊 AI Spark Analyzer - Dashboard Example")
    print("=" * 50)

    print("\n🌐 Starting dashboard...")
    print("   To start the dashboard, run:")
    print("   python -m src.dashboard.app")
    print("   Or use the main application:")
    print("   python -m src.main dashboard")
    print()

    print("📋 Dashboard Features:")
    print("   📊 Real-time job monitoring")
    print("   💰 Cost analysis and optimization")
    print("   🚀 Performance metrics and trends")
    print("   💡 AI-powered recommendations")
    print("   🧠 Long-term memory insights")
    print("   🔍 Interactive search and filtering")

    print("\n🎛️ Dashboard Controls:")
    print("   • Run daily analysis")
    print("   • Generate 30-day optimization plans")
    print("   • Clean up old data")
    print("   • Filter and search recommendations")
    print("   • View cost and performance trends")


async def main():
    """Main example runner"""
    print("🤖 AI Spark Analyzer - Examples")
    print("=" * 60)
    print()

    examples = [
        ("Basic Analysis", basic_analysis_example),
        ("Cost Optimization", cost_optimization_example),
        ("Memory Insights", memory_insights_example),
        ("Dashboard", dashboard_example)
    ]

    for name, example_func in examples:
        print(f"\n🎯 Running {name} Example...")
        try:
            await example_func()
            print(f"✅ {name} example completed successfully!")
        except Exception as e:
            print(f"❌ {name} example failed: {e}")

        print("\n" + "=" * 60)

    print("\n🎉 All examples completed!")
    print("\n💡 Next Steps:")
    print("   1. Set up Google Cloud credentials")
    print("   2. Configure your Dataproc clusters")
    print("   3. Run the main analyzer with your data")
    print("   4. Explore the dashboard interface")
    print("   5. Customize for your specific use cases")


if __name__ == "__main__":
    # Run examples
    asyncio.run(main())