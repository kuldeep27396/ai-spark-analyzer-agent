"""
AI-Powered Spark Analyzer for Dataproc

An intelligent platform for analyzing Spark jobs, maintaining long-term memory,
and providing AI-driven optimization recommendations.
"""

__version__ = "1.0.0"
__author__ = "AI Spark Analyzer Team"
__email__ = "team@ai-spark-analyzer.com"

from .core.analyzer import SparkAnalyzer
from .core.config import Config
from .ai.engine import AIEngine
from .memory.langgraph_memory import LangGraphMemory

__all__ = [
    "SparkAnalyzer",
    "Config",
    "AIEngine",
    "LangGraphMemory"
]