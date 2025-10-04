"""Initializes the AI module.

This module contains the core AI engines for analyzing Spark jobs.
It exposes the `AIEngine` and `AgenticAIEngine` for different
analysis modes.
"""

from .agentic_engine import AgenticAIEngine
from .engine import AIEngine

__all__ = ["AIEngine", "AgenticAIEngine"]