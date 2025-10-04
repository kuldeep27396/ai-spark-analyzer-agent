"""Initializes the Dataproc module.

This module provides the necessary tools for interacting with Google Cloud
Dataproc. It exposes the `DataprocClient` for easy access to Dataproc jobs
and cluster information.
"""

from .client import DataprocClient

__all__ = ["DataprocClient"]