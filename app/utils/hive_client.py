"""
Hive client utilities.

Note: Hive client functionality has been moved to hadoop_client.py
This file is kept for backward compatibility.
"""

from .hadoop_client import HiveClient

# Re-export for backward compatibility
__all__ = ["HiveClient"]