"""
TODO 数据仓库操作的Hive服务。 该服务提供与Hive交互的操作，包括数据库管理、表操作和元数据查询。
"""

import asyncio
import datetime
from typing import Dict, List, Optional

from app.utils.hadoop_client import HiveClient
from config.settings import settings


class HiveService:
    """Service for Hive data warehouse operations."""

    def __init__(self):
        """Initialize Hive service."""
        self.hive_client = HiveClient()

    async def get_databases_summary(self) -> Dict:
        """
        Get summary of all Hive databases.

        Returns:
            Dict: Database summary including counts by layer
        """
        databases = await asyncio.to_thread(self.hive_client.get_databases)
        layer_counts = await asyncio.to_thread(self.hive_client.get_tables_count_by_layer)
        total_tables = await asyncio.to_thread(self.hive_client.get_total_tables_count)
        business_systems = await asyncio.to_thread(self.hive_client.get_business_systems_count)

        return {
            'total_databases': len(databases),
            'business_systems': business_systems,
            'total_tables': total_tables,
            'layer_distribution': layer_counts,
            'databases': databases,
            'last_updated': datetime.now()
        }

    async def get_database_info(self, database_name: str) -> Optional[Dict]:
        """
        Get detailed information about a specific database.

        Args:
            database_name: Name of the database

        Returns:
            Dict: Database information including tables and metadata
        """
        try:
            # Execute query to get tables in the database
            tables_result = await asyncio.to_thread(
                self.hive_client.execute_query,
                f"SHOW TABLES FROM {database_name}"
            )

            tables = [row.get('tab_name', row.get('table_name', '')) for row in tables_result]

            return {
                'database_name': database_name,
                'table_count': len(tables),
                'tables': tables,
                'created_time': None,  # Would need additional query
                'last_updated': datetime.now()
            }

        except Exception as e:
            return None

    async def execute_query(self, query: str) -> List[Dict]:
        """
        Execute a Hive SQL query.

        Args:
            query: SQL query to execute

        Returns:
            List[Dict]: Query results
        """
        return await asyncio.to_thread(self.hive_client.execute_query, query)