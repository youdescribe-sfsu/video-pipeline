import sqlite3
from enum import Enum
from typing import Dict, Any, List, Optional, Union
import logging
import traceback
import json
from datetime import datetime, timedelta
import asyncio
from contextlib import asynccontextmanager
import os
from .web_server_utils import return_pipeline_save_file_folder
from .custom_logger import setup_logger

# Setup logger
logger = setup_logger()


class StatusEnum(str, Enum):
    """Pipeline status enumeration"""
    failed = "failed"
    done = "done"
    in_progress = "in_progress"
    pending = "pending"


class DatabaseError(Exception):
    """Custom exception for database errors"""

    def __init__(self, message: str, details: Optional[Dict] = None):
        self.message = message
        self.details = details or {}
        self.timestamp = datetime.utcnow()
        super().__init__(self.message)


class ConnectionManager:
    """Manage database connections with connection pooling"""

    def __init__(self, database_path: str, max_connections: int = 10):
        self.database_path = database_path
        self.max_connections = max_connections
        self.connections: List[sqlite3.Connection] = []
        self.available_connections = asyncio.Queue(maxsize=max_connections)
        self._lock = asyncio.Lock()

    def _create_connection(self) -> sqlite3.Connection:
        """Create a new database connection"""
        connection = sqlite3.connect(
            self.database_path,
            isolation_level=None,
            check_same_thread=False
        )
        connection.row_factory = self._dict_factory
        return connection

    @staticmethod
    def _dict_factory(cursor: sqlite3.Cursor, row: tuple) -> Dict:
        """Convert SQLite row to dictionary"""
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

    async def get_connection(self) -> sqlite3.Connection:
        """Get a connection from the pool"""
        try:
            return await self.available_connections.get()
        except asyncio.QueueEmpty:
            async with self._lock:
                if len(self.connections) < self.max_connections:
                    conn = self._create_connection()
                    self.connections.append(conn)
                    return conn
                else:
                    return await self.available_connections.get()

    async def release_connection(self, connection: sqlite3.Connection):
        """Return a connection to the pool"""
        await self.available_connections.put(connection)

    async def close_all(self):
        """Close all connections"""
        for conn in self.connections:
            conn.close()
        self.connections.clear()


class DatabaseManager:
    """Database operations manager"""

    def __init__(self, database_path: Optional[str] = None):
        self.database_path = database_path or return_pipeline_save_file_folder()
        self.connection_manager = ConnectionManager(self.database_path)
        self.logger = logging.getLogger("DatabaseManager")

    @asynccontextmanager
    async def get_db(self):
        """Context manager for database connections"""
        connection = await self.connection_manager.get_connection()
        try:
            yield connection
        finally:
            await self.connection_manager.release_connection(connection)

    async def execute(self, query: str, params: tuple = (), fetch: bool = False) -> Optional[Union[List[Dict], Dict]]:
        """Execute a database query"""
        async with self.get_db() as db:
            try:
                cursor = db.cursor()
                cursor.execute(query, params)

                if fetch:
                    return cursor.fetchall()

                db.commit()
                return None
            except sqlite3.Error as e:
                db.rollback()
                self.logger.error(f"Database error: {e}")
                self.logger.error(f"Query: {query}")
                self.logger.error(f"Parameters: {params}")
                raise DatabaseError(str(e), {
                    "query": query,
                    "params": params,
                    "traceback": traceback.format_exc()
                })

    async def execute_transaction(self, queries: List[tuple]) -> None:
        """Execute multiple queries in a transaction"""
        async with self.get_db() as db:
            try:
                cursor = db.cursor()
                cursor.execute("BEGIN TRANSACTION")

                for query, params in queries:
                    cursor.execute(query, params)

                db.commit()
            except sqlite3.Error as e:
                db.rollback()
                self.logger.error(f"Transaction error: {e}")
                raise DatabaseError(str(e), {
                    "queries": queries,
                    "traceback": traceback.format_exc()
                })


# Initialize database manager
db_manager = DatabaseManager()


async def create_database() -> None:
    """Create necessary database tables"""
    try:
        queries = [
            ('''
            CREATE TABLE IF NOT EXISTS youtube_data (
                youtube_id TEXT,
                ai_user_id TEXT NOT NULL,
                status TEXT,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (youtube_id, ai_user_id)
            )
            ''', ()),
            ('''
            CREATE TABLE IF NOT EXISTS ai_user_data (
                user_id TEXT,
                youtube_id TEXT,
                ai_user_id TEXT,
                ydx_server TEXT,
                ydx_app_host TEXT,
                status TEXT,
                metadata TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (youtube_id, ai_user_id) REFERENCES youtube_data (youtube_id, ai_user_id),
                UNIQUE(user_id, youtube_id, ai_user_id)
            )
            ''', ()),
            ('''
            CREATE TABLE IF NOT EXISTS module_outputs (
                youtube_id TEXT,
                ai_user_id TEXT,
                module_name TEXT,
                output_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (youtube_id, ai_user_id, module_name),
                FOREIGN KEY (youtube_id, ai_user_id) REFERENCES youtube_data (youtube_id, ai_user_id)
            )
            ''', ())
        ]
        await db_manager.execute_transaction(queries)
        logger.info("Database created successfully")
    except DatabaseError as e:
        logger.error(f"Error creating database: {e}")
        raise


async def process_incoming_data(
        user_id: str,
        ydx_server: str,
        ydx_app_host: str,
        ai_user_id: str,
        youtube_id: str
) -> None:
    """Process and store incoming request data"""
    try:
        metadata = {
            "request_time": datetime.utcnow().isoformat(),
            "ydx_server": ydx_server,
            "ydx_app_host": ydx_app_host
        }

        queries = [
            (
                '''
                INSERT OR REPLACE INTO youtube_data (youtube_id, ai_user_id, status, metadata)
                VALUES (?, ?, ?, ?)
                ''',
                (youtube_id, ai_user_id, StatusEnum.in_progress.value, json.dumps(metadata))
            ),
            (
                '''
                INSERT OR REPLACE INTO ai_user_data 
                (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, status, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''',
                (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host,
                 StatusEnum.in_progress.value, json.dumps(metadata))
            )
        ]

        await db_manager.execute_transaction(queries)
        logger.info(f"Processed incoming data for YouTube ID: {youtube_id}")
    except DatabaseError as e:
        logger.error(f"Error processing incoming data: {e}")
        raise


async def update_status(youtube_id: str, ai_user_id: str, status: str) -> None:
    """Update status for a video process"""
    try:
        metadata = {
            "status_updated_at": datetime.utcnow().isoformat(),
            "status": status
        }

        query = '''
            UPDATE youtube_data 
            SET status = ?, metadata = ?, updated_at = CURRENT_TIMESTAMP
            WHERE youtube_id = ? AND ai_user_id = ?
        '''

        await db_manager.execute(query, (status, json.dumps(metadata), youtube_id, ai_user_id))
        logger.info(f"Updated status to {status} for YouTube ID: {youtube_id}")
    except DatabaseError as e:
        logger.error(f"Error updating status: {e}")
        raise


async def update_ai_user_data(
        youtube_id: str,
        ai_user_id: str,
        user_id: str,
        status: str
) -> None:
    """Update AI user data status"""
    try:
        query = '''
            UPDATE ai_user_data 
            SET status = ?, updated_at = CURRENT_TIMESTAMP
            WHERE youtube_id = ? AND ai_user_id = ? AND user_id = ?
        '''
        await db_manager.execute(query, (status, youtube_id, ai_user_id, user_id))
        logger.info(f"Updated AI user data for YouTube ID: {youtube_id}")
    except DatabaseError as e:
        logger.error(f"Error updating AI user data: {e}")
        raise


async def get_status_for_youtube_id(youtube_id: str, ai_user_id: str) -> Optional[Dict]:
    """Get current status for a video process"""
    try:
        query = '''
            SELECT status, metadata, updated_at
            FROM youtube_data
            WHERE youtube_id = ? AND ai_user_id = ?
        '''
        results = await db_manager.execute(query, (youtube_id, ai_user_id), fetch=True)
        return results[0] if results else None
    except DatabaseError as e:
        logger.error(f"Error getting status: {e}")
        raise


async def get_data_for_youtube_id_ai_user_id(
        youtube_id: str,
        ai_user_id: str
) -> Optional[tuple]:
    """Get YDX server and app host info"""
    try:
        query = '''
            SELECT ydx_server, ydx_app_host
            FROM ai_user_data
            WHERE youtube_id = ? AND ai_user_id = ?
            LIMIT 1
        '''
        results = await db_manager.execute(query, (youtube_id, ai_user_id), fetch=True)
        if results:
            return results[0]["ydx_server"], results[0]["ydx_app_host"]
        return None
    except DatabaseError as e:
        logger.error(f"Error getting data: {e}")
        raise


async def get_data_for_youtube_id_and_user_id(
        youtube_id: str,
        ai_user_id: str
) -> List[Dict]:
    """Get all data for a video and user"""
    try:
        query = '''
            SELECT * FROM ai_user_data
            WHERE youtube_id = ? AND ai_user_id = ?
        '''
        return await db_manager.execute(query, (youtube_id, ai_user_id), fetch=True)
    except DatabaseError as e:
        logger.error(f"Error getting user data: {e}")
        raise


async def remove_sqlite_entry(youtube_id: str, ai_user_id: str) -> None:
    """Remove all database entries for a video"""
    try:
        queries = [
            (
                "DELETE FROM module_outputs WHERE youtube_id = ? AND ai_user_id = ?",
                (youtube_id, ai_user_id)
            ),
            (
                "DELETE FROM ai_user_data WHERE youtube_id = ? AND ai_user_id = ?",
                (youtube_id, ai_user_id)
            ),
            (
                "DELETE FROM youtube_data WHERE youtube_id = ? AND ai_user_id = ?",
                (youtube_id, ai_user_id)
            )
        ]
        await db_manager.execute_transaction(queries)
        logger.info(f"Removed entries for YouTube ID: {youtube_id}")
    except DatabaseError as e:
        logger.error(f"Error removing entries: {e}")
        raise


async def cleanup_stale_entries(hours: int = 24) -> None:
    """Clean up old entries"""
    try:
        cutoff_time = (datetime.utcnow() - timedelta(hours=hours)).isoformat()

        queries = [
            (
                "DELETE FROM module_outputs WHERE updated_at < ?",
                (cutoff_time,)
            ),
            (
                "DELETE FROM ai_user_data WHERE updated_at < ?",
                (cutoff_time,)
            ),
            (
                "DELETE FROM youtube_data WHERE updated_at < ? AND status != ?",
                (cutoff_time, StatusEnum.done.value)
            )
        ]
        await db_manager.execute_transaction(queries)
        logger.info("Cleaned up stale entries")
    except DatabaseError as e:
        logger.error(f"Error cleaning up stale entries: {e}")
        raise


# Background cleanup task
async def run_periodic_cleanup():
    """Run cleanup periodically"""
    while True:
        try:
            await cleanup_stale_entries()
            await asyncio.sleep(3600)  # Run every hour
        except Exception as e:
            logger.error(f"Error in periodic cleanup: {e}")
            await asyncio.sleep(300)  # Retry after 5 minutes