from .web_server_utils import return_pipeline_save_file_folder
import sqlite3
from enum import Enum
import json
import threading
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from .custom_logger import setup_logger
import traceback

logger = setup_logger()


class StatusEnum(str, Enum):
    """Task status enumeration"""
    FAILED = "failed"
    DONE = "done"
    IN_PROGRESS = "in_progress"
    QUEUED = "queued"


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


class SQLiteConnection:
    """Thread-safe SQLite connection manager"""
    _instance = None
    _lock = threading.Lock()

    def __init__(self, database_path: str):
        self.database_path = database_path
        self.connection = None

    @classmethod
    def get_instance(cls, database_path: str) -> 'SQLiteConnection':
        """Get singleton instance with double-checked locking"""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = cls(database_path)
        return cls._instance

    def get_connection(self) -> sqlite3.Connection:
        """Get thread-local database connection"""
        if not hasattr(threading.current_thread(), 'db_connection'):
            connection = sqlite3.connect(
                self.database_path,
                isolation_level=None,
                check_same_thread=False
            )
            connection.row_factory = self._row_to_dict
            threading.current_thread().db_connection = connection

        return threading.current_thread().db_connection

    def _row_to_dict(self, cursor: sqlite3.Cursor, row: sqlite3.Row) -> Dict:
        """Convert SQL row to dictionary"""
        return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}

    def execute_with_retry(self, query: str, params: tuple = None,
                           max_retries: int = 3) -> Optional[sqlite3.Cursor]:
        """Execute SQL with retry logic"""
        last_error = None

        for attempt in range(max_retries):
            try:
                cursor = self.get_connection().cursor()
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                return cursor

            except sqlite3.Error as e:
                last_error = e
                logger.error(f"Database error (attempt {attempt + 1}): {str(e)}")

                if "database is locked" in str(e):
                    # Back off and retry
                    time.sleep(0.1 * (attempt + 1))
                    continue

                raise DatabaseError(f"Database error: {str(e)}")

        if last_error:
            raise DatabaseError(f"Max retries reached. Last error: {str(last_error)}")

    def cleanup(self):
        """Close thread-local connections"""
        if hasattr(threading.current_thread(), 'db_connection'):
            threading.current_thread().db_connection.close()
            del threading.current_thread().db_connection


# Initialize connection manager
connection = SQLiteConnection.get_instance(return_pipeline_save_file_folder())


def create_database():
    """Initialize database schema"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()

            # Create youtube_data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS youtube_data (
                    youtube_id TEXT,
                    ai_user_id TEXT NOT NULL,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    retry_count INTEGER DEFAULT 0,
                    error_message TEXT,
                    PRIMARY KEY (youtube_id, ai_user_id)
                )
            ''')

            # Create ai_user_data table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_user_data (
                    user_id TEXT,
                    youtube_id TEXT,
                    ai_user_id TEXT,
                    ydx_server TEXT,
                    ydx_app_host TEXT,
                    status TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (youtube_id) REFERENCES youtube_data (youtube_id),
                    UNIQUE(user_id, youtube_id, ai_user_id)
                )
            ''')

            # Create module_outputs table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS module_outputs (
                    youtube_id TEXT,
                    ai_user_id TEXT,
                    module_name TEXT,
                    output_data TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (youtube_id, ai_user_id, module_name)
                )
            ''')

            # Create processing_history table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS processing_history (
                    youtube_id TEXT,
                    ai_user_id TEXT,
                    status TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    error_message TEXT,
                    PRIMARY KEY (youtube_id, ai_user_id, start_time)
                )
            ''')

            logger.info("Database schema created successfully")

    except sqlite3.Error as e:
        logger.error(f"Error creating database: {str(e)}")
        raise DatabaseError(f"Failed to create database: {str(e)}")


def process_incoming_data(user_id: str, ydx_server: str, ydx_app_host: str,
                          ai_user_id: str, youtube_id: str) -> None:
    """Process new task data"""
    try:
        # Validate inputs
        if not all([user_id, youtube_id, ai_user_id]):
            raise ValueError("Missing required parameters")

        with connection.get_connection() as con:
            cursor = con.cursor()

            # Check if entry exists in youtube_data
            cursor.execute(
                'SELECT COUNT(*) as count FROM youtube_data WHERE youtube_id = ? AND ai_user_id = ?',
                (youtube_id, ai_user_id)
            )
            count = cursor.fetchone()['count']

            timestamp = datetime.utcnow().isoformat()

            if count == 0:
                # Insert new youtube_data entry
                cursor.execute('''
                    INSERT INTO youtube_data 
                    (youtube_id, ai_user_id, status, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                ''', (youtube_id, ai_user_id, StatusEnum.IN_PROGRESS.value,
                      timestamp, timestamp))

            # Insert or update ai_user_data
            cursor.execute('''
                INSERT OR REPLACE INTO ai_user_data 
                (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, 
                 status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host,
                  StatusEnum.IN_PROGRESS.value, timestamp, timestamp))

            # Add to processing history
            cursor.execute('''
                INSERT INTO processing_history
                (youtube_id, ai_user_id, status, start_time)
                VALUES (?, ?, ?, ?)
            ''', (youtube_id, ai_user_id, StatusEnum.IN_PROGRESS.value, timestamp))

            logger.info(f"Successfully processed data for video {youtube_id}")

    except (sqlite3.Error, ValueError) as e:
        logger.error(f"Error processing incoming data: {str(e)}")
        raise DatabaseError(f"Failed to process incoming data: {str(e)}")

def get_status_for_youtube_id(youtube_id, ai_user_id):
    try:
        youtube_id = str(youtube_id)
        ai_user_id = str(ai_user_id)
        with connection.return_connection() as con:
            cursor = con.cursor()
            logger.info(f"Executing query with youtube_id: {youtube_id}, ai_user_id: {ai_user_id} (types: {type(youtube_id)}, {type(ai_user_id)})")
            cursor.execute('''
                SELECT status FROM youtube_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))
            status = cursor.fetchone()
            logger.info(f"Query result: {status}")
            return status['status'] if status else None
    except sqlite3.Error as e:
        logger.error(f"Error getting status for YouTube ID {youtube_id} and AI User ID {ai_user_id}: {e}")
        return None

def update_status(youtube_id: str, ai_user_id: str, status: str,
                  error_message: Optional[str] = None) -> None:
    """Update task status"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            timestamp = datetime.utcnow().isoformat()

            # Update youtube_data status
            if error_message:
                cursor.execute('''
                    UPDATE youtube_data
                    SET status = ?, updated_at = ?, 
                        retry_count = retry_count + 1,
                        error_message = ?
                    WHERE youtube_id = ? AND ai_user_id = ?
                ''', (status, timestamp, error_message, youtube_id, ai_user_id))
            else:
                cursor.execute('''
                    UPDATE youtube_data
                    SET status = ?, updated_at = ?
                    WHERE youtube_id = ? AND ai_user_id = ?
                ''', (status, timestamp, youtube_id, ai_user_id))

            # Update processing history
            cursor.execute('''
                UPDATE processing_history
                SET status = ?, end_time = ?, error_message = ?
                WHERE youtube_id = ? AND ai_user_id = ? 
                AND end_time IS NULL
            ''', (status, timestamp, error_message, youtube_id, ai_user_id))

            logger.info(f"Updated status to {status} for video {youtube_id}")

    except sqlite3.Error as e:
        logger.error(f"Error updating status: {str(e)}")
        raise DatabaseError(f"Failed to update status: {str(e)}")


def update_ai_user_data(youtube_id: str, ai_user_id: str, user_id: str,
                        status: str) -> None:
    """Update AI user data status"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            timestamp = datetime.utcnow().isoformat()

            cursor.execute('''
                UPDATE ai_user_data
                SET status = ?, updated_at = ?
                WHERE youtube_id = ? AND ai_user_id = ? AND user_id = ?
            ''', (status, timestamp, youtube_id, ai_user_id, user_id))

            logger.info(f"Updated AI user data status for video {youtube_id}")

    except sqlite3.Error as e:
        logger.error(f"Error updating AI user data: {str(e)}")
        raise DatabaseError(f"Failed to update AI user data: {str(e)}")


def get_data_for_youtube_id_ai_user_id(youtube_id: str, ai_user_id: str
                                       ) -> Tuple[Optional[str], Optional[str]]:
    """Get data for specific video and AI user"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                SELECT ydx_server, ydx_app_host
                FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
                LIMIT 1
            ''', (youtube_id, ai_user_id))

            result = cursor.fetchone()
            if result:
                return result['ydx_server'], result['ydx_app_host']
            return None, None

    except sqlite3.Error as e:
        logger.error(f"Error getting data: {str(e)}")
        raise DatabaseError(f"Failed to get data: {str(e)}")


def get_pending_tasks() -> List[Dict[str, Any]]:
    """Get all pending tasks"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                SELECT youtube_data.youtube_id, youtube_data.ai_user_id,
                       youtube_data.retry_count, ai_user_data.*
                FROM youtube_data
                JOIN ai_user_data ON youtube_data.youtube_id = ai_user_data.youtube_id
                WHERE youtube_data.status = ?
            ''', (StatusEnum.IN_PROGRESS.value,))

            return cursor.fetchall()

    except sqlite3.Error as e:
        logger.error(f"Error getting pending tasks: {str(e)}")
        raise DatabaseError(f"Failed to get pending tasks: {str(e)}")


async def remove_sqlite_entry(youtube_id: str, ai_user_id: str) -> None:
    """Remove all entries for a task"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()

            # Remove from youtube_data
            cursor.execute('''
                DELETE FROM youtube_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            # Remove from ai_user_data
            cursor.execute('''
                DELETE FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            # Remove from module_outputs
            cursor.execute('''
                DELETE FROM module_outputs
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            logger.info(f"Removed all entries for video {youtube_id}")

    except sqlite3.Error as e:
        logger.error(f"Error removing entries: {str(e)}")
        raise DatabaseError(f"Failed to remove entries: {str(e)}")


def cleanup_old_entries() -> None:
    """Clean up old completed/failed entries"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            cutoff = (datetime.utcnow() - timedelta(days=7)).isoformat()

            # Clean up old completed/failed tasks
            cursor.execute('''
                DELETE FROM youtube_data
                WHERE status IN (?, ?)
                AND updated_at < ?
            ''', (StatusEnum.DONE.value, StatusEnum.FAILED.value, cutoff))

            # Clean up orphaned entries in other tables
            cursor.execute('''
                DELETE FROM ai_user_data
                WHERE youtube_id NOT IN (
                    SELECT youtube_id FROM youtube_data
                )
            ''')

            cursor.execute('''
                DELETE FROM module_outputs
                WHERE youtube_id NOT IN (
                    SELECT youtube_id FROM youtube_data
                )
            ''')

        cursor.execute('''
                        DELETE FROM module_outputs
                        WHERE youtube_id NOT IN (
                            SELECT youtube_id FROM youtube_data
                        )
                    ''')

        # Clean up old processing history
        cursor.execute('''
                        DELETE FROM processing_history
                        WHERE end_time < ?
                    ''', (cutoff,))

        logger.info("Completed database cleanup")

    except sqlite3.Error as e:
        logger.error(f"Error cleaning up old entries: {str(e)}")
        raise DatabaseError(f"Failed to clean up old entries: {str(e)}")


def update_module_output(youtube_id: str, ai_user_id: str, module_name: str,
                         output_data: Dict[str, Any]) -> None:
    """Store module processing output"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            timestamp = datetime.utcnow().isoformat()

            cursor.execute('''
                    INSERT OR REPLACE INTO module_outputs
                    (youtube_id, ai_user_id, module_name, output_data, 
                     created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (youtube_id, ai_user_id, module_name,
                      json.dumps(output_data), timestamp, timestamp))

            logger.info(f"Updated output for module {module_name}, "
                        f"video {youtube_id}")

    except sqlite3.Error as e:
        logger.error(f"Error updating module output: {str(e)}")
        raise DatabaseError(f"Failed to update module output: {str(e)}")


def get_module_output(youtube_id: str, ai_user_id: str, module_name: str
                      ) -> Optional[Dict[str, Any]]:
    """Retrieve module processing output"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                    SELECT output_data
                    FROM module_outputs
                    WHERE youtube_id = ? AND ai_user_id = ? AND module_name = ?
                ''', (youtube_id, ai_user_id, module_name))

            result = cursor.fetchone()
            if result and result['output_data']:
                return json.loads(result['output_data'])
            return None

    except sqlite3.Error as e:
        logger.error(f"Error getting module output: {str(e)}")
        raise DatabaseError(f"Failed to get module output: {str(e)}")


def get_processing_stats() -> Dict[str, Any]:
    """Get processing statistics"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            stats = {}

            # Get counts by status
            cursor.execute('''
                    SELECT status, COUNT(*) as count
                    FROM youtube_data
                    GROUP BY status
                ''')
            stats['status_counts'] = {
                row['status']: row['count']
                for row in cursor.fetchall()
            }

            # Get average processing time
            cursor.execute('''
                    SELECT AVG(
                        ROUND(
                            (JULIANDAY(end_time) - JULIANDAY(start_time)) * 86400
                        )
                    ) as avg_time
                    FROM processing_history
                    WHERE status = ? AND end_time IS NOT NULL
                ''', (StatusEnum.DONE.value,))
            result = cursor.fetchone()
            stats['avg_processing_time'] = result['avg_time'] if result else None

            # Get recent error count
            cursor.execute('''
                    SELECT COUNT(*) as count
                    FROM processing_history
                    WHERE status = ?
                    AND start_time > datetime('now', '-1 day')
                ''', (StatusEnum.FAILED.value,))
            stats['recent_errors'] = cursor.fetchone()['count']

            return stats

    except sqlite3.Error as e:
        logger.error(f"Error getting processing stats: {str(e)}")
        raise DatabaseError(f"Failed to get processing stats: {str(e)}")


def get_task_details(youtube_id: str, ai_user_id: str) -> Optional[Dict[str, Any]]:
    """Get detailed information about a specific task"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()

            # Get main task data
            cursor.execute('''
                    SELECT youtube_data.*, ai_user_data.*
                    FROM youtube_data
                    LEFT JOIN ai_user_data 
                        ON youtube_data.youtube_id = ai_user_data.youtube_id
                        AND youtube_data.ai_user_id = ai_user_data.ai_user_id
                    WHERE youtube_data.youtube_id = ? 
                    AND youtube_data.ai_user_id = ?
                ''', (youtube_id, ai_user_id))

            task_data = cursor.fetchone()
            if not task_data:
                return None

            # Get processing history
            cursor.execute('''
                    SELECT start_time, end_time, status, error_message
                    FROM processing_history
                    WHERE youtube_id = ? AND ai_user_id = ?
                    ORDER BY start_time DESC
                ''', (youtube_id, ai_user_id))

            task_data['processing_history'] = cursor.fetchall()

            # Get module outputs
            cursor.execute('''
                    SELECT module_name, output_data
                    FROM module_outputs
                    WHERE youtube_id = ? AND ai_user_id = ?
                ''', (youtube_id, ai_user_id))

            task_data['module_outputs'] = {
                row['module_name']: json.loads(row['output_data'])
                for row in cursor.fetchall()
            }

            return task_data

    except sqlite3.Error as e:
        logger.error(f"Error getting task details: {str(e)}")
        raise DatabaseError(f"Failed to get task details: {str(e)}")


def get_stalled_tasks(timeout_minutes: int = 60) -> List[Dict[str, Any]]:
    """Get tasks that haven't been updated recently"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            cutoff = (datetime.utcnow() -
                      timedelta(minutes=timeout_minutes)).isoformat()

            cursor.execute('''
                    SELECT youtube_data.*, ai_user_data.*
                    FROM youtube_data
                    JOIN ai_user_data 
                        ON youtube_data.youtube_id = ai_user_data.youtube_id
                        AND youtube_data.ai_user_id = ai_user_data.ai_user_id
                    WHERE youtube_data.status = ?
                    AND youtube_data.updated_at < ?
                ''', (StatusEnum.IN_PROGRESS.value, cutoff))

            return cursor.fetchall()

    except sqlite3.Error as e:
        logger.error(f"Error getting stalled tasks: {str(e)}")
        raise DatabaseError(f"Failed to get stalled tasks: {str(e)}")


def reset_stalled_task(youtube_id: str, ai_user_id: str) -> None:
    """Reset a stalled task for retry"""
    try:
        with connection.get_connection() as con:
            cursor = con.cursor()
            timestamp = datetime.utcnow().isoformat()

            # Update status and increment retry count
            cursor.execute('''
                    UPDATE youtube_data
                    SET status = ?, updated_at = ?, retry_count = retry_count + 1
                    WHERE youtube_id = ? AND ai_user_id = ?
                ''', (StatusEnum.QUEUED.value, timestamp, youtube_id, ai_user_id))

            # Add new processing history entry
            cursor.execute('''
                    INSERT INTO processing_history
                    (youtube_id, ai_user_id, status, start_time)
                    VALUES (?, ?, ?, ?)
                ''', (youtube_id, ai_user_id, StatusEnum.QUEUED.value, timestamp))

            logger.info(f"Reset stalled task for video {youtube_id}")

    except sqlite3.Error as e:
        logger.error(f"Error resetting stalled task: {str(e)}")
        raise DatabaseError(f"Failed to reset stalled task: {str(e)}")


# Register cleanup on module exit
import atexit


@atexit.register
def cleanup_connections():
    """Clean up database connections on exit"""
    try:
        connection.cleanup()
        logger.info("Database connections cleaned up")
    except Exception as e:
        logger.error(f"Error cleaning up connections: {str(e)}")