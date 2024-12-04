from .web_server_utils import return_pipeline_save_file_folder
import sqlite3
from enum import Enum
from .custom_logger import setup_logger
import traceback
import json

logger = setup_logger()

# Define an Enum for the status options
class StatusEnum(str, Enum):
    failed = "failed"
    done = "done"
    in_progress = "in_progress"

class SQLiteConnection:
    def __init__(self, database):
        self.database = database
        self.connection = sqlite3.connect(self.database, isolation_level=None, check_same_thread=False)
        self.connection.row_factory = self.row_to_dict

    def row_to_dict(self, cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict:
        data = {}
        for idx, col in enumerate(cursor.description):
            data[col[0]] = row[idx]
        return data

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()

    def return_connection(self):
        return self.connection

# Initialize the connection
print(return_pipeline_save_file_folder()) # Debugging line
connection = SQLiteConnection(return_pipeline_save_file_folder())
connection.connection.execute('PRAGMA autocommit = ON')

def create_database():
    """
    Creates necessary tables if they don't exist.
    """
    with connection.return_connection() as con:
        try:
            cursor = con.cursor()

            # Create youtube_data table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS youtube_data (
                    youtube_id TEXT,
                    ai_user_id TEXT NOT NULL,
                    status TEXT,
                    PRIMARY KEY (youtube_id, ai_user_id)
                )
            ''')

            # Create ai_user_data table if it doesn't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_user_data (
                    user_id TEXT,
                    youtube_id TEXT,
                    ai_user_id TEXT,
                    ydx_server TEXT,
                    ydx_app_host TEXT,
                    status TEXT,
                    FOREIGN KEY (youtube_id) REFERENCES youtube_data (youtube_id),
                    UNIQUE(user_id, youtube_id, ai_user_id)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS module_outputs (
                    youtube_id TEXT,
                    ai_user_id TEXT,
                    module_name TEXT,
                    output_data TEXT,  -- JSON-like structure to store output values
                    PRIMARY KEY (youtube_id, ai_user_id, module_name)
                );
            ''')

            logger.info("Database created successfully.")
        except sqlite3.Error as e:
            logger.error(f"Error creating database: {e}")

def get_pending_jobs_with_youtube_ids():
    """
    Retrieves the YouTube IDs and AI User IDs for pending jobs.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                SELECT youtube_data.youtube_id, youtube_data.ai_user_id
                FROM youtube_data
                WHERE youtube_data.status = ?
            ''', (StatusEnum.in_progress.value,))
            pending_jobs_with_youtube_ids = cursor.fetchall()
            return pending_jobs_with_youtube_ids
    except sqlite3.Error as e:
        logger.error(f"Error getting pending jobs with YouTube IDs: {e}")
        return []

def get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id):
    """
    Retrieves all data for the specified YouTube ID and AI User ID.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            data = cursor.fetchall()
            return data
    except sqlite3.Error as e:
        logger.error(f"Error getting data for YouTube ID and AI User ID: {e}")
        return []

def get_data_for_youtube_id_ai_user_id(youtube_id, ai_user_id):
    """
    Retrieves the status for the specified YouTube ID and AI User ID.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            status = cursor.fetchone()
            return status['ydx_server'], status['ydx_app_host'] if status else None
    except sqlite3.Error as e:
        logger.error(f"Error getting status for YouTube ID and AI User ID: {e}")
        return None

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

def process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id):
    """
    Processes incoming data and updates the database for new tasks.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Check if entry exists in youtube_data
            cursor.execute('SELECT COUNT(*) as count FROM youtube_data WHERE youtube_id = ? AND ai_user_id = ?',
                           (youtube_id, ai_user_id))
            count = cursor.fetchone()

            if count['count'] == 0:
                # Insert into youtube_data if not exists
                cursor.execute('INSERT INTO youtube_data (youtube_id, ai_user_id, status) VALUES (?, ?, ?)',
                               (youtube_id, ai_user_id, StatusEnum.in_progress.value))

            # Insert or replace in ai_user_data
            cursor.execute('''
                INSERT OR REPLACE INTO ai_user_data 
                (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, status) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, StatusEnum.in_progress.value))

        con.commit()
    except sqlite3.Error as e:
        logger.error(f"SQLite error in process_incoming_data: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in process_incoming_data: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")

def update_status(youtube_id, ai_user_id, status):
    try:
        # Ensure all parameters are strings and not None
        youtube_id = str(youtube_id) if youtube_id is not None else None
        ai_user_id = str(ai_user_id) if ai_user_id is not None else None
        status = str(status) if status is not None else None

        if youtube_id is None or ai_user_id is None or status is None:
            logger.error(f"Cannot update status with None values. YouTube ID: {youtube_id}, AI User ID: {ai_user_id}, Status: {status}")
            return

        with connection.return_connection() as con:
            cursor = con.cursor()
            logger.info(f"Updating status for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}, New Status: {status}")
            cursor.execute('''
                UPDATE youtube_data
                SET status = ?
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (status, youtube_id, ai_user_id))
            logger.info(f"Status update complete. Rows affected: {cursor.rowcount}")
    except sqlite3.Error as e:
        logger.error(f"SQLite error updating status for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}, Status: {status}")
        logger.error(f"Error details: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in update_status: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")

def update_ai_user_data(youtube_id, ai_user_id, user_id, status):
    """
    Updates the status of the ai_user_data table.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                UPDATE ai_user_data
                SET status = ?
                WHERE youtube_id = ? AND ai_user_id = ? AND user_id = ?
            ''', (status, youtube_id, ai_user_id, user_id))
    except sqlite3.Error as e:
        logger.error(f"Error updating status in ai_user_data: {e}")

def return_all_user_data_for_youtube_id_ai_user_id(youtube_id, ai_user_id):
    """
    Returns all the user data for a particular youtube_id and ai_user_id.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            data = cursor.fetchall()
            return data
    except sqlite3.Error as e:
        logger.error(f"Error getting data for YouTube ID and AI User ID: {e}")
        return None

def update_module_output(youtube_id, ai_user_id, module_name, output_data):
    """
    Store the output of a module in the database for future use.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO module_outputs (youtube_id, ai_user_id, module_name, output_data)
                VALUES (?, ?, ?, ?)
            ''', (youtube_id, ai_user_id, module_name, json.dumps(output_data)))
            logger.info(f"Updated module output for {module_name} in the database.")
    except sqlite3.Error as e:
        logger.error(f"Error updating module output: {e}")

def get_module_output(youtube_id, ai_user_id, module_name):
    """
    Retrieve the output of a module from the database.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()
            cursor.execute('''
                SELECT output_data FROM module_outputs
                WHERE youtube_id = ? AND ai_user_id = ? AND module_name = ?
            ''', (youtube_id, ai_user_id, module_name))
            result = cursor.fetchone()
            return json.loads(result['output_data']) if result else None
    except sqlite3.Error as e:
        logger.error(f"Error getting module output: {e}")
        return None

async def remove_sqlite_entry(youtube_id: str, ai_user_id: str):
    """
    Removes the SQLite entry for the given YouTube ID and AI User ID.
    """
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            cursor.execute('''
                DELETE FROM youtube_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            cursor.execute('''
                DELETE FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

        logger.info(f"Removed SQLite entries for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")
    except sqlite3.Error as e:
        logger.error(f"Error removing SQLite entries: {e}")
