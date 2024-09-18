from .web_server_utils import return_pipeline_save_file_folder
import sqlite3
from enum import Enum
from .custom_logger import setup_logger
import traceback

logger = setup_logger()

# Define an Enum for the status options
class StatusEnum(str, Enum):
    failed = "failed"
    done = "done"
    in_progress = "in_progress"

class SQLiteConnection:
    def __init__(self, database):
        self.database = database
        self.connection = sqlite3.connect(self.database,isolation_level=None,check_same_thread=False)
        self.connection.row_factory = self.row_to_dict

    def row_to_dict(self,cursor: sqlite3.Cursor, row: sqlite3.Row) -> dict:
        data = {}
        for idx, col in enumerate(cursor.description):
            data[col[0]] = row[idx]
        return data

    def __exit__(self, exc_type, exc_value, traceback):
        if self.connection:
            self.connection.close()
    def return_connection(self):
        return self.connection

connection = SQLiteConnection(return_pipeline_save_file_folder())
connection.connection.execute('PRAGMA autocommit = ON')

def create_database():
    # Create a new SQLite database (or connect to an existing one)
    with connection.return_connection() as con:
        try:
            cursor = con.cursor()

            # Create tables if they don't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS youtube_data (
                    youtube_id TEXT,
                    ai_user_id TEXT NOT NULL,
                    status TEXT,
                    PRIMARY KEY (youtube_id, ai_user_id)
                )
            ''')

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

            logger.info("Database created successfully.")
        except sqlite3.Error as e:
            print("Error creating database:", e)
            logger.error("Error creating database:", e)
            return
    return


def get_pending_jobs_with_youtube_ids():
    try:
        # Get a connection from the pool (a temporary connection)
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Select pending jobs' youtube_id and ai_user_id
            cursor.execute('''
                SELECT youtube_data.youtube_id, youtube_data.ai_user_id
                FROM youtube_data
                WHERE youtube_data.status = ?
            ''', (StatusEnum.in_progress.value,))
            pending_jobs_with_youtube_ids = cursor.fetchall()
            return pending_jobs_with_youtube_ids
    except sqlite3.Error as e:
        print("Error getting pending jobs with YouTube IDs:", e)
        logger.error("Error getting pending jobs with YouTube IDs:", e)
        return []


def get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Select all data for the specified YouTube ID and AI user ID
            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))
            
            data = cursor.fetchall()

            return data
    except sqlite3.Error as e:
        logger.error("Error getting data get_data_for_youtube_id_and_user_id:", e)
        print("Error getting data for YouTube ID and AI user ID:", e)
        return []
    

def get_data_for_youtube_id_ai_user_id(youtube_id,ai_user_id):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Select the status for the specified YouTube ID
            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id,ai_user_id))
            
            status = cursor.fetchone()
            ydx_server = status['ydx_server']
            ydx_app_host = status['ydx_app_host']
            return ydx_server,ydx_app_host
    except sqlite3.Error as e:
        logger.error("Error getting get_data_for_youtube_id_ai_user_id ", str(e))
        print("Error getting status for YouTube ID:", e)
        return None


def get_status_for_youtube_id(youtube_id,ai_user_id):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Select the status for the specified YouTube ID
            cursor.execute('''
                SELECT status FROM youtube_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id,ai_user_id))
            
            status = cursor.fetchone()
        
            return status[0] if status else None
    except sqlite3.Error as e:
        print("Error getting status for YouTube ID:", e)
        return None


def process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id):
    try:
        print("Inside Process Incoming Data")
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Check if entry exists in youtube_data
            cursor.execute('SELECT COUNT(*) as count FROM youtube_data WHERE youtube_id = ? AND ai_user_id = ?',
                           (youtube_id, ai_user_id))
            count = cursor.fetchone()

            if count['count'] == 0:
                print("Inside IF")
                # Insert into youtube_data if not exists
                cursor.execute('INSERT INTO youtube_data (youtube_id, ai_user_id, status) VALUES (?, ?, ?)',
                               (youtube_id, ai_user_id, StatusEnum.in_progress.value))

            # Use INSERT OR REPLACE for ai_user_data
            cursor.execute('''
                INSERT OR REPLACE INTO ai_user_data 
                (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, status) 
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, StatusEnum.in_progress.value))

        con.commit()
    except sqlite3.Error as e:
        print("Error ", e)
        logger.error(f"SQLite error in process_incoming_data: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in process_incoming_data: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise

def update_status(youtube_id, ai_user_id, status):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()
            # Update the status for the specified YouTube ID and AI user ID
            cursor.execute('''
                UPDATE youtube_data
                SET status = ?
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (status, youtube_id, ai_user_id,))
    except sqlite3.Error as e:
        print("Error updating status:", e)
        logger.error("Error updating status:", e)
        
        

def update_ai_user_data(youtube_id, ai_user_id,user_id, status):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()
            # Update the status for the specified YouTube ID and AI user ID
            cursor.execute('''
                UPDATE ai_user_data
                SET status = ?
                WHERE youtube_id = ? AND ai_user_id = ? AND user_id = ?
            ''', (status, youtube_id, ai_user_id,user_id,))
    except sqlite3.Error as e:
        print("Error updating status:", e)
        logger.error("Error updating status:", e)
        
        

def return_all_user_data_for_youtube_id_ai_user_id(youtube_id,ai_user_id):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Select the status for the specified YouTube ID
            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id,ai_user_id))
            
            status = cursor.fetchall()
            return status
    except sqlite3.Error as e:
        print("Error getting status for YouTube ID:", e)
        logger.error("Error getting status for YouTube ID:", e)
        return None


async def remove_sqlite_entry(youtube_id: str, ai_user_id: str):
    try:
        with connection.return_connection() as con:
            cursor = con.cursor()

            # Remove entry from youtube_data
            cursor.execute('''
                DELETE FROM youtube_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

            # Remove entries from ai_user_data
            cursor.execute('''
                DELETE FROM ai_user_data
                WHERE youtube_id = ? AND ai_user_id = ?
            ''', (youtube_id, ai_user_id))

        logger.info(f"Removed SQLite entries for YouTube ID: {youtube_id}, AI User ID: {ai_user_id}")
    except sqlite3.Error as e:
        logger.error(f"Error removing SQLite entries: {str(e)}")