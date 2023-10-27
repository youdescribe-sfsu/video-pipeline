from tinydb import TinyDB, Query
from web_server_utils import return_pipeline_save_file_folder
import sqlite3
import sqlite3
from cuttlepool import CuttlePool

class SQLitePool(CuttlePool):
    def normalize_resource(self, resource):
        resource.row_factory = None

    def ping(self, resource):
        try:
            with resource:
                # Use a context manager to ensure the connection is properly closed
                cursor = resource.cursor()
                cursor.execute('SELECT 1')
                result = cursor.fetchall()
                return (1,) in result
        except sqlite3.Error:
            return False

# Create a connection pool with the custom SQLitePool class
pool = SQLitePool(factory=sqlite3.connect, database=return_pipeline_save_file_folder(), capacity=10)

def create_database():
    # Create a new SQLite database (or connect to an existing one)
    with pool.get_resource() as con:
            cursor = con.cursor()

            # Create tables if they don't exist
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS youtube_data (
                    youtube_id TEXT,
                    AI_USER_ID TEXT NOT NULL,
                    status TEXT,
                    PRIMARY KEY (youtube_id, AI_USER_ID)
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS ai_user_data (
                    user_id TEXT,
                    youtube_id TEXT,
                    AI_USER_ID TEXT,
                    ydx_server TEXT,
                    ydx_app_host TEXT,
                    status TEXT,
                    FOREIGN KEY (youtube_id) REFERENCES youtube_data (youtube_id)
                )
            ''')

        # The temporary connection is automatically returned to the pool

            print("Database created successfully.")
    return


def get_pending_jobs_with_youtube_ids():
    try:
        # Get a connection from the pool (a temporary connection)
        with pool.get_resource() as con:
            cursor = con.cursor()

            # Select pending jobs' youtube_id and AI_USER_ID
            cursor.execute('''
                SELECT ai_user_data.youtube_id, ai_user_data.AI_USER_ID
                FROM ai_user_data
                INNER JOIN youtube_data ON ai_user_data.youtube_id = youtube_data.youtube_id
                WHERE ai_user_data.status = "pending"
            ''')
            pending_jobs_with_youtube_ids = cursor.fetchall()

            return pending_jobs_with_youtube_ids
    except sqlite3.Error as e:
        print("Error getting pending jobs with YouTube IDs:", e)
        return []


def get_data_for_youtube_id_and_user_id(youtube_id, ai_user_id):
    try:
        with pool.get_resource() as con:
            cursor = con.cursor()

            # Select all data for the specified YouTube ID and AI user ID
            cursor.execute('''
                SELECT * FROM ai_user_data
                WHERE youtube_id = ? AND AI_USER_ID = ?
            ''', (youtube_id, ai_user_id))
            
            data = cursor.fetchall()

            return data
    except sqlite3.Error as e:
        print("Error getting data for YouTube ID and AI user ID:", e)
        return []


def get_status_for_youtube_id(youtube_id,ai_user_id):
    try:
        with pool.get_resource() as con:
            cursor = con.cursor()

            # Select the status for the specified YouTube ID
            cursor.execute('''
                SELECT status FROM youtube_data
                WHERE youtube_id = ? AND AI_USER_ID = ?
            ''', (youtube_id,ai_user_id))
            
            status = cursor.fetchone()

            return status[0] if status else None
    except sqlite3.Error as e:
        print("Error getting status for YouTube ID:", e)
        return None


def process_incoming_data(user_id, ydx_server, ydx_app_host, ai_user_id, youtube_id):
    try:

        # Create a connection pool with the custom SQLitePool class
        pool = SQLitePool(factory=sqlite3.connect, database='youtube_data.db')

        with pool.get_resource() as con:
            cursor = con.cursor()

            # Check if the (youtube_id, AI_USER_ID) combination exists in the database
            cursor.execute('SELECT COUNT(*) FROM youtube_data WHERE youtube_id = ? AND AI_USER_ID = ?', (youtube_id, ai_user_id))
            count = cursor.fetchone()[0]

            if count == 0:
                # If the combination does not exist, add it to the database and add (youtube_id, ai_user_id) to the queue
                cursor.execute('INSERT INTO youtube_data (youtube_id, AI_USER_ID, status) VALUES (?, ?, ?)', (youtube_id, ai_user_id, 'pending'))
                cursor.execute('INSERT INTO ai_user_data (user_id, youtube_id, AI_USER_ID, ydx_server, ydx_app_host, status) VALUES (?, ?, ?, ?, ?, ?)',
                               (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, 'pending'))
            else:
                # If the combination exists in the database add a new row to the ai_user_data table
                cursor.execute('INSERT INTO ai_user_data (user_id, youtube_id, AI_USER_ID, ydx_server, ydx_app_host, status) VALUES (?, ?, ?, ?, ?, ?)',
                               (user_id, youtube_id, ai_user_id, ydx_server, ydx_app_host, 'pending'))

        # Commit the changes
        con.commit()
    except sqlite3.Error as e:
        print("Error processing incoming data:", e)


def update_status(youtube_id, ai_user_id, status):
    try:
        with pool.get_resource() as con:
            cursor = con.cursor()

            # Update the status for the specified YouTube ID and AI user ID
            cursor.execute('''
                UPDATE youtube_data
                SET status = ?
                WHERE youtube_id = ? AND AI_USER_ID = ?
            ''', (status, youtube_id, ai_user_id))

            # Commit the changes
            con.commit()
    except sqlite3.Error as e:
        print("Error updating status:", e)