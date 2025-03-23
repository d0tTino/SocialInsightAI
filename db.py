import psycopg2
from psycopg2 import Error

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname="socialinsight_db",
            user="george",
            password="mypassword",
            host="localhost",
            port="5432"
        )
        return conn
    except Error as e:
        print(f"Error connecting to database: {e}")
        return None

def init_db():
    conn = get_db_connection()
    if conn is None:
        return
    cursor = conn.cursor()
    # Table for Discord data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS discord_messages (
            id SERIAL PRIMARY KEY,
            message_id VARCHAR(50) UNIQUE,
            content TEXT,
            timestamp TIMESTAMP,
            channel_id VARCHAR(50),
            user_id VARCHAR(50)
        );
    """)
    # Table for Bluesky data
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bluesky_posts (
            id SERIAL PRIMARY KEY,
            post_id VARCHAR(100) UNIQUE,
            content TEXT,
            timestamp TIMESTAMP,
            user_did VARCHAR(100),
            likes INTEGER
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    print("Database initialized.")

if __name__ == "__main__":
    init_db()
