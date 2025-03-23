import json
import os
from db import get_db_connection

def import_message_data(file_path):
    conn = get_db_connection()
    if not conn:
        print("Database connection failed")
        return 0
    cursor = conn.cursor()
    imported = 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            messages = [data] if isinstance(data, dict) else data
            for msg in messages:
                try:
                    cursor.execute("""
                        INSERT INTO discord_messages (message_id, content, timestamp, channel_id, user_id)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (message_id) DO NOTHING;
                    """, (
                        str(msg.get('id', '')),
                        msg.get('content', ''),
                        msg.get('timestamp', None),
                        str(msg.get('channel_id', '')),
                        str(msg.get('author', {}).get('id', '')) if isinstance(msg.get('author'), dict) else str(msg.get('user_id', ''))
                    ))
                    conn.commit()
                    imported += 1
                    print(f"Imported message {msg.get('id')}")
                except Exception as e:
                    print(f"Error importing message from {file_path}: {e}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    finally:
        cursor.close()
        conn.close()
    return imported

def import_relationship_data(file_path):
    print(f"Skipping relationship file {file_path} - no parsing implemented yet")
    return 0

def main():
    data_dir = "C:/Users/w1n51/OneDrive/Desktop/Programing Projects/SocialInsightAI/discord_data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created {data_dir} - please move your JSON files there and rerun")
        return
    total_imported = 0
    for filename in os.listdir(data_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(data_dir, filename)
            if 'message' in filename.lower():
                total_imported += import_message_data(file_path)
            elif 'relationship' in filename.lower():
                total_imported += import_relationship_data(file_path)
            else:
                print(f"Skipping unknown file {filename}")
    print(f"Import complete - {total_imported} messages imported")

if __name__ == "__main__":
    main() 