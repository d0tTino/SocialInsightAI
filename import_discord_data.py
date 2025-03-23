import json
import os
from db import get_db_connection

def import_message_data(file_paths, batch_size=50):
    imported = 0
    for i in range(0, len(file_paths), batch_size):
        batch = file_paths[i:i + batch_size]
        conn = get_db_connection()
        if not conn:
            print(f"Database connection failed for batch starting at {batch[0]}")
            return imported
        cursor = conn.cursor()
        try:
            for file_path in batch:
                print(f"Processing {file_path}")
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
                                print(f"Imported message {msg.get('id')} from {file_path}")
                            except Exception as e:
                                print(f"Error importing message in {file_path}: {e}")
                except Exception as e:
                    print(f"Error reading {file_path}: {e}")
        finally:
            cursor.close()
            conn.close()
    return imported

def import_relationship_data(file_path):
    print(f"Skipping relationship file {file_path} - no parsing implemented")
    return 0

def main():
    data_dir = "C:/Users/w1n51/OneDrive/Desktop/Programing Projects/SocialInsightAI/discord_data/data"
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created {data_dir} - please move your JSON files there and rerun")
        return
    all_files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if f.endswith('.json')]
    message_files = [f for f in all_files if 'message' in f.lower()]
    relationship_files = [f for f in all_files if 'relationship' in f.lower()]
    # Test run on first 5 message files
    print("Testing import on first 5 message files...")
    test_files = message_files[:5]
    test_imported = import_message_data(test_files)
    print(f"Test complete - {test_imported} messages imported")
    # Full run
    print("Starting full import...")
    total_imported = import_message_data(message_files)
    for file_path in relationship_files:
        total_imported += import_relationship_data(file_path)
    print(f"Full import complete - {total_imported} messages imported")

if __name__ == "__main__":
    main() 