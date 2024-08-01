import sqlite3

class BlockHashDB:
    def __init__(self, db_name):
        # Connect to the SQLite database (or create it if it doesn't exist)
        self.conn = sqlite3.connect('data/disk_blocks.db')
        self.cursor = conn.cursor()

        # Create a table to store hashes and corresponding disk block data
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS disk_blocks (
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                block_data BLOB NOT NULL
            )
        ''')

    # Function to insert data into the database
    def insert_disk_block(self, hash_value, block_data):
        self.cursor.execute('''
            INSERT INTO disk_blocks (hash, block_data)
            VALUES (?, ?)
        ''', (hash_value, block_data))
        self.conn.commit()

    # Function to retrieve data from the database by hash
    def get_disk_block_by_hash(self, hash_value):
        self.cursor.execute('''
            SELECT block_data FROM disk_blocks
            WHERE hash = ?
        ''', (hash_value,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    # Function to check if a hash exists in the database
    def hash_exists(self, hash_value):
        self.cursor.execute('''
            SELECT 1 FROM disk_blocks
            WHERE hash = ?
        ''', (hash_value,))
        return self.cursor.fetchone() is not None

    # Example usage
    hash_value = 'example_hash'
    block_data = b'example_block_data'
    insert_disk_block(hash_value, block_data)

    retrieved_data = get_disk_block_by_hash(hash_value)
    print(retrieved_data)

    # Close the connection when done
    conn.close()