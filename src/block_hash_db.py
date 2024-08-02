import sqlite3

import sqlite3


class BlockHashDB:
    """
    A class representing a database for storing disk block hashes and corresponding data.
    """

    def __init__(self, database_path):
        self.conn = sqlite3.connect(database_path)
        self.cursor = self.conn.cursor()

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS disk_blocks (
                id INTEGER PRIMARY KEY,
                hash TEXT NOT NULL,
                block_data BLOB NOT NULL
            )
            """
        )

    def insert_disk_block(self, hash_value, block_data):
        """
        Inserts a disk block into the database.

        Parameters:
        - hash_value (str): The hash value of the disk block.
        - block_data (bytes): The data of the disk block.

        Returns:
        - None
        """
        self.cursor.execute(
            """
            INSERT INTO disk_blocks (hash, block_data)
            VALUES (?, ?)
            """,
            (hash_value, block_data),
        )
        self.conn.commit()

    def get_disk_block_by_hash(self, hash_value):
        """
        Retrieves a disk block from the database by its hash value.

        Parameters:
        - hash_value (str): The hash value of the disk block.

        Returns:
        - bytes: The data of the disk block, or None if the hash value is not found.
        """
        self.cursor.execute(
            """
            SELECT block_data FROM disk_blocks
            WHERE hash = ?
            """,
            (hash_value,),
        )
        result = self.cursor.fetchone()
        return result[0] if result else None

    def hash_exists(self, hash_value):
        """
        Checks if a hash value exists in the database.

        Parameters:
        - hash_value (str): The hash value to check.

        Returns:
        - bool: True if the hash value exists, False otherwise.
        """
        self.cursor.execute(
            """
            SELECT 1 FROM disk_blocks
            WHERE hash = ?
            """,
            (hash_value,),
        )
        return self.cursor.fetchone() is not None
