from hashlib import sha256
from block_hash_db import BlockHashDB


class DiskDelta:
    """
    A class for generating delta files between an initial image and a target image.
    """

    def __init__(self, block_size=4096, database_path="data/disk_blocks.db"):
        self.database = BlockHashDB(database_path)
        self.block_size = block_size

    def generate_binary(self, initial_image_path, target_image_path):
        """
        Generates a binary delta file between an initial image and a target image.

        Args:
            initial_image_path (str): The file path of the initial image.
            target_image_path (str): The file path of the target image.

        Returns:
            bytes: The binary delta file representing the changes between the initial and target images.
        """
        # Create initial and target image tempfiles
        initial_image_hashes = DiskDelta.hashes_db(self, initial_image_path)
        target_image_hashes = DiskDelta.hashes_db(self, target_image_path)

        changed_indexes = DiskDelta.find_differences(
            self, initial_image_hashes, target_image_hashes
        )

        blocks_in_initial = DiskDelta.indexes_on_disk(
            self, changed_indexes, initial_image_path, target_image_path
        )

        delta_as_binary = b""  # initial_image.delta(target_image)

        return delta_as_binary

    def hashes_db(self, file_path):
        """
        Stores the hashes of each block in a temporary file.
        Adds the hash to the database if it does not already exist.

        Args:
            file_path (str): The path to the file.

        Returns:
            TemporaryFile: A temporary file object containing the hashes of each block.
        """
        with open(file_path, "rb") as file:
            file_name = file.name.split("/")[-1].split(".")[0]
            hashes = BlockHashDB(f"data/{file_name}.db")
            
            while True:
                block = file.read(self.block_size)
                if not block:
                    break
                block_hash = sha256(block).digest()
                if not self.database.hash_exists(block_hash):
                    self.database.insert_disk_block(block_hash, block)
                hashes.insert_disk_block(block_hash, block)

        return hashes

    def find_differences(self, initial_hashes, updated_hashes):
        """
        Finds the differences between two files by comparing their blocks.

        Args:
            initial_hashes (tempfile): The temporary file object containing the initial file's hashes.
            updated_hashes (tempfile): The temporary file object containing the updated file's hashes.

        Returns:
            list: A list of indexes where the blocks in the initial file differ from the updated file.
        """

        changed_indexes = []
        index = 0
        with open(initial_file_path, "rb") as init_file:
            with open(updated_file_path, "rb") as updated_file:
                while True:
                    init_block = init_file.read(self.block_size)
                    updated_block = updated_file.read(self.block_size)
                    if not init_block:
                        print(f"End of initial file at index {index}")
                    if not updated_block:
                        print(f"End of updated file at index {index}")
                    if not init_block or not updated_block:
                        break
                    if init_block != updated_block:
                        changed_indexes.append(index)
                    index += 1
        return changed_indexes

    def indexes_on_disk(self, indexes, initial_file_path, target_file_path):
        """
        Find the indexes of blocks that exist on disk based on the given indexes, initial file path, and target file path.

        Args:
            indexes (list): A list of indexes representing the blocks to search for on disk.
            initial_file_path (str): The file path of the initial file to compare against.
            target_file_path (str): The file path of the target file to search for blocks on disk.

        Returns:
            list: A list of indexes representing the blocks that exist on disk.
        """
        blocks_on_disk = []
        for index in indexes:
            # Get block in target file
            with open(target_file_path, "rb") as target_file:
                target_file.seek(index * self.block_size)
                target_block = target_file.read(self.block_size)
            # Check already found blocks
            if target_block in blocks_on_disk:
                blocks_on_disk.append(b"")
                continue
            # Check every block in initial file to see if it matches the target block
            with open(initial_file_path, "rb") as initial_file:
                while True:
                    initial_block = initial_file.read(self.block_size)
                    if not initial_block:
                        blocks_on_disk.append(b"")
                        break
                    if initial_block == target_block:
                        blocks_on_disk.append(index)
                        break
        return blocks_on_disk
