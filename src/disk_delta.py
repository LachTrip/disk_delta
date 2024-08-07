from hashlib import sha256
import index_hash_mapper
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

        initial_hashes = index_hash_mapper(initial_image_path, self.block_size)
        target_hashes = index_hash_mapper(target_image_path, self.block_size)

        changed_indexes = DiskDelta.find_differences(
            self, initial_hashes, target_hashes
        )

        blocks_in_initial = DiskDelta.indexes_on_disk(
            self, changed_indexes, initial_hashes
        )

        delta_as_binary = b""  # initial_image.delta(target_image)

        return delta_as_binary

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
        while True:
            initial_hash = initial_hashes.hash_by_index(index)
            updated_hash = updated_hashes.hash_by_index(index)
            if not initial_hash or not updated_hash:
                break
            if initial_hash != updated_hash:
                changed_indexes.append(index)
            index += 1

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
