from hashlib import sha256
from typing import List
from block_hash_store import BlockHashStore
from index_hash_mapper import FileIndexHashMapper
from block import Block, BlockDataType


class DiskDelta:
    """
    A class for generating delta files between an initial image and a target image.
    """

    def __init__(self, block_size=4096):
        self.block_size = block_size

        self.known_blocks = BlockHashStore(self.block_size, sha256().digest_size)

    def generate_binary(self, initial_image_path, target_image_path):
        """
        Generates a binary delta file between an initial image and a target image.

        Args:
            initial_image_path (str): The file path of the initial image.
            target_image_path (str): The file path of the target image.

        Returns:
            bytes: The binary delta file representing the changes between the initial and target images.
        """

        initial_hashes = FileIndexHashMapper(initial_image_path, self.block_size)
        target_hashes = FileIndexHashMapper(target_image_path, self.block_size)

        blocks_to_send = target_hashes.changed_blocks_from(initial_hashes)

        for block in blocks_to_send:
            pass

        delta_as_binary = b""  # initial_image.delta(target_image)

        return delta_as_binary
