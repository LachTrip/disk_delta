from enum import Enum
from hashlib import sha256
import math
from typing import Any, List
from block_hash_store import BlockHashStore
from index_hash_mapper import IndexHashMapper
import numpy as np

class DataType(Enum):
    Literal = 1
    Hash = 2
    DiskIndex = 3
    Reference = 4

class Block:
    """
    Index and hash of a block
    """

    def __init__(self, index, hash):
        self.index = index
        self.hash = hash

class MessageBlock(Block):
    """
    Block ready to be sent
    """

    def __init__(self, index:int, hash:bytes, data:bytes, data_type:DataType):
        super().__init__(index, hash)
        self.data = data
        self.data_type = data_type

    def to_binary(self) -> bytes:
        match self.data_type:
            case DataType.Literal:
                return b'\x00' + self.data
            case DataType.Hash:
                return b'\x01' + self.data
            case DataType.DiskIndex:
                return b'\x02' + self.data.to_bytes()
            case DataType.Reference:
                return b'\x03' + self.data.to_bytes()


class BlocksMessage:
    def __init__(
        self,
        initial_hashes: IndexHashMapper,
        target_hashes: IndexHashMapper,
        store: BlockHashStore,
    ):
        self.blocks = []

        index = 0
        while index < initial_hashes.size():
            initial_hash = initial_hashes.get_hash_by_index(index)
            updated_hash = target_hashes.get_hash_by_index(index)
            if not initial_hash or not updated_hash:
                # Raw images are same size, one is out of blocks then both are
                break
            if initial_hash != updated_hash:
                self.process_changed_block(index, updated_hash, initial_hashes, target_hashes, store)
            index += 1

    def process_changed_block(
        self,
        index: int,
        hash: bytes,
        hashes_on_disk: IndexHashMapper,
        target_hashes: IndexHashMapper,
        known_blocks: BlockHashStore,
    ):
        # Check block is in message
        for block in self.blocks:
            if block.hash == hash:
                self.blocks.append(
                    MessageBlock(index, hash, block.data, DataType.Reference)
                )
                return
        # Check block is in initial image
        if hashes_on_disk.get_indexes_by_hash(hash):
            self.blocks.append(
                MessageBlock(
                    index,
                    hash,
                    hashes_on_disk.get_indexes_by_hash(hash)[0],
                    DataType.DiskIndex,
                )
            )
        # Check block hash is in store
        elif known_blocks.contains_hash(hash):
            self.blocks.append(
                MessageBlock(
                    index, hash, known_blocks.get_data_by_hash(hash), DataType.Hash
                )
            )
        # Send block as literal
        else:
            with open(target_hashes.image_path, "rb") as f:
                f.seek(index * target_hashes.block_size)
                self.blocks.append(
                    MessageBlock(
                        index, hash, f.read(target_hashes.block_size), DataType.Literal
                    )
                )

    def to_binary(self) -> bytes:
        # Convert blocks to binary message
        binary_message = b""
        for block in self.blocks:
            binary_message += block.to_binary()
        return binary_message


class DiskDelta:
    """
    A class for generating delta files between an initial image and a target image.
    """

    def __init__(self, block_size, digest_size):
        self.block_size = block_size
        self.digest_size = digest_size
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

        initial_hashes = IndexHashMapper(
            initial_image_path, self.block_size, self.digest_size
        )

        target_hashes = IndexHashMapper(
            target_image_path, self.block_size, self.digest_size
        )

        message = BlocksMessage(initial_hashes, target_hashes, self.known_blocks)

        delta_as_binary = message.to_binary()

        return delta_as_binary
