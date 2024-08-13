from enum import Enum
from hashlib import sha256
import math
from typing import Any, List
from block_hash_store import BlockHashStore
from index_hash_mapper import IndexHashMapper
from bitarray import bitarray


class DataType(Enum):
    Literal = 1
    Hash = 2
    DiskIndex = 3
    Reference = 4


class MessageBlock:
    """
    Block ready to be sent.
    """

    def __init__(self, index: int, hash: bytes, data: bytes, data_type: DataType):
        self.index = index
        self.hash = hash
        self.data = data
        self.data_type = data_type

    def to_bitarray(self) -> bitarray:
        match self.data_type:
            case DataType.Literal:
                bytesData = bitarray()
                bytesData.frombytes(self.data)
                return bitarray() + bytesData
            case DataType.Hash:
                return bitarray() + bitarray(self.data)
            case DataType.DiskIndex:
                return bitarray() + bitarray(self.data)
            case DataType.Reference:
                bytesData = bitarray(self.data)
                return bitarray() + bytesData
        raise ValueError("Invalid data type")

    def to_string(self) -> str:
        match self.data_type:
            case DataType.Literal:
                data_string = self.data.decode("utf-8")
                return f"{self.index}, literal, {data_string}\n"
            case DataType.Hash:
                data_string = self.data.hex()
                return f"{self.index}, hash, {data_string}\n"
            case DataType.DiskIndex:
                data_string = self.data
                return f"{self.index}, diskindex, {data_string}\n"
            case DataType.Reference:
                data_string = self.data.decode("utf-8")
                return f"{self.index}, reference, {data_string}\n"


class Message:
    def __init__(
        self,
        initial_hashes: IndexHashMapper,
        target_hashes: IndexHashMapper,
        store: BlockHashStore,
    ):
        self.blocks: List[MessageBlock] = []

        index = 0
        while index < initial_hashes.size():
            initial_hash = initial_hashes.get_hash_by_index(index)
            updated_hash = target_hashes.get_hash_by_index(index)
            if not initial_hash or not updated_hash:
                # Raw images are same size, one is out of blocks then both are
                break
            if initial_hash != updated_hash:
                self.process_changed_block(
                    index, updated_hash, initial_hashes, target_hashes, store
                )
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
                    MessageBlock(
                        index, hash, self.blocks.index(block), DataType.Reference
                    )
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

    def to_bitarray(self) -> bytes:
        # Convert blocks to bitarray message
        bitarray_message = bitarray()
        for block in self.blocks:
            appendable = block.to_bitarray()
            bitarray_message += appendable
        return bitarray_message

    def to_string(self) -> str:
        # Convert blocks to string message
        string_message = ""
        for block in self.blocks:
            string_message += block.to_string()
        return string_message


class DiskDelta:
    """
    A class for generating delta files between an initial image and a target image.
    """

    def __init__(self, initial_image_path, target_image_path, block_size, digest_size):
        self.block_size = block_size
        self.digest_size = digest_size
        self.known_blocks = BlockHashStore(self.block_size, sha256().digest_size)
        self.initial_hashes = IndexHashMapper(
            initial_image_path, self.block_size, self.digest_size
        )
        self.target_hashes = IndexHashMapper(
            target_image_path, self.block_size, self.digest_size
        )

        if self.initial_hashes.size() != self.target_hashes.size():
            raise ValueError("Initial and target images are not the same size")

        self.message = Message(
            self.initial_hashes, self.target_hashes, self.known_blocks
        )

    def generate_bitarray(self) -> bitarray:
        """
        Generates bitarray representation of the disk delta.
        """

        delta_as_bitarray = self.message.to_bitarray()

        # Add padding to make the bitarray length a multiple of 8
        padding_length = 8 - len(delta_as_bitarray) % 8
        if padding_length != 8:
            delta_as_bitarray += bitarray(padding_length)

        return delta_as_bitarray

    def generate_string(self) -> str:
        """
        Generates string representation of the disk delta.
        """

        delta_as_string = self.message.to_string()

        return delta_as_string
