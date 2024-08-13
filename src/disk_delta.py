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

    def to_bitarray(self, disk_index_bits, ref_index_bits) -> bitarray:
        match self.data_type:
            case DataType.Literal:
                bytesData = bitarray()
                bytesData.frombytes(self.data)
                return bitarray("00") + bytesData
            case DataType.Hash:
                return bitarray("01") + bitarray(self.data)
            case DataType.DiskIndex:
                return bitarray("10") + bitarray(self.data)
            case DataType.Reference:
                bytesData = bitarray(self.data)
                return bitarray("11") + bytesData
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

        image_size = initial_hashes.size()
        greatest_disk_index = 0
        greatest_ref_index = 0

        index = 0
        while index < image_size:
            initial_hash = initial_hashes.get_hash_by_index(index)
            updated_hash = target_hashes.get_hash_by_index(index)

            # Raw images are same size, one is out of blocks then both are
            if not initial_hash:
                break

            # If hashes are different, process block
            if initial_hash != updated_hash:
                self.process_changed_block(
                    index, updated_hash, initial_hashes, target_hashes, store
                )

                # Update greatest index values
                changed_block = self.blocks[-1]
                if changed_block.data_type == DataType.DiskIndex:
                    greatest_disk_index = max(greatest_disk_index, changed_block.data)
                elif changed_block.data_type == DataType.Reference:
                    greatest_ref_index = max(greatest_ref_index, changed_block.data)

            index += 1

        # (const) number of bits used to convey bits for indexes in message
        self.max_index_bits = math.ceil(math.log2(image_size))

        # number of bits needed to index a block in the message
        self.disk_index_bits = (
            math.ceil(math.log2(greatest_disk_index)) if greatest_disk_index else 0
        )
        self.ref_index_bits = (
            math.ceil(math.log2(greatest_ref_index)) if greatest_ref_index else 0
        )

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
        """
        Convert blocks to bitarray message
        """

        bitarray_message = bitarray()

        # Add index sizes header
        bitarray_message += bitarray(f"{self.disk_index_bits:0{self.max_index_bits}b}")
        bitarray_message += bitarray(f"{self.ref_index_bits:0{self.max_index_bits}b}")

        for block in self.blocks:
            bitarray_message += block.to_bitarray(
                self.disk_index_bits, self.ref_index_bits
            )
        return bitarray_message

    def to_string(self) -> str:
        # Convert blocks to string message
        string_message = ""

        # Add index sizes header
        string_message += f"Header size: {self.max_index_bits} x 2\n"
        string_message += f"Disk index size:{self.disk_index_bits}\n"
        string_message += f"Msg Ref index size:{self.ref_index_bits}\n"

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
        # padding_length = 8 - len(delta_as_bitarray) % 8
        # if padding_length != 8:
        #     delta_as_bitarray += bitarray(padding_length)

        return delta_as_bitarray

    def generate_string(self) -> str:
        """
        Generates string representation of the disk delta.
        """

        delta_as_string = self.message.to_string()

        return delta_as_string
