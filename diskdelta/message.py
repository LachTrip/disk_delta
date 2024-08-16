from enum import Enum
from hashlib import sha256
import math
from typing import Any, List
from diskdelta.block_hash_store import BlockHashStore
from diskdelta.index_hash_mapper import IndexHashMapper
from bitarray import bitarray


class DataType(Enum):
    Literal = 0
    Hash = 1
    DiskIndex = 2
    Reference = 3


class MessageBlock:
    """
    Block ready to be sent.
    """

    def __init__(self, index: int, hash: bytes, data: bytes, data_type: DataType):
        self.index = index
        self.hash = hash
        self.data = data
        self.data_type = data_type

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MessageBlock):
            return False
        return (
            self.index == other.index
            and self.hash == other.hash
            and self.data == other.data
            and self.data_type == other.data_type
        )

    def to_bitarray(self, index_bits, disk_index_bits, ref_index_bits) -> bitarray:
        match self.data_type:
            case DataType.Literal:
                indexArr = bitarray(f"{self.index:0{index_bits}b}")
                bytesData = bitarray()
                bytesData.frombytes(self.data)
                return indexArr + bitarray("00") + bytesData
            case DataType.Hash:
                indexArr = bitarray(f"{self.index:0{index_bits}b}")
                return indexArr + bitarray("01") + bitarray(self.data)
            case DataType.DiskIndex:
                indexArr = bitarray(f"{self.index:0{index_bits}b}")
                bytesData = bitarray(f"{self.data:0{disk_index_bits}b}")
                return indexArr + bitarray("10") + bytesData
            case DataType.Reference:
                indexArr = bitarray(f"{self.index:0{index_bits}b}")
                bytesData = bitarray(f"{self.data:0{ref_index_bits}b}")
                return indexArr + bitarray("11") + bytesData
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
    def __init__(self):
        self.blocks: List[MessageBlock] = []
        self.index_bits_bits = 0
        self.disk_index_bits = 0
        self.ref_index_bits = 0

    def to_bitarray(self) -> bytes:
        """
        Convert blocks to bitarray message
        """

        bitarray_message = bitarray()

        # Add index sizes header
        bitarray_message += bitarray(f"{self.disk_index_bits:0{self.index_bits_bits}b}")
        bitarray_message += bitarray(f"{self.ref_index_bits:0{self.index_bits_bits}b}")

        for block in self.blocks:
            bitarray_message += block.to_bitarray(
                self.index_bits_bits, self.disk_index_bits, self.ref_index_bits
            )
        return bitarray_message

    def to_string(self) -> str:
        # Convert blocks to string message
        string_message = ""

        # Add index sizes header
        string_message += f"Header size: {self.index_bits_bits} x 2\n"
        string_message += f"Disk index size:{self.disk_index_bits}\n"
        string_message += f"Msg Ref index size:{self.ref_index_bits}\n"

        for block in self.blocks:
            string_message += block.to_string()
        return string_message

    def equals(self, other: Any) -> bool:
        """
        Compares two messages for equality.
        """
        if not isinstance(other, Message):
            raise ValueError("Is not a Message object")
        if len(self.blocks) != len(other.blocks):
            raise ValueError("Messages are not the same length")
        for i in range(len(self.blocks)):
            if self.blocks[i] != other.blocks[i]:
                return False
        return True


class MessageBuilder:
    def __init__(
        self,
        store: BlockHashStore,
        image_size: int,
    ):
        self.store = store
        self.image_size = image_size

    def build_message(
        self,
        initial_hashes: IndexHashMapper,
        target_hashes: IndexHashMapper,
    ):
        message = Message()

        greatest_disk_index = 0
        greatest_ref_index = 0

        index = 0
        while index < self.image_size:
            initial_hash = initial_hashes.get_hash_by_index(index)
            updated_hash = target_hashes.get_hash_by_index(index)

            # Raw images are same size, one is out of blocks then both are
            if not initial_hash:
                break

            # If hashes are different, process block
            if initial_hash != updated_hash:
                self.process_changed_block(
                    index,
                    updated_hash,
                    initial_hashes,
                    target_hashes,
                    self.store,
                    message,
                )

                # Update greatest index values
                changed_block = message.blocks[-1]
                if changed_block.data_type == DataType.DiskIndex:
                    greatest_disk_index = max(greatest_disk_index, changed_block.data)
                elif changed_block.data_type == DataType.Reference:
                    greatest_ref_index = max(greatest_ref_index, changed_block.data)

            index += 1



        # (const) bits used to convey bits for indexes in message
        message.index_bits_bits = get_needed_bits(self.image_size)

        # The number of bits needed to index a block in the message. 
        message.disk_index_bits = get_needed_bits(greatest_disk_index)
        message.ref_index_bits = get_needed_bits(greatest_ref_index)

        return message

    def get_message_from_bitarray(self, bitarr_message: bitarray, initial_image: IndexHashMapper) -> Message:
        """
        Create a Message object from a bitarray.
        """
        message = Message()
        message.index_bits_bits = get_needed_bits(self.image_size - 1)
        ind_size = message.index_bits_bits

        # Get the index sizes from the header
        disk_index_bits = int(bitarr_message[:ind_size].to01(), 2)
        if not disk_index_bits:
            disk_index_bits = 1
        ref_index_bits = int(bitarr_message[ind_size : ind_size * 2].to01(), 2)
        if not ref_index_bits:
            ref_index_bits = 1

        message.disk_index_bits = disk_index_bits
        message.ref_index_bits = ref_index_bits

        # Read bitarray to get message data
        i = ind_size * 2
        while i < len(bitarr_message):

            index = int(bitarr_message[i : i + message.index_bits_bits].to01(), 2)
            i += message.index_bits_bits

            data_type = int(bitarr_message[i : i + 2].to01(), 2)
            i += 2

            if data_type == DataType.Literal.value:
                # Literal
                data = bitarr_message[i : i + self.store.block_size * 8].tobytes()
                i += self.store.block_size * 8
                hash = sha256(data).digest()
            elif data_type == DataType.Hash.value:
                # Hash
                data = bitarr_message[i : i + self.store.digest_size * 8].tobytes()
                i += self.store.digest_size * 8
                hash = data
            elif data_type == DataType.DiskIndex.value:
                # Disk index
                if disk_index_bits == 0:
                    data = 0
                data = int(bitarr_message[i : i + disk_index_bits].to01(), 2)
                i += disk_index_bits
                hash = initial_image.get_hash_by_index(data)
            elif data_type == DataType.Reference.value:
                # Reference
                data = int(bitarr_message[i : i + ref_index_bits].to01(), 2)
                i += ref_index_bits
                for block in message.blocks:
                    if block.index == data:
                        hash = block.hash
                        break
            else:
                raise ValueError("Invalid data type")

            message.blocks.append(
                MessageBlock(
                    index,
                    hash,
                    data,
                    DataType(data_type),
                )
            )

        return message

    def process_changed_block(
        self,
        index: int,
        hash: bytes,
        hashes_on_disk: IndexHashMapper,
        target_hashes: IndexHashMapper,
        known_blocks: BlockHashStore,
        message: Message,
    ):
        # Check block is in message
        for block in message.blocks:
            if block.hash == hash:
                message.blocks.append(
                    MessageBlock(
                        index, hash, message.blocks.index(block), DataType.Reference
                    )
                )
                return
        # Check block is in initial image
        if hashes_on_disk.get_indexes_by_hash(hash):
            message.blocks.append(
                MessageBlock(
                    index,
                    hash,
                    hashes_on_disk.get_indexes_by_hash(hash)[0],
                    DataType.DiskIndex,
                )
            )
        # Check block hash is in store
        elif known_blocks.contains_hash(hash):
            message.blocks.append(
                MessageBlock(
                    index, hash, known_blocks.get_data_by_hash(hash), DataType.Hash
                )
            )
        # Send block as literal
        else:
            with open(target_hashes.image_path, "rb") as f:
                f.seek(index * target_hashes.block_size)
                message.blocks.append(
                    MessageBlock(
                        index, hash, f.read(target_hashes.block_size), DataType.Literal
                    )
                )

def get_needed_bits(value: int) -> int:
    """
    Get the number of bits needed to represent a value.
    """
    # We add 1 to the log2 because it will otherwise give the wrong number for 
    # powers of 2.
    if value == 0:
        return 1
    return math.ceil(math.log2(value + 1))