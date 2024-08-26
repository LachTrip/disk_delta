from enum import Enum
import math
from typing import Any, List, Set
from diskdelta import bitbuffer
from diskdelta.block_hash_store import BlockHashStore
from diskdelta.index_hash_mapper import Hasher, IndexHashMapper
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
        self.disk_index = index
        self.hash = hash
        self.data = data
        self.data_type = data_type

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MessageBlock):
            return False
        return (
            self.disk_index == other.disk_index
            and self.hash == other.hash
            and self.data == other.data
            and self.data_type == other.data_type
        )

    def to_bitarray(self, index_bits, disk_index_bits, ref_index_bits) -> bitarray:
        match self.data_type:
            case DataType.Literal:
                indexArr = bitarray(f"{self.disk_index:0{index_bits}b}")
                bytesData = bitarray()
                bytesData.frombytes(self.data)
                return indexArr + bitarray("00") + bytesData
            case DataType.Hash:
                indexArr = bitarray(f"{self.disk_index:0{index_bits}b}")
                return indexArr + bitarray("01") + bitarray(self.data)
            case DataType.DiskIndex:
                indexArr = bitarray(f"{self.disk_index:0{index_bits}b}")
                bytesData = bitarray(f"{self.data:0{disk_index_bits}b}")
                return indexArr + bitarray("10") + bytesData
            case DataType.Reference:
                indexArr = bitarray(f"{self.disk_index:0{index_bits}b}")
                bytesData = bitarray(f"{self.data:0{ref_index_bits}b}")
                return indexArr + bitarray("11") + bytesData
        raise ValueError("Invalid data type")

    def to_string(self) -> str:
        match self.data_type:
            case DataType.Literal:
                data_string = self.data.decode("utf-8")
                return f"{self.disk_index}, literal, {data_string}\n"
            case DataType.Hash:
                data_string = self.data.hex()
                return f"{self.disk_index}, hash, {data_string}\n"
            case DataType.DiskIndex:
                data_string = self.data
                return f"{self.disk_index}, diskindex, {data_string}\n"
            case DataType.Reference:
                data_string = self.data.decode("utf-8")
                return f"{self.disk_index}, reference, {data_string}\n"


class Message:
    def __init__(self):
        self.blocks: List[MessageBlock] = []
        self.hash_to_message_index = {}
        self.index_bits_bits = 0
        self.disk_index_bits = 0
        self.ref_index_bits = 0

    def write_bits_to_file(self, output_path: str) -> None:
        """
        Convert blocks to bitarray message
        """

        with bitbuffer.open(output_path, "w") as f:
            f.write(bitarray(f"{self.disk_index_bits:0{self.index_bits_bits}b}"))
            f.write(bitarray(f"{self.ref_index_bits:0{self.index_bits_bits}b}"))

            for block in self.blocks:
                f.write(
                    block.to_bitarray(
                        self.index_bits_bits, self.disk_index_bits, self.ref_index_bits
                    )
                )

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
        try:
            if not isinstance(other, Message):
                raise ValueError("Is not a Message object")
            if len(self.blocks) != len(other.blocks):
                raise ValueError("Messages are not the same length")
            for block1, block2 in zip(self.blocks, other.blocks):
                if block1 != block2:
                    raise ValueError("Message blocks are not equal")
            return True
        except ValueError:
            return False
    
    def calculate_size_bits(self, store: BlockHashStore) -> int:
        """
        Calculate the size of the message in bits.
        """
        size = self.index_bits_bits * 2
        for block in self.blocks:
            size += 2
            match block.data_type:
                case DataType.Literal:
                    size += len(block.data) * 8
                case DataType.Hash:
                    size += store.digest_size * 8
                case DataType.DiskIndex:
                    size += self.disk_index_bits
                case DataType.Reference:
                    size += self.ref_index_bits
        return size


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

        greatest_disk_index: int = 0
        greatest_ref_index: int = 0

        disk_index = 0
        while disk_index < self.image_size:

            five_percent = self.image_size // 20
            if five_percent != 0:
                if disk_index % five_percent == 0:
                    if disk_index / five_percent * 5 != 100:
                        print(f"{int(disk_index/five_percent)*5}%... ", end="")
                    else:
                        print("100%")

            initial_hash = initial_hashes.get_hash_by_index(disk_index)
            updated_hash = target_hashes.get_hash_by_index(disk_index)

            # Raw images are same size, one is out of blocks then both are
            if not initial_hash:
                break

            # If hashes are different, process block
            if initial_hash != updated_hash:
                self.process_changed_block(
                    disk_index,
                    updated_hash,
                    initial_hashes,
                    target_hashes,
                    self.store,
                    message,
                )

                # Update greatest index values
                changed_block = message.blocks[-1]
                if changed_block.data_type == DataType.DiskIndex:
                    greatest_disk_index = max(
                        greatest_disk_index, int(changed_block.data)
                    )
                elif changed_block.data_type == DataType.Reference:
                    greatest_ref_index = max(
                        greatest_ref_index, int(changed_block.data)
                    )

            disk_index += 1

        # (const) bits used to convey bits for indexes in message
        message.index_bits_bits = get_needed_bits(self.image_size)

        # The number of bits needed to index a block in the message.
        message.disk_index_bits = get_needed_bits(greatest_disk_index)
        message.ref_index_bits = get_needed_bits(greatest_ref_index)

        return message

    def get_message_from_bits(
        self, file_path: str, initial_image: IndexHashMapper
    ) -> Message:
        """
        Create a Message object from a bit file.
        """
        message = Message()
        message.index_bits_bits = get_needed_bits(self.image_size)
        ind_size = message.index_bits_bits

        with bitbuffer.open(file_path, "r") as f:
            # Get the index sizes from the header
            # disk_index_bits = int(bitarr_message[:ind_size].to01(), 2)
            disk_index_bits = int(f.read(ind_size).to01(), 2)

            if not disk_index_bits:
                disk_index_bits = 1
            # ref_index_bits = int(bitarr_message[ind_size : ind_size * 2].to01(), 2)
            ref_index_bits = int(f.read(ind_size).to01(), 2)
            if not ref_index_bits:
                ref_index_bits = 1

            message.disk_index_bits = disk_index_bits
            message.ref_index_bits = ref_index_bits

            # Read bitarray to get message data
            hasher = Hasher(self.store.digest_size)
            # i = ind_size * 2

            # while i < len(bitarr_message):
            while True:

                # index = int(bitarr_message[i : i + message.index_bits_bits].to01(), 2)
                bits = f.read(message.index_bits_bits)
                if bits is None:
                    break
                disk_index = int(bits.to01(), 2)

                # i += message.index_bits_bits

                # data_type = int(bitarr_message[i : i + 2].to01(), 2)
                # i += 2
                data_type = int(f.read(2).to01(), 2)

                if data_type == DataType.Literal.value:
                    # Literal
                    # data = bitarr_message[i : i + self.store.block_size * 8].tobytes()
                    # i += self.store.block_size * 8
                    data = f.read(self.store.block_size * 8)
                    if data is None:
                        break
                    hash = hasher.hash(data)
                elif data_type == DataType.Hash.value:
                    # Hash
                    # data = bitarr_message[i : i + self.store.digest_size * 8].tobytes()
                    # i += self.store.digest_size * 8
                    data = f.read(self.store.digest_size)
                    if data is None:
                        break
                    hash = data
                elif data_type == DataType.DiskIndex.value:
                    # Disk index
                    # if disk_index_bits == 0:
                    #     data = 0
                    # data = int(bitarr_message[i : i + disk_index_bits].to01(), 2)
                    # i += disk_index_bits
                    bits = f.read(disk_index_bits)
                    if bits is None:
                        break
                    data = int(bits.to01(), 2)
                    hash = initial_image.get_hash_by_index(data)
                elif data_type == DataType.Reference.value:
                    # Reference
                    # data = int(bitarr_message[i : i + ref_index_bits].to01(), 2)
                    # i += ref_index_bits
                    bits = f.read(ref_index_bits)
                    if bits is None:
                        break
                    data = int(bits.to01(), 2)
                    hash = message.blocks[data].hash
                else:
                    raise ValueError("Invalid data type")

                message.blocks.append(
                    MessageBlock(
                        disk_index,
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
        if hash in message.hash_to_message_index:
            message.blocks.append(
                MessageBlock(
                    index, hash, message.hash_to_message_index[hash], DataType.Reference
                )
            )
        # Check block is in initial image
        elif hashes_on_disk.get_indexes_by_hash(hash):
            message.blocks.append(
                MessageBlock(
                    index,
                    hash,
                    hashes_on_disk.get_indexes_by_hash(hash)[0],
                    DataType.DiskIndex,
                )
            )
            message.hash_to_message_index[hash] = len(message.blocks) - 1
        # Check block hash is in store
        elif known_blocks.contains_hash(hash):
            message.blocks.append(
                MessageBlock(
                    index, hash, known_blocks.get_data_by_hash(hash), DataType.Hash
                )
            )
            message.hash_to_message_index[hash] = len(message.blocks) - 1
        # Send block as literal
        else:
            with open(target_hashes.image_path, "rb") as f:
                f.seek(index * target_hashes.block_size)
                message.blocks.append(
                    MessageBlock(
                        index, hash, f.read(target_hashes.block_size), DataType.Literal
                    )
                )
            message.hash_to_message_index[hash] = len(message.blocks) - 1


def get_needed_bits(value: int) -> int:
    """
    Get the number of bits needed to represent a value.
    """
    # We add 1 to the log2 because it will otherwise give the wrong number for
    # powers of 2.
    if value == 0:
        return 1
    return math.ceil(math.log2(value + 1))
