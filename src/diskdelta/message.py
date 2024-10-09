from enum import Enum
import math
from typing import cast

from bitarray import bitarray

from diskdelta import bitbuffer
from diskdelta.block_hash_store import BlockHashStore
from diskdelta.debug import Debug
from diskdelta.index_hash_mapper import Hasher, IndexHashMapper


class DataType(Enum):
    Literal = 0  # Literal data
    Hash = 1  # Hash of the data
    DiskReference = 2  # Index on initial disk
    MessageReference = 3  # Index in message


class Instruction:
    """
    An instruction in a message.
    """

    def __init__(self, index: int, data_type: DataType, data: bytes) -> None:
        self.disk_index = index
        self.data_type = data_type
        self.data = data

    def __eq__(self, other) -> bool:
        if not isinstance(other, Instruction):
            return False
        return (
            self.disk_index == other.disk_index
            and self.data_type == other.data_type
            and self.data == other.data
        )

    def to_bitarray(
        self, disk_index_bits: int, disk_ref_bits: int, msg_ref_bits: int
    ) -> bitarray:
        # Disk index
        instructionBits = bitarray(f"{self.disk_index:0{disk_index_bits}b}")

        # Data type & data
        match self.data_type:
            case DataType.Literal:
                instructionBits.extend(bitarray("00"))
                data_bits = bitarray()
                data_bits.frombytes(self.data)
                instructionBits.extend(data_bits)
            case DataType.Hash:
                instructionBits.extend(bitarray("01"))
                data_bits = bitarray()
                data_bits.frombytes(self.data)
                instructionBits.extend(data_bits)
            case DataType.DiskReference:
                instructionBits.extend(bitarray("10"))
                instructionBits.extend(
                    bitarray(f"{int.from_bytes(self.data):0{disk_ref_bits}b}")
                )
            case DataType.MessageReference:
                instructionBits.extend(bitarray("11"))
                instructionBits.extend(
                    bitarray(f"{int.from_bytes(self.data):0{msg_ref_bits}b}")
                )
            case _:
                raise ValueError("Invalid data type")

        return instructionBits


class Message:
    """
    List of instructions.
    """

    def __init__(self) -> None:
        self.header_bits_size = 0
        self.changed_block_index_size = 0
        self.disk_ref_bits_size = 0
        self.msg_ref_bits_size = 0

        self.instructions: list[Instruction] = []
        self.hash_to_message_index = {}

    def __eq__(self, other) -> bool:
        if not isinstance(other, Message):
            Debug.log("Not instance")
            return False
        if len(self.instructions) != len(other.instructions):
            Debug.log("Different lengths")
            return False
        for instruction1, instruction2 in zip(self.instructions, other.instructions):
            if instruction1 != instruction2:
                Debug.log(
                    f"Different instructions {instruction1.disk_index} {instruction2.disk_index}"
                )
                return False
        return True

    def write_bits_to_file(self, output_path: str) -> None:
        """
        Convert instructions to bitarray message
        """

        with bitbuffer.open(output_path, "w") as f:
            f = cast(bitbuffer.BitWriter, f)
            Debug.log("Writing index sizes")
            f.write(bitarray(f"{self.disk_ref_bits_size:0{self.header_bits_size}b}"))
            f.write(bitarray(f"{self.msg_ref_bits_size:0{self.header_bits_size}b}"))

            Debug.log("Writing instructions")
            for index, inst in enumerate(self.instructions):
                self.log_write_message_progress(index, len(self.instructions))
                f.write(
                    inst.to_bitarray(
                        self.changed_block_index_size,
                        self.disk_ref_bits_size,
                        self.msg_ref_bits_size,
                    )
                )
            Debug.log("Done.")

    def log_write_message_progress(self, index, max_index):
        five_percent = len(self.instructions) // 20

        if five_percent == 0:
            return
        if index % five_percent != 0:
            return
        if index == 0:
            Debug.log("    0%... ", end="")
            return

        progress = int(index / five_percent) * 5
        if progress > 100:
            return

        if progress != 100:
            Debug.log(f"{progress}%... ", end="")
        else:
            Debug.log("100%", end="")
            Debug.log("")

    def calculate_size_bits(self) -> int:
        """
        Calculate the size of the message in bits.
        """
        # bits used to convey bits for indexes in message
        size = self.header_bits_size * 2

        for inst in self.instructions:
            # block index on disk and data type
            size += self.changed_block_index_size + 2
            match inst.data_type:
                case DataType.Literal:
                    # bytes of literal data
                    size += len(inst.data) * 8
                case DataType.Hash:
                    # bytes of hash data
                    size += len(inst.data) * 8
                case DataType.DiskReference:
                    # bits of disk index
                    size += self.disk_ref_bits_size
                case DataType.MessageReference:
                    # bits of reference index
                    size += self.msg_ref_bits_size
        return size


class MessageBuilder:
    def __init__(
        self,
        store: BlockHashStore,
        image_size_by_blocks: int,
    ):
        self.known_blocks_store = store
        self.image_size = image_size_by_blocks

    def build_message(
        self,
        initial_hashes_map: IndexHashMapper,
        target_hashes_map: IndexHashMapper,
    ):
        message = Message()

        greatest_disk_ref: int = 0
        greatest_msg_ref: int = 0

        disk_index = 0
        while disk_index < self.image_size:

            if Debug.isEnabled:
                self.log_build_message_progress(disk_index, self.image_size)

            initial_hash = initial_hashes_map.get_hash_by_index(disk_index)
            # Raw images are same size, one is out of blocks then both are
            if not initial_hash:
                break
            updated_hash = target_hashes_map.get_hash_by_index(disk_index)

            # If hashes are different, process block
            if initial_hash != updated_hash:
                self.process_changed_block(
                    disk_index,
                    updated_hash,
                    initial_hashes_map,
                    target_hashes_map,
                    self.known_blocks_store,
                    message,
                )

                self.known_blocks_store.add(updated_hash, target_hashes_map.literal_by_index(disk_index))

                # Update greatest index values
                changed_block_inst = message.instructions[-1]
                if changed_block_inst.data_type == DataType.DiskReference:
                    greatest_disk_ref = max(
                        greatest_disk_ref, int.from_bytes(changed_block_inst.data)
                    )
                elif changed_block_inst.data_type == DataType.MessageReference:
                    greatest_msg_ref = max(
                        greatest_msg_ref, int.from_bytes(changed_block_inst.data)
                    )

            disk_index += 1

        # Bits used to convey bits for indexes in message. Needs to be big
        # enough to store the largest index.
        message.header_bits_size = get_index_bits_size(self.image_size)

        # The number of bits needed to index a reference block on disk.
        message.disk_ref_bits_size = get_index_bits_size(greatest_disk_ref)

        # The number of bits needed to index an instruction in the message.
        message.msg_ref_bits_size = get_index_bits_size(greatest_msg_ref)

        # The number of bits needed to index any changed block.
        message.changed_block_index_size = get_index_bits_size(disk_index - 1)

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
        """
        Process changed block into instruction and add to message.
        """
        # Check literal block data is referenced in message
        if hash in message.hash_to_message_index:
            msg_index: int = message.hash_to_message_index[hash]
            bytes_needed = (get_index_bits_size(msg_index) + 7) // 8
            inst = Instruction(
                index,
                DataType.MessageReference,
                msg_index.to_bytes(length=bytes_needed),
            )
            message.instructions.append(inst)

        # Check literal block data is in initial image
        elif hashes_on_disk.get_indexes_by_hash(hash):
            disk_index: int = hashes_on_disk.get_indexes_by_hash(hash)[0][0]
            bytes_needed = (get_index_bits_size(disk_index) + 7) // 8
            inst = Instruction(
                index, DataType.DiskReference, disk_index.to_bytes(length=bytes_needed)
            )
            message.instructions.append(inst)
            message.hash_to_message_index[hash] = len(message.instructions) - 1

        # Check block hash is in store
        elif known_blocks.contains_hash(hash):
            digest: bytes = message.hash_to_message_index[hash]
            inst = Instruction(index, DataType.Hash, digest)
            message.instructions.append(inst)
            message.hash_to_message_index[hash] = len(message.instructions) - 1

        # Send block as literal
        else:
            with open(target_hashes.image_path, "rb") as f:
                f.seek(index * target_hashes.block_literal_size)
                block_literal = f.read(target_hashes.block_literal_size)
            inst = Instruction(index, DataType.Literal, block_literal)
            message.instructions.append(inst)
            message.hash_to_message_index[hash] = len(message.instructions) - 1

    def get_message_from_bits(
        self, file_path: str, initial_image: IndexHashMapper
    ) -> Message:
        """
        Create a Message object from a bit file.
        """
        Debug.log("Reading header")
        message = Message()
        message.header_bits_size = get_index_bits_size(self.image_size - 1)
        header_size = message.header_bits_size
        message.changed_block_index_size = get_index_bits_size(self.image_size - 1)

        Debug.log("Reading instructions")
        with bitbuffer.open(file_path, "r") as f:
            f = cast(bitbuffer.BitReader, f)
            # Get the index sizes from the header
            bits = f.read(header_size)
            if bits is None:
                raise ValueError("Failed to read header")
            disk_ref_bits = int(bits.to01(), 2)
            if not disk_ref_bits:
                disk_ref_bits = 1
            bits = f.read(header_size)
            if bits is None:
                raise ValueError("Failed to read header")
            msg_ref_bits = int(bits.to01(), 2)
            if not msg_ref_bits:
                msg_ref_bits = 1
            message.disk_ref_bits_size = disk_ref_bits
            message.msg_ref_bits_size = msg_ref_bits

            # Read bitarray to get message data
            hasher = Hasher(self.known_blocks_store.digest_size)

            while True:
                bits = f.read(message.changed_block_index_size)
                if bits is None:
                    break
                disk_index = int(bits.to01(), 2)

                if Debug.isEnabled:
                    self.log_build_message_progress(disk_index, self.image_size)

                bits = f.read(2)
                if bits is None:
                    break
                data_type = DataType(int(bits.to01(), 2))

                data = self.get_data_by_type(f, data_type, disk_ref_bits, msg_ref_bits)
                if data is None:
                    break

                inst = Instruction(disk_index, data_type, data)
                message.instructions.append(inst)

        return message

    def get_data_by_type(
        self,
        f: bitbuffer.BitReader,
        data_type: DataType,
        disk_ref_bits: int,
        msg_ref_bits: int,
    ) -> bytes | None:
        match data_type:
            case DataType.Literal:
                bits = f.read(self.known_blocks_store.block_size * 8)
                if bits is None:
                    return None
                data = bits.tobytes()
            case DataType.Hash:
                bits = f.read(self.known_blocks_store.digest_size)
                if bits is None:
                    return None
                data = bits.tobytes()
            case DataType.DiskReference:
                bits = f.read(disk_ref_bits)
                if bits is None:
                    return None
                disk_ref_index = int(bits.to01(), 2)
                bytes_needed = (get_index_bits_size(disk_ref_index) + 7) // 8
                data = disk_ref_index.to_bytes(bytes_needed)
            case DataType.MessageReference:
                bits = f.read(msg_ref_bits)
                if bits is None:
                    return None
                msg_ref_index = int(bits.to01(), 2)
                bytes_needed = (get_index_bits_size(msg_ref_index) + 7) // 8
                data = msg_ref_index.to_bytes(bytes_needed)
            case _:
                raise ValueError("Invalid data type")

        return data

    def log_build_message_progress(self, index, max_index):
        five_percent = max_index // 20

        if five_percent == 0:
            return
        if index % five_percent != 0:
            return
        if index == 0:
            Debug.log("    0%... ", end="")
            return

        progress = int(index / five_percent) * 5
        if progress > 100:
            return

        if progress != 100:
            Debug.log(f"{progress}%... ", end="")
        else:
            Debug.log("100%", end="")
            Debug.log("")


def get_index_bits_size(value: int) -> int:
    """
    Get the number of bits needed to represent a value.
    """
    # We add 1 to the log2 because it will otherwise give the wrong number for
    # powers of 2.
    if value == 0:
        return 1
    return value.bit_length()
