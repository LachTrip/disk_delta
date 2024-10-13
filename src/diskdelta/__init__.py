import os
from bitarray import bitarray

from diskdelta.debug import Debug
from diskdelta.block_hash_store import BlockHashStore

# from diskdelta.delta_decoder import DeltaDecoder
from diskdelta.delta_decoder import DeltaDecoder
from diskdelta.index_hash_mapper import IndexHashMapper
from diskdelta.message import Message, MessageBuilder, DataType


class DiskDelta:
    """
    A class for generating delta files between an initial image and a target
    image.
    """

    def __init__(
        self, initial_image_path, target_image_path, block_size, digest_size_bytes
    ):
        self.image_block_size = block_size
        self.digest_size = digest_size_bytes
        self.known_blocks = BlockHashStore(self.image_block_size, self.digest_size)

        # Check if the initial and target images are the same size
        initial_image_size = os.path.getsize(initial_image_path)
        target_image_size = os.path.getsize(target_image_path)
        if initial_image_size != target_image_size:
            raise ValueError("Initial and target images are not the same size")

        self.image_size = initial_image_size

        Debug.log("Generating hashes for initial image")
        Debug.increment_indent()
        self.initial_hashes = IndexHashMapper(
            initial_image_path, self.image_block_size, self.digest_size
        )
        Debug.increment_indent(-1)

        Debug.log("Generating hashes for target image")
        Debug.increment_indent()
        self.target_hashes = IndexHashMapper(
            target_image_path, self.image_block_size, self.digest_size
        )
        Debug.increment_indent(-1)

    def build_message(self):
        message_builder = MessageBuilder(self.known_blocks, int(self.image_size/self.image_block_size))

        Debug.log("Building message")
        Debug.increment_indent()
        self.message: Message = message_builder.build_message(
            self.initial_hashes, self.target_hashes
        )
        Debug.increment_indent(-1)

    def write_message_to_file(self, file_path):
        """
        Writes bit representation of the message.
        """
        Debug.log("Writing message to file")
        Debug.increment_indent()
        self.message.write_bits_to_file(file_path)
        Debug.increment_indent(-1)

    def get_decoder(self):
        return DeltaDecoder(self.initial_hashes, self.known_blocks)

    def apply_message(
        self, initial_image_path: str, output_path: str
    ):
        """
        Apply the message to the initial image to reconstruct the target image.
        """
        # Copy the initial image to the output file
        with open(initial_image_path, "rb") as f:
            with open(output_path, "wb") as out:
                out.write(f.read())

        # Ensure the output file is the initial image
        with open(initial_image_path, "rb") as f:
            with open(output_path, "rb") as out:
                assert f.read() == out.read()

        # Apply the message to the output file
        with open(output_path, "r+b") as out:
            for instruction in self.message.instructions:
                literal = self.get_literal_from_instruction(instruction, self.message)
                if literal is None:
                    raise ValueError("Data literal not found")
                out.seek(instruction.disk_index * self.image_block_size)
                out.write(literal)

    def get_literal_from_instruction(self, instruction, message):
        data = None
        match instruction.data_type:
            case DataType.Literal:
                data = instruction.data
            case DataType.Hash:
                data = self.known_blocks.get_data_by_hash(instruction.data)
            case DataType.DiskReference:
                disk_index = int.from_bytes(instruction.data)
                data = self.initial_hashes.literal_by_index(disk_index)
            case DataType.MessageReference:
                msg_index = int.from_bytes(instruction.data)
                ref_instruction = message.instructions[msg_index]
                data = self.get_literal_from_instruction(ref_instruction, message)
        return data
