from hashlib import sha256
from bitarray import bitarray

from diskdelta.block_hash_store import BlockHashStore
from diskdelta.delta_decoder import DeltaDecoder
from diskdelta.index_hash_mapper import IndexHashMapper
from diskdelta.message import Message, MessageBuilder, DataType


class DiskDelta:
    """
    A class for generating delta files between an initial image and a target
    image.
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

        message_builder = MessageBuilder(self.known_blocks, self.initial_hashes.size())

        self.message = message_builder.build_message(
            self.initial_hashes, self.target_hashes
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

    def get_decoder(self):
        return DeltaDecoder(self)
    
    def apply_message(self, message: Message, initial_image_path: str, output_path: str):
        """
        Apply the message to the initial image to reconstruct the target image.
        """
        with open(initial_image_path, "rb") as f:
            with open(output_path, "wb") as out:
                # Copy the initial image to the output file
                out.write(f.read())

                # Apply the message to the output file
                for instruction in message.blocks:
                    if instruction.data_type == DataType.Literal:
                        out.seek(instruction.index * self.block_size)
                        out.write(instruction.data)
                    elif instruction.data_type == DataType.Hash:
                        data = self.known_blocks.get_data_by_hash(instruction.data)
                        out.seek(instruction.index * self.block_size)
                        out.write(data)
                    elif instruction.data_type == DataType.DiskIndex:
                        data = self.initial_hashes.data_by_index(instruction.data)
                        out.seek(instruction.index * self.block_size)
                        out.write(data)
                    elif instruction.data_type == DataType.Reference:
                        for block in message.blocks:
                            if block.index == instruction.data:
                                data = block.data
                                break
                        out.seek(instruction.index * self.block_size)
                        out.write(data)
