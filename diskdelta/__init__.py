from hashlib import sha256
from bitarray import bitarray

from diskdelta.block_hash_store import BlockHashStore
from diskdelta.index_hash_mapper import IndexHashMapper
from diskdelta.message import Message, MessageBuilder


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
