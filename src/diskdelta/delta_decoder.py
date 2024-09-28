from bitarray import bitarray
from diskdelta.block_hash_store import BlockHashStore
from diskdelta.index_hash_mapper import IndexHashMapper

import diskdelta
from diskdelta.message import Message


class DeltaDecoder:
    def __init__(self, delta):
        self.delta = delta

    def update_initial(self):
        """
        Update the initial image in place with the new hashes.
        """
        block_size: int = self.delta.block_size
        digest_size: int = self.delta.digest_size
        initial_image: IndexHashMapper = self.delta.initial_hashes
        target_image: IndexHashMapper = self.delta.target_hashes
        known_blocks: BlockHashStore = self.delta.known_blocks
        message = self.delta.message
        pass

    def get_message_from_bits(self, file_path: str) -> Message:
        """
        Returns target image through applying message instructions to initial
        image.
        """
        initial_image: IndexHashMapper = self.delta.initial_hashes
        known_blocks: BlockHashStore = self.delta.known_blocks

        message_builder = diskdelta.MessageBuilder(
            known_blocks, initial_image.image_size()
        )

        decoded_message: Message = message_builder.get_message_from_bits(
            file_path, initial_image
        )
        # bit_message = decoded_message.to_bitarray()

        return decoded_message
