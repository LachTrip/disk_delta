from bitarray import bitarray
from diskdelta.block_hash_store import BlockHashStore
from diskdelta.index_hash_mapper import IndexHashMapper

import diskdelta
from diskdelta.message import Message


class DeltaDecoder:
    def __init__(self, initial_hashes, known_blocks):
        self.initial_image_map = initial_hashes
        self.known_blocks = known_blocks

    def get_message_from_bits(self, file_path: str) -> Message:
        """
        Returns target image through applying message instructions to initial
        image.
        """
        message_builder = diskdelta.MessageBuilder(
            self.known_blocks,
            int(
                self.initial_image_map.image_size()
                / self.initial_image_map.block_literal_size
            ),
        )

        decoded_message: Message = message_builder.get_message_from_bits(
            file_path, self.initial_image_map
        )

        return decoded_message
