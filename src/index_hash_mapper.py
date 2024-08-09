from hashlib import sha256
from typing import Any, List

from block import Block
from block_hash_store import BlockHashStore


class FileIndexHashMapper:
    """
    This class is used to map block indexes and their corresponding hashes.
    Hashes can be accesse by the block index and hashes can be used to retrieve a list of block indexes.
    """

    def __init__(self, block_size, hash_size, image_path=""):
        self.block_size = block_size
        self.hash_size = hash_size
        self.image_path = image_path

        self.hash_by_index = []
        self.indexes_by_hash = {}

        if image_path:
            self.load()

    def indexes_by_hash(self, hash: Any) -> list:
        """
        Return the list of indexes that have the given hash.
        """
        return self.indexes_by_hash[hash].copy()

    def hash_by_index(self, index: int) -> Any:
        """
        Return the hash of the block with the given index.
        """
        return self.hash_by_index[index]

    def load(self):
        """
        Load the hashes and indexes from the image file.
        """
        store = BlockHashStore(self.block_size, self.hash_size)
        with open(self.image_path, "rb") as f:
            index = 0
            while True:
                block = f.read(self.block_size)
                if not block:
                    break

                hash = sha256(block).digest()
                store.add_hash(hash, block)
                self.hash_by_index.append(hash)

                if hash not in self.indexes_by_hash:
                    self.indexes_by_hash[hash] = []

                self.indexes_by_hash[hash].append(index)
                index += 1

    def changed_blocks_from(self, updated_hashes) -> List[Block]:
        """
        Finds the differences between two files by comparing their blocks.

        Args:
            initial_hashes (tempfile): The temporary file object containing the initial file's hashes.
            updated_hashes (tempfile): The temporary file object containing the updated file's hashes.

        Returns:
            list: A list of indexes where the blocks in the initial file differ from the updated file.
        """
        changed_indexes = []
        index = 0
        while True:
            initial_hash = self.hash_by_index(index)
            updated_hash = updated_hashes.hash_by_index(index)
            if not initial_hash or not updated_hash:
                # Raw images are same size, one is out of blocks then both are
                break
            if initial_hash != updated_hash:
                changed_indexes.append(Block(index, b"", self.block_size))
            index += 1

        return changed_indexes
