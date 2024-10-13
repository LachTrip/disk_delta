from hashlib import sha256
import math
import os

from diskdelta.debug import Debug


class Hasher:
    """
    This class is used to hash blocks of data.
    """

    def __init__(self, hash_size: int):
        self.hash_size = hash_size

    def hash(self, data: bytes) -> bytes:
        """
        Return the hash of the given data.
        """
        num_bytes = math.ceil(self.hash_size / 8)
        full_hash = sha256(data).digest()
        hash_bytes = full_hash[:num_bytes]
        if self.hash_size % 8 != 0:
            mask = 0xFF << (8 - (self.hash_size % 8)) & 0xFF
            hash_bytes = hash_bytes[:-1] + bytes([hash_bytes[-1] & mask])

        return hash_bytes


class IndexHashMapper:
    """
    This class is used to map indexes of blocks in an image and their
    corresponding hashes. Use block index to access corresponding hash and
    hashes to retrieve a list of block indexes with that hash.
    """

    def __init__(
        self, image_path: str, block_size_by_bytes: int, hash_size_by_bytes: int
    ):
        self.block_literal_size = block_size_by_bytes
        self.block_hash_size = hash_size_by_bytes
        self.image_path = image_path

        self.indexes_by_hash: dict[bytes, list[tuple[int, int]]] = {}

        if image_path:
            self.load()

    def load(self):
        """
        Load the hashes and indexes from the image file.
        """
        hasher: Hasher = Hasher(self.block_hash_size)
        with open(self.image_path, "rb") as f:
            index = 0
            while True:
                if not self.load_entry(f, index, hasher):
                    break
                index += 1

    def load_entry(self, f, index, hasher):
        if Debug.isEnabled:
            self.log_generating_hashes_progress(index)

        block = f.read(self.block_literal_size)
        if not block:
            return False

        hash = hasher.hash(block)

        if hash not in self.indexes_by_hash:
            self.indexes_by_hash[hash] = []

        self.add_index_to_rle(self.indexes_by_hash[hash], index)

        return True
    
    def add_index_to_rle(self, rle: list[tuple[int, int]], index: int):
        if len(rle) == 0:
            rle.append((index, 1))
            return

        last_index, count = rle[-1]
        if last_index + count == index:
            rle[-1] = (last_index, count + 1)
        else:
            rle.append((index, 1))

    def get_indexes_by_hash(self, hash: bytes) -> list[tuple[int, int]]:
        """
        Return the list of indexes that have the given hash.
        """
        if hash in self.indexes_by_hash:
            return self.indexes_by_hash[hash].copy()
        else:
            return []

    def get_hash_by_index(self, index: int) -> bytes:
        """
        Return the hash of the block with the given index.
        """
        with open(self.image_path, "rb") as f:
            f.seek(index * self.block_literal_size)
            block = f.read(self.block_literal_size)
            hasher = Hasher(self.block_hash_size)
            return hasher.hash(block)

    def literal_by_index(self, index: int) -> bytes:
        """
        Return the data of the block with the given index.
        """
        with open(self.image_path, "rb") as f:
            f.seek(index * self.block_literal_size)
            return f.read(self.block_literal_size)

    def image_size(self):
        return os.path.getsize(self.image_path)

    def log_generating_hashes_progress(self, index: int):
        image_bytes_num = os.path.getsize(self.image_path)
        image_size = image_bytes_num // self.block_literal_size
        five_percent = image_size // 20

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
