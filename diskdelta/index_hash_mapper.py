from hashlib import sha256
import os
from typing import Any, List


class Hasher:
    """
    This class is used to hash blocks of data.
    """

    def __init__(self, hash_size: int):
        self.hash_size = hash_size

    def hash(self, data: Any) -> Any:
        """
        Return the hash of the given data.
        """
        return sha256(data).digest()[0 : self.hash_size]

class IndexHashMapper:
    """
    This class is used to map block indexes and their corresponding hashes.
    Hashes can be accesse by the block index and hashes can be used to retrieve 
    a list of block indexes.
    """

    def __init__(self, image_path, block_size, hash_size):
        self.block_size = block_size
        self.hash_size = hash_size
        self.image_path = image_path

        self.hash_by_index = []
        self.indexes_by_hash = {}

        if image_path:
            self.load()

    def size(self):
        return len(self.hash_by_index)

    def get_indexes_by_hash(self, hash: Any) -> list:
        """
        Return the list of indexes that have the given hash.
        """
        if hash in self.indexes_by_hash:
            return self.indexes_by_hash[hash].copy()
        else:
            return []

    def get_hash_by_index(self, index: int) -> Any:
        """
        Return the hash of the block with the given index.
        """
        return self.hash_by_index[index]

    def load(self):
        """
        Load the hashes and indexes from the image file.
        """
        hasher: Hasher = Hasher(self.hash_size)
        with open(self.image_path, "rb") as f:
            index = 0
            while True:
                image_bytes_num = os.path.getsize(self.image_path)
                image_size = image_bytes_num // self.block_size
                five_percent = image_size // 20
                if five_percent != 0:
                    if index % five_percent == 0:
                        if index/five_percent*5 != 100:
                            print(f"{int(index/five_percent)*5}%... ", end="")
                        else:
                            print("100%")

                # if index % 1000 == 0:
                #     print(f"Index {index}... ", end="")
                block = f.read(self.block_size)
                if not block:
                    break

                hash = hasher.hash(block)
                self.hash_by_index.append(hash)

                if hash not in self.indexes_by_hash:
                    self.indexes_by_hash[hash] = []

                self.indexes_by_hash[hash].append(index)
                index += 1
    
    def data_by_index(self, index: int) -> Any:
        """
        Return the data of the block with the given index.
        """
        with open(self.image_path, "rb") as f:
            f.seek(index * self.block_size)
            return f.read(self.block_size)
