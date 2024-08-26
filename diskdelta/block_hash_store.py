import os


class BlockHashStore:
    """
    A simple file store for block hashes.
    """

    def __init__(self, block_size, digest_size):
        self.block_size: int = block_size
        self.digest_size: int = digest_size
        self.hashes: list[bytes]= []
        self.load()

    def load(self):
        """
        Load the hashes from the file. Skip block_size
        """
        filepath = "data/hashes_" + str(self.block_size) + "_" + str(self.digest_size)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if not os.path.exists(filepath):
            open(filepath, "w").close()
        with open(filepath, "rb") as f:
            index = 0
            while True:
                f.seek(index * (self.block_size + self.digest_size))
                block = f.read(self.digest_size)
                if not block:
                    break
                self.hashes.append(block)
                index += 1

    def add(self, hash: bytes, data: bytes):
        """
        Add a hash to the store.
        """
        if hash in self.hashes:
            return
        self.hashes.append(hash)
        with open("data/hashes_" + self.block_size + "_" + self.digest_size, "ab") as f:
            f.write(hash + data)

    def get_data_by_hash(self, hash: bytes) -> bytes:
        """
        Get the data associated with a hash.
        """
        # Get the index of the hash
        index = self.hashes.index(hash)
        # Get the data associated with the hash
        filepath = "data/hashes_" + self.block_size + "_" + self.digest_size
        with open(filepath, "rb") as f:
            f.seek(index * (self.block_size + self.digest_size) + self.digest_size)
            data = f.read(self.block_size)
        return data

    def contains_hash(self, hash: bytes) -> bool:
        """
        Check if the store contains a hash.
        """
        return hash in self.hashes
