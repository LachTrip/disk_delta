from hashlib import sha256
from typing import Any

class IndexHashMapper:
    '''
    This class is used to map block indexes and their corresponding hashes.
    Hashes can be accesse by the block index and hashes can be used to retrieve a list of block indexes.
    '''
    def __init__(self, image_path, block_size):
        self.hash_by_index = []
        self.indexes_by_hash = {}
        self.image_path = image_path
        self.block_size = block_size
        self.load()

    def indexes_by_hash(self, hash: Any) -> list:
        '''
        Return the list of indexes that have the given hash.
        '''
        return self.indexes_by_hash[hash].copy()
    
    def hash_by_index(self, index: int) -> Any:
        '''
        Return the hash of the block with the given index.
        '''
        return self.hash_by_index[index]
    
    def load(self):
        '''
        Load the hashes and indexes from the image file.
        '''
        with open(self.image_path, 'rb') as f:
            index = 0
            while True:
                block = f.read(self.block_size)
                if not block:
                    break
                hash = sha256(block).digest()
                self.hash_by_index.append(hash)
                if hash not in self.indexes_by_hash:
                    self.indexes_by_hash[hash] = []
                self.indexes_by_hash[hash].append(index)
                index += 1