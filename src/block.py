from enum import Enum

from index_hash_mapper import IndexHashMapper

class Block:
    '''
    Block to be sent
    '''
    
    blocks = []

    def __init__(self, data, block_size):
        self.data = data
        self.block_size = block_size
        self.sending_type = BlockDataType.Literal
        Block.blocks.append(self)

class BlockDataType(Enum):
    Literal = 1
    Hash = 2
    DiskIndex = 3
    Reference = 4