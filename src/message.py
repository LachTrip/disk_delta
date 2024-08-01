# from hashlib import sha256


# class Message:
#     def __init__(self):
#         disk_indexes = []
#         block_hashes = []
#         block_literals = []

#     def add_disk_index(self, disk_index):
#         self.disk_indexes.append(disk_index)

#     def add_block_hash(self, block_hash):
#         self.block_hashes.append(block_hash)

#     def add_block_literal(self, block_literal):
#         self.block_literals.append(block_literal)

# class Block_Hash:
#     def __init__(self, block):
#         value = hash_alg(block)

#     def hash_alg(self, block):
#         return sha256(block)