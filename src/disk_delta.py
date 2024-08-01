from hashlib import sha256
from tempfile import TemporaryFile

class DiskDelta:
    BLOCK_SIZE = 4096

    @staticmethod
    def generate(initial_image_path, target_image_path):
        # Create initial and target image tempfiles
        initial_image_hashes = DiskDelta.store_hashes_in_tempfile(initial_image_path)
        target_image_hashes = DiskDelta.store_hashes_in_tempfile(target_image_path)

        changed_indexes = DiskDelta.find_differences(initial_image_hashes, target_image_hashes)

        blocks_in_initial = DiskDelta.indexes_on_disk(changed_indexes, initial_image_path, target_image_path)



        return initial_image.delta(target_image)
    
    def store_hashes_in_tempfile(self, file_path):
        # Store the hashes of each block in a tempfile
        with open(file_path, 'rb') as file:
            hashes = TemporaryFile()
            while True:
                block = file.read(self.BLOCK_SIZE)
                if not block:
                    break
                block_hash = sha256(block).digest()
                hashes.write(block_hash)
        return hashes
    
    def find_differences(self, initial_file_path, updated_file_path):
        # Find the changed blocks between the initial and target files
        changed_indexes = []
        index = 0
        with open(initial_file_path, 'rb') as init_file:
            with open(updated_file_path, 'rb') as updated_file:
                while True:
                    init_block = init_file.read(self.BLOCK_SIZE)
                    updated_block = updated_file.read(self.BLOCK_SIZE)
                    if not init_block:
                        print (f"End of initial file at index {index}")
                    if not updated_block:
                        print (f"End of updated file at index {index}")
                    if not init_block or not updated_block:
                        break
                    if init_block != updated_block:
                        changed_indexes.append(index)
                    index += 1
        return changed_indexes

    def indexes_on_disk(self, indexes, initial_file_path, target_file_path):
        blocks_on_disk =  []
        for index in indexes:
            # Get block in target file
            with open(target_file_path, 'rb') as target_file:
                target_file.seek(index * self.BLOCK_SIZE)
                target_block = target_file.read(self.BLOCK_SIZE)
            # Check already found blocks
            if target_block in blocks_on_disk:
                blocks_on_disk.append(b'')
                continue
            # Check every block in initial file to see if it matches the target block
            with open(initial_file_path, 'rb') as initial_file:
                while True:
                    initial_block = initial_file.read(self.BLOCK_SIZE)
                    if not initial_block:
                        blocks_on_disk.append(b'')
                        break
                    if initial_block == target_block:
                        blocks_on_disk.append(index)
                        break
        return blocks_on_disk