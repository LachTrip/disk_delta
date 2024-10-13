import hashlib
import math
import os
import re
import shutil
from diskdelta import DiskDelta
from diskdelta.block_hash_store import BlockHashStore
from diskdelta.delta_decoder import DeltaDecoder
from diskdelta.index_hash_mapper import IndexHashMapper


def main():
    test_run_start = "2024-10-13_10-57-28"
    input_directory = "input/"
    delta_directory = f"output/delta/{test_run_start}"
    reconstuct_directory = f"output/reconstructed/{test_run_start}"
    delta_file_regex = re.compile(r"LACH1__\w+\.img__\w+.img__\d+\w+" ) 
    # find all files matching the regex
    delta_files = [f for f in os.listdir(delta_directory) if delta_file_regex.match(f)]
    for delta_file in delta_files:
        # split the file name to get the initial and target image paths
        technique, initial_image_name, target_image_name, block_size = delta_file.split("__")
        
        # get number from format "[number]B_Block"
        block_size = int(block_size[:-7])
        tb = 1024**4
        digest_size = math.ceil(2 * math.log2(100000 * tb / block_size))
        store = BlockHashStore(block_size, digest_size)
        initial_hashes = IndexHashMapper(os.path.join(input_directory, initial_image_name), block_size, digest_size)
        decoder = DeltaDecoder(initial_hashes, store)
        message = decoder.get_message_from_bits(os.path.join(delta_directory, delta_file))

        initial_image_path = os.path.join(input_directory, initial_image_name)
        target_image_path = os.path.join(input_directory, target_image_name)

        # ensure the output directory exists
        output_file_path = os.path.join(reconstuct_directory, delta_file)
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        # apply message to the image
        disk_delta = DiskDelta(initial_image_path, target_image_path, block_size, digest_size)
        disk_delta.message = message
        disk_delta.known_blocks = store
        disk_delta.apply_message(initial_image_path, output_file_path)

        # verify the reconstructed image
        target_hash = hashlib.sha256(open(target_image_path, "rb").read()).hexdigest()
        recon_hash = hashlib.sha256(open(output_file_path, "rb").read()).hexdigest()
        if target_hash == recon_hash:
            print("Reconstructed image verified")
        else:
            print("Reconstructed image verification failed")
            print(f"Original hash: {target_hash}")
            print(f"Reconstructed hash: {recon_hash}")



if __name__ == "__main__":
    main()