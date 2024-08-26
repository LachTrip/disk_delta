import argparse
import datetime
import hashlib
import math
import tempfile
import os

from diskdelta import DiskDelta


def main():
    print(f"Running diskdelta: {datetime.datetime.now()}")

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-i",
        "--initial_image_path",
        help="Path to the initial image",
        default="input/initial_image.img",
    )

    parser.add_argument(
        "-t",
        "--target_image_path",
        help="Path to the target image",
        default="input/target_image.img",
    )

    now_formatted = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    default_output_path = f"output/diskdelta_{now_formatted}"
    parser.add_argument(
        "-o",
        "--output_path",
        help="Path to save the disk delta",
        default=default_output_path,
    )

    args = parser.parse_args()

    block_size = 4096 # In bytes
    tb = 1024**4
    # Assuming drive TBW is 100,000 (very high)
    digest_size = math.ceil(2 * math.log2(100000 * tb / block_size))

    # Generate the disk delta
    disk_delta = DiskDelta(
        args.initial_image_path, args.target_image_path, block_size, digest_size
    )

    # write to file as bits
    # disk_delta.write_bits_to_file(args.output_path + "_bits")
    
    # bit_message = disk_delta.generate_bitarray()
    # with open(args.output_path + "_bits", "wb") as f:
    #     bit_message.tofile(f)

    # write to file as string (only works for text files)
    # str_message = disk_delta.generate_string()
    # with open(args.output_path + "_str", "wb") as f:
    #     f.write(str_message.encode("utf-8"))

    message_size = disk_delta.message.calculate_size_bits(disk_delta.known_blocks)
    message_Gb = message_size / 8 / 1024 / 1024 / 1024
    print("Message size: ", message_Gb, "Gb")

    decoder = disk_delta.get_decoder()
    reg_message = decoder.get_message_from_bits(args.output_path + "_bits")

    disk_delta.apply_message(reg_message, args.initial_image_path, args.output_path + "_reconstructed_image.img")

    # Create hashes for target and reconstructed image and compare
    with open(args.target_image_path, "rb") as f:
        digest_target = hashlib.file_digest(f, "sha256")

    with open(args.output_path + "_reconstructed_image.img", "rb") as f:
        digest_reconstructed = hashlib.file_digest(f, "sha256")
    
    if digest_target.digest() == digest_reconstructed.digest():
        print("Reconstructed image matches target image")
    else:
        print("Reconstructed image does not match target image")
        print(f"Target image hash       : {digest_target.hexdigest()}")
        print(f"Reconstructed image hash: {digest_reconstructed.hexdigest()}")

if __name__ == "__main__":
    main()
