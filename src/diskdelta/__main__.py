import argparse
import datetime
import hashlib
import math
from string import digits

from diskdelta import DiskDelta
from diskdelta.debug import Debug


def get_args():
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

    parser.add_argument(
        "-b",
        "--block_size",
        help="Block size in bytes",
        default=1,
    )

    now_formatted = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    default_output_path = f"output/diskdelta_{now_formatted}"
    parser.add_argument(
        "-o",
        "--output_path",
        help="Path to save the disk delta",
        default=default_output_path,
    )

    parser.add_argument(
        "-d",
        "--enable_debug",
        help="Enable debug output",
        action="store_true",
    )

    return parser.parse_args()


def get_message_size(disk_delta):
    message_size = disk_delta.message.calculate_size_bits()

    if message_size < 8:
        return str(message_size) + " bits"
    elif message_size < 8 * 1024:
        message_bytes = message_size / 8
        return str(message_bytes) + " bytes"
    elif message_size < 8 * 1024 * 1024:
        message_Kb = message_size / 8 / 1024
        return str(message_Kb) + " Kb"
    elif message_size < 8 * 1024 * 1024 * 1024:
        message_Mb = message_size / 8 / 1024 / 1024
        return str(message_Mb) + " Mb"
    else:
        message_Gb = message_size / 8 / 1024 / 1024 / 1024
        return str(message_Gb) + " Gb"


def simulate_send_disk_delta(
    initial_image_path: str,
    target_image_path: str,
    block_size: int,
    digest_size_bytes: int,
    output_path: str,
):
    """
    Simulate sending disk delta
    """
    # Generate the disk delta
    disk_delta = DiskDelta(
        initial_image_path, target_image_path, block_size, digest_size_bytes
    )

    # write to file as bits
    disk_delta.write_message_to_file(output_path + "_bits")

    Debug.log(f"Message size: {get_message_size(disk_delta)}")

    return disk_delta


def simulate_receive_disk_delta(
    disk_delta: DiskDelta,
    output_path: str,
    initial_image_path: str,
    target_image_path,
    block_size,
    digest_size,
):
    Debug.log("Generating message from bits message")
    Debug.increment_indent()
    decoder = disk_delta.get_decoder()
    regenerated_message = decoder.get_message_from_bits(output_path + "_bits")
    Debug.increment_indent(-1)

    # Compare the regenerated message with the original message
    original_message = disk_delta.message
    assert regenerated_message == original_message

    Debug.log("Reconstructing target image")

    disk_delta.apply_message(
        regenerated_message,
        initial_image_path,
        output_path + "_reconstructed_image.img",
    )

    Debug.log("Verifying reconstructed image")
    target_hash = hashlib.sha256(open(target_image_path, "rb").read()).hexdigest()
    recon_hash = hashlib.sha256(
        open(output_path + "_reconstructed_image.img", "rb").read()
    ).hexdigest()
    if target_hash == recon_hash:
        Debug.log("Reconstructed image verified")
    else:
        Debug.log("Reconstructed image verification failed")
        Debug.log(f"Original hash: {target_hash}")
        Debug.log(f"Reconstructed hash: {recon_hash}")


def main():
    args = get_args()
    block_size = int(args.block_size)  # In bytes
    initial_image_path = args.initial_image_path
    target_image_path = args.target_image_path
    output_path = args.output_path

    tb = 1024**4
    # Assuming drive TBW is 100,000 (very high)
    digest_size = math.ceil(2 * math.log2(100000 * tb / block_size))

    Debug.enable(args.enable_debug)

    time_start = datetime.datetime.now()
    Debug.log(f"Running diskdelta: {time_start}")
    Debug.log("")

    Debug.log("Simulating message creation:")
    Debug.increment_indent()

    disk_delta: DiskDelta = simulate_send_disk_delta(
        initial_image_path,
        target_image_path,
        block_size,
        digest_size,
        output_path,
    )

    Debug.increment_indent(-1)
    Debug.log("")
    Debug.log("Simulating message application:")
    Debug.increment_indent()

    simulate_receive_disk_delta(
        disk_delta,
        output_path,
        initial_image_path,
        target_image_path,
        block_size,
        digest_size,
    )

    Debug.increment_indent(-1)
    Debug.log("")
    time_end = datetime.datetime.now()
    Debug.log(f"Completed: {time_end}")
    time_complete_arr = str(time_end - time_start).split(":")
    time_complete = (
        time_complete_arr[0]
        + " hours, "
        + time_complete_arr[1]
        + " minutes, "
        + time_complete_arr[2]
        + " seconds"
    )
    Debug.log(f"Time to complete: {time_complete}")
    Debug.log("")


if __name__ == "__main__":
    main()
