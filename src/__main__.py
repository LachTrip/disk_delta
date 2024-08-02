import argparse
import datetime
import tempfile
import os

from disk_delta import DiskDelta


def main():
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

    # Generate the disk delta as a binary file
    disk_delta = DiskDelta(8)
    delta_message = disk_delta.generate_binary(
        args.initial_image_path, args.target_image_path
    )

    # Save the disk delta to a file
    with open(args.output_path, "wb") as f:
        f.write(delta_message)


if __name__ == "__main__":
    main()
