import csv
import datetime
from enum import Enum
from hashlib import sha256
import itertools
import math
import os
import diskdelta
import subprocess
import time
import yaml


class Technique(Enum):
    LACH = "lach"
    XZ = "xz"
    RSYNC = "rsync"


TEST_HEADERS = [
    "image_size",
    "initial_image_name",
    "initial_image_hash",
    "target_image_name",
    "target_image_hash",
    "compression_technique",
    "block_size",
]

RESULT_HEADERS = [
    "compressed_message_size",
    "compression_ratio",
    "compression_time",
]


class Result:

    def __init__(
        self, compressed_size_bits: int, compression_ratio: float, time_seconds: float
    ):
        self.size_bits = compressed_size_bits
        self.ratio = compression_ratio
        self.time = time_seconds

    def __str__(self):
        return f"{self.size_bits},{self.ratio},{self.time}"

    def __repr__(self):
        return self.__str__()


class BaseTest:
    def __init__(
        self,
        initial_image_path: str,
        target_image_path: str,
        output_folder_path: str,
        block_size: int,
    ):
        self.initial_image_path = initial_image_path
        self.target_image_path = target_image_path
        self.output_folder_path = output_folder_path
        self.block_size = block_size

    def run(self) -> Result:
        raise NotImplementedError("Subclasses should implement this method")

    def __str__(self):
        raise NotImplementedError("Subclasses should implement this method")

    def __repr__(self):
        return self.__str__()


class LachTest(BaseTest):
    def __init__(
        self,
        initial_image_path: str,
        target_image_path: str,
        output_folder_path: str,
        block_size: int,
        lach_version: str,
    ):
        super().__init__(initial_image_path, target_image_path, output_folder_path, block_size)
        self.lach_version = lach_version

    def run(self) -> Result:
        try:
            output_path = os.path.join(
                self.output_folder_path,
                f"LACH{self.lach_version}__{os.path.basename(self.initial_image_path)}__{os.path.basename(self.target_image_path)}__{self.block_size}B_Block",
            )

            tb = 1024**4
            # Assuming drive TBW is 100,000 (very high)
            self.digest_size = math.ceil(
                2 * math.log2(100000 * tb / self.block_size)
            )

            time_start = time.perf_counter()

            disk_delta = diskdelta.DiskDelta(
                self.initial_image_path,
                self.target_image_path,
                self.block_size,
                self.digest_size,
            )
            disk_delta.write_message_to_file(output_path)

            time_end = time.perf_counter()

            compressed_size = os.path.getsize(output_path)
            compression_ratio = (
                os.path.getsize(self.target_image_path) / compressed_size
            )
            compression_time = time_end - time_start

            return Result(compressed_size, compression_ratio, compression_time)
        except Exception as e:
            print(f"Error during test run: {e}")
            raise

    def __str__(self):
        return (
            f"{os.path.getsize(self.initial_image_path)},"
            f"{os.path.basename(self.initial_image_path)},"
            f"{sha256(open(self.initial_image_path, 'rb').read()).hexdigest()},"
            f"{os.path.basename(self.target_image_path)},"
            f"{sha256(open(self.target_image_path, 'rb').read()).hexdigest()},"
            f"{Technique.LACH.value + self.lach_version},"
            f"{self.block_size}"
        )


class XzTest(BaseTest):
    def __init__(
        self,
        initial_image_path: str,
        target_image_path: str,
        output_folder_path: str,
        block_size: int,
        xz_level: int,
    ):
        super().__init__(initial_image_path, target_image_path, output_folder_path, block_size)
        self.xz_level = xz_level

    def run(self) -> Result:
        if self.block_size == 1:
            return Result(-1, -1, -1)
        try:
            output_path = os.path.join(
                self.output_folder_path,
                f"XZ__{os.path.basename(self.target_image_path)}__{self.block_size}B_Block",
            )

            time_start = time.perf_counter()

            try:
                subprocess.run(
                    [
                        "xz",
                        "-k",
                        "-z",
                        f"-{self.xz_level}",
                        "-e",
                        "-c",
                        "-T0",
                        f"--block-size={self.block_size}",
                        self.target_image_path,
                    ],
                    stdout=open(output_path, "wb"),
                    stderr=subprocess.PIPE,
                    text=True,
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Error: {e.stderr}")
                return Result(-1, -1, -1)

            time_end = time.perf_counter()

            compressed_size = os.path.getsize(output_path)
            compression_ratio = (
                os.path.getsize(self.target_image_path) / compressed_size
            )
            compression_time = time_end - time_start

            return Result(compressed_size, compression_ratio, compression_time)
        except Exception as e:
            print(f"Error during test run: {e}")
            raise

    def __str__(self):
        return (
            f"{os.path.getsize(self.initial_image_path)},"
            f"{os.path.basename(self.initial_image_path)},"
            f"{sha256(open(self.initial_image_path, 'rb').read()).hexdigest()},"
            f"{os.path.basename(self.target_image_path)},"
            f"{sha256(open(self.target_image_path, 'rb').read()).hexdigest()},"
            f"{Technique.XZ.value + str(self.xz_level)},"
            f"{self.block_size}"
        )


class RsyncTest(BaseTest):
    def run(self) -> Result:
        try:
            output_path = os.path.join(
                self.output_folder_path,
                f"RSYNC__{os.path.basename(self.initial_image_path)}__{os.path.basename(self.target_image_path)}__{self.block_size}B_Block",
            )

            time_start = time.perf_counter()

            try:
                subprocess.run(
                    [
                        "rsync",
                        "--no-whole-file",
                        f"--block-size={self.block_size}",
                        self.initial_image_path,
                        self.target_image_path,
                        output_path,
                    ],
                    check=True,
                )
            except subprocess.CalledProcessError as e:
                print(f"Error: {e.stderr}")
                return Result(-1, -1, -1)
            
            time_end = time.perf_counter()

            compressed_size = os.path.getsize(output_path)
            compression_ratio = (
                os.path.getsize(self.target_image_path) / compressed_size
            )
            compression_time = time_end - time_start

            return Result(compressed_size, compression_ratio, compression_time)
        except Exception as e:
            print(f"Error during test run: {e}")
            raise

    def __str__(self):
        return (
            f"{os.path.getsize(self.initial_image_path)},"
            f"{os.path.basename(self.initial_image_path)},"
            f"{sha256(open(self.initial_image_path, 'rb').read()).hexdigest()},"
            f"{os.path.basename(self.target_image_path)},"
            f"{sha256(open(self.target_image_path, 'rb').read()).hexdigest()},"
            f"{Technique.RSYNC.value},"
            f"{self.block_size}"
        )


class ResultsWriter:
    def __init__(self, file_path):
        # Check file is csv that follows the format
        self.file_path = file_path
        self.file = open(file_path, "a+")
        self.initialize_or_validate_file()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def initialize_or_validate_file(self):
        if self.file.tell() == 0:
            self.init_headers()
        else:
            self.validate_headers()

    def init_headers(self):
        self.file.write(",".join(TEST_HEADERS + RESULT_HEADERS) + "\n")

    def validate_headers(self):
        self.file.seek(0)
        headers = self.file.readline().strip().split(",")
        if headers != TEST_HEADERS + RESULT_HEADERS:
            raise ValueError(
                "Invalid headers. Please provide a valid or empty csv file"
            )
        self.file.seek(0, 2)

    def write(self, test: BaseTest, result: Result):
        self.file.write(str(test) + ", " + str(result) + "\n")


def load_test_config() -> tuple[list[tuple[str, str]], list[Technique], list[int], str, int]:
    with open("config/test.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    input_path_couples = config["input_path_couples"]

    techniques = [Technique(technique) for technique in config["techniques"]]

    image_size = os.path.getsize(input_path_couples[0][0])
    block_sizes = [
        (
            1
            if size == 1
            else image_size if size == "image_size" else int(math.sqrt(image_size))
        )
        for size in config["block_sizes"]
    ]

    lach_version = str(config["lach_version"])
    xz_level = int(config["xz_level"])

    return input_path_couples, techniques, block_sizes, lach_version, xz_level

def load_completed_tests(file_path: str) -> set:
    if not os.path.exists(file_path):
        return set()

    with open(file_path, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        return {tuple(row[:len(TEST_HEADERS)]) for row in reader}

def main():
    input_path_couples, techniques, block_sizes, lach_version, xz_level = load_test_config()
    completed_tests = load_completed_tests("output/test_results.csv")

    print("Running tests...")
    for input_paths, technique, block_size in itertools.product(
        input_path_couples, techniques, block_sizes
    ):
        if technique == Technique.LACH:
            test = LachTest(
                input_paths[0],
                input_paths[1],
                "output/images/",
                block_size,
                lach_version,
            )
        elif technique == Technique.XZ:
            test = XzTest(
                input_paths[0],
                input_paths[1],
                "output/images/",
                block_size,
                xz_level,
            )
        elif technique == Technique.RSYNC:
            test = RsyncTest(
                input_paths[0],
                input_paths[1],
                "output/images/",
                block_size,
            )
        else:
            raise ValueError("Invalid technique")

        test_str = str(test)
        test_tuple = tuple(test_str.split(","))

        if test_tuple in completed_tests:
            print(f"Skipping already run test: {test_str}")
            continue

        print("Test: " + str(test), end=" - ")
        with ResultsWriter("output/test_results.csv") as f:
            result = test.run()
            print("Result: " + str(result))
            f.write(test, result)
    print("Tests finished")


if __name__ == "__main__":
    main()