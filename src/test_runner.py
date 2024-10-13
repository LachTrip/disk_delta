import csv
import datetime
from enum import Enum
from hashlib import sha256
import hashlib
import itertools
import math
import os
import diskdelta
import subprocess
import time
import yaml

from diskdelta.block_hash_store import BlockHashStore
from diskdelta.debug import Debug
from diskdelta.delta_decoder import DeltaDecoder
from diskdelta.index_hash_mapper import IndexHashMapper


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
        super().__init__(
            initial_image_path, target_image_path, output_folder_path, block_size
        )
        self.lach_version = lach_version

    def run(self) -> Result:
        try:
            output_path = os.path.join(
                self.output_folder_path,
                f"LACH{self.lach_version}__{os.path.basename(self.initial_image_path)}__{os.path.basename(self.target_image_path)}__{self.block_size}B_Block",
            )

            tb = 1024**4
            # Assuming drive TBW is 100,000 (very high)
            self.digest_size = math.ceil(2 * math.log2(100000 * tb / self.block_size))

            time_start = time.perf_counter()

            disk_delta = diskdelta.DiskDelta(
                self.initial_image_path,
                self.target_image_path,
                self.block_size,
                self.digest_size,
            )
            disk_delta.build_message()
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
        super().__init__(
            initial_image_path, target_image_path, output_folder_path, block_size
        )
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
                        f"--only-write-batch={output_path}",
                        self.target_image_path,
                        self.initial_image_path,
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


def load_test_config() -> (
    tuple[list[tuple[str, str]], list[Technique], list[int], str, int]
):
    with open("config/test.yaml", "r") as config_file:
        config = yaml.safe_load(config_file)

    input_path_couples = config["input_path_couples"]

    techniques = [Technique(technique) for technique in config["techniques"]]

    image_size = os.path.getsize(input_path_couples[0][0])
    block_sizes = [
        (
            size if isinstance(size, int) else
            image_size if size == "image_size" else
            int(math.sqrt(image_size)) if size == "sqrt_image_size" else
            None
        )
        for size in config["block_sizes"]
    ]

    if None in block_sizes:
        raise ValueError("Invalid block size")

    lach_version = str(config["lach_version"])
    xz_level = int(config["xz_level"])

    return input_path_couples, techniques, block_sizes, lach_version, xz_level # type: ignore


def load_completed_tests_from_file(file_path: str) -> set:
    with open(file_path, "r") as file:
        reader = csv.reader(file)
        next(reader)  # Skip the header
        return {tuple(row[: len(TEST_HEADERS)]) for row in reader}


def load_completed_tests(directory_path: str) -> set:
    completed_tests = set()
    for file_name in os.listdir(directory_path):
        if file_name.endswith(".csv"):
            file_path = os.path.join(directory_path, file_name)
            completed_tests |= load_completed_tests_from_file(file_path)
    return completed_tests

def test_run_and_write_results():
    input_path_couples, techniques, block_sizes, lach_version, xz_level = (
        load_test_config()
    )
    completed_tests = set() # load_completed_tests("output/test_results")
    now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    test_output_path = f"output/test_results/{now}.csv"
    deltas_output_path = f"output/delta/{now}"
    os.makedirs(deltas_output_path, exist_ok=True)
    print("Running tests...")
    for input_paths, technique, block_size in itertools.product(
        input_path_couples, techniques, block_sizes
    ):
        if technique == Technique.LACH:
            test = LachTest(
                input_paths[0],
                input_paths[1],
                deltas_output_path,
                block_size,
                lach_version,
            )
        elif technique == Technique.XZ:
            test = XzTest(
                input_paths[0],
                input_paths[1],
                deltas_output_path,
                block_size,
                xz_level,
            )
        elif technique == Technique.RSYNC:
            test = RsyncTest(
                input_paths[0],
                input_paths[1],
                deltas_output_path,
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
        with ResultsWriter(test_output_path) as f:
            result = test.run()
            print("Result: " + str(result))
            f.write(test, result)
    print("Tests finished")

def lach_run_and_validate():
    input_path_couples, techniques, block_sizes, lach_version, xz_level = (
        load_test_config()
    )
    
    now = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    deltas_output_path = f"output/delta/{now}"
    os.makedirs(deltas_output_path, exist_ok=True)
    print("Running tests...")

    test_output_path = f"output/test_results/{now}.csv"

    for input_paths, block_size in itertools.product(
        input_path_couples, block_sizes
    ):
        initial_image_path = input_paths[0]
        target_image_path = input_paths[1]
        delta_file_name = f"LACH__{os.path.basename(initial_image_path)}__{os.path.basename(target_image_path)}__{block_size}B_Block"

        print("Running test: " + delta_file_name)

        

        delta_output_path = os.path.join(
            deltas_output_path,
            delta_file_name,
        )

        tb = 1024**4
        # Assuming drive TBW is 100,000 (very high)
        digest_size = math.ceil(2 * math.log2(100000 * tb / block_size))

        time_start = time.perf_counter()

        sending_disk_delta = diskdelta.DiskDelta(
            initial_image_path,
            target_image_path,
            block_size,
            digest_size,
        )
        sending_disk_delta.build_message()
        sending_disk_delta.write_message_to_file(delta_output_path)

        time_end = time.perf_counter()

        compressed_size = os.path.getsize(delta_output_path)
        compression_ratio = (
            os.path.getsize(target_image_path) / compressed_size
        )
        compression_time = time_end - time_start
    
        print("Message created")

        reconstuct_directory = f"output/reconstructed/{now}"
        os.makedirs(reconstuct_directory, exist_ok=True)
        
        store = BlockHashStore(block_size, digest_size)
        initial_hashes = IndexHashMapper(initial_image_path, block_size, digest_size)
        decoder = DeltaDecoder(initial_hashes, store)
        message = decoder.get_message_from_bits(delta_output_path)

        # verify the message
        if sending_disk_delta.message == message:
            print("Message verified")

        # ensure the output directory exists
        output_file_path = os.path.join(reconstuct_directory, delta_file_name)
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)

        # apply message to the image
        recon_disk_delta = diskdelta.DiskDelta(initial_image_path, target_image_path, block_size, digest_size)
        recon_disk_delta.message = message
        recon_disk_delta.known_blocks = store
        recon_disk_delta.apply_message(initial_image_path, output_file_path)

        # verify the reconstructed image
        target_hash = hashlib.sha256(open(target_image_path, "rb").read()).hexdigest()
        recon_hash = hashlib.sha256(open(output_file_path, "rb").read()).hexdigest()
        if target_hash == recon_hash:
            print("Reconstructed image verified")
        else:
            print("Reconstructed image verification failed")

        with ResultsWriter(test_output_path) as f:
            test = LachTest(
                initial_image_path,
                target_image_path,
                deltas_output_path,
                block_size,
                lach_version,
            )
            result = Result(compressed_size, compression_ratio, compression_time)
            f.write(test, result)
    
def main():
    lach_run_and_validate()



if __name__ == "__main__":
    main()
