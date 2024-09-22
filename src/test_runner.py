import datetime
from enum import Enum
import itertools
import math
import os
import diskdelta
import subprocess
import time


class Technique(Enum):
    LACH = "lach"
    XZ = "xz"
    RSYNC = "rsync"


class Result:
    headers = [
        # test parameters
        "image_size",
        "initial_image_hash",
        "target_image_hash",
        "block_size",
        "compression_technique",
        # result data
        "compressed_message_size",
        "compression_ratio",
        "compression_time",
    ]

    def __init__(self, compressed_size_bits: int, compression_ratio: float, time_seconds: float):
        self.size_bits = compressed_size_bits
        self.ratio = compression_ratio
        self.time = time_seconds

    def __str__(self):
        return f"{self.size_bits}, {self.ratio}, {self.time}"

    def __repr__(self):
        return self.__str__()


class Test:
    def __init__(self, initial_image_path: str, target_image_path: str, output_folder_path: str, technique: Technique, block_size: int):
        self.initial_image_path = initial_image_path
        self.target_image_path = target_image_path
        self.output_folder_path = output_folder_path
        self.technique = technique
        self.block_size = block_size

    def run(self) -> Result:
        try:
            match self.technique:
                case Technique.LACH:
                    output_path = os.path.join(
                        self.output_folder_path,
                        f"LACH_{os.path.basename(self.initial_image_path)}_{os.path.basename(self.target_image_path)}_{self.block_size}"
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
                    disk_delta.write_message_to_file(output_path)

                    time_end = time.perf_counter()

                    compressed_size = os.path.getsize(output_path)
                    compression_ratio = os.path.getsize(self.target_image_path) / compressed_size
                    compression_time = time_end - time_start

                    return Result(compressed_size, compression_ratio, compression_time)
                case Technique.XZ:
                    output_path = os.path.join(
                        self.output_folder_path,
                        f"XZ_{os.path.basename(self.target_image_path)}_{self.block_size}"
                    )

                    time_start = time.perf_counter()

                    subprocess.run(
                        ["xz", "-k", "-z", "-9", "-e", "-c", "-T0", f"-B{self.block_size}", self.target_image_path],
                        stdout=open(output_path, 'wb'),
                        check=True
                    )

                    time_end = time.perf_counter()

                    compressed_size = os.path.getsize(output_path)
                    compression_ratio = os.path.getsize(self.target_image_path) / compressed_size
                    compression_time = time_end - time_start

                    return Result(compressed_size, compression_ratio, compression_time)
                case Technique.RSYNC:
                    output_path = os.path.join(
                        self.output_folder_path,
                        f"RSYNC_{os.path.basename(self.initial_image_path)}_{os.path.basename(self.target_image_path)}_{self.block_size}"
                    )

                    time_start = time.perf_counter()

                    subprocess.run(
                        ["rsync", "--no-whole-file", f"--block-size={self.block_size}", self.initial_image_path, self.target_image_path, output_path],
                        check=True
                    )

                    time_end = time.perf_counter()

                    compressed_size = os.path.getsize(output_path)
                    compression_ratio = os.path.getsize(self.target_image_path) / compressed_size
                    compression_time = time_end - time_start

                    return Result(compressed_size, compression_ratio, compression_time)
                case _:
                    raise ValueError("Invalid technique")
        except Exception as e:
            print(f"Error during test run: {e}")
            raise

    def __str__(self):
        return (
            f"{os.path.getsize(self.initial_image_path)}, "
            f"{os.path.getsize(self.target_image_path)}, "
            f"{self.block_size}, "
            f"{self.technique}"
        )

    def __repr__(self):
        return self.__str__()


class ResultsWriter:
    def __init__(self, file_path):
        # Check file is csv that follows the format
        self.file_path = file_path
        self.file = open(file_path, "a+")
        if self.file.tell() == 0:
            self.init_headers()
        else:
            self.check_headers()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.file.close()

    def init_headers(self):
        self.file.write(",".join(Result.headers) + "\n")

    def check_headers(self):
        self.file.seek(0)
        self.headers = self.file.readline().strip().split(",")
        if self.headers != Result.headers:
            raise ValueError("Invalid headers. Please provide a valid or empty csv file")
        self.file.seek(0, 2)

    def write(self, test: Test, result: Result):
        self.file.write(str(test) + ", " + str(result) + "\n")


def main():
    # Create test cases
    input_path_couples = [
        ("input/empty_1GB.img", "input/empty_1GB.img"),
        ("input/empty_1GB.img", "input/empty_1GB_ext4.img"),
        ("input/empty_1GB_ext4.img", "input/initial_empty.img"),
        # ("input/initial_empty.img", "input/run_1.img"),
        # ("input/run_1.img", "input/run_2.img"),
        # ("input/run_2.img", "input/run_3.img"),
        # ("input/run_3.img", "input/run_4.img"),
        # ("input/run_4.img", "input/run_5.img"),
    ]

    # max_exponent = 16
    # block_sizes = [2**i for i in range(max_exponent + 1)]
    image_size = os.path.getsize("input/initial_empty.img")
    # block_sizes += [image_size, math.sqrt(image_size)]

    block_sizes = [1, image_size, math.sqrt(image_size)]
    techniques = [Technique.LACH, Technique.XZ, Technique.RSYNC]
    
    print("Running tests...")
    for input_paths, technique, block_size in itertools.product(input_path_couples, techniques, block_sizes):
        test = Test(input_paths[0], input_paths[1], "output/images/", technique, block_size)
        print("Test: " + str(test), end=" - ")
        with ResultsWriter("output/test_results.csv") as f:
            result = test.run()
            print("Result: " + str(result))
            f.write(test, result)
    print("Tests finished")



if __name__ == "__main__":
    main()