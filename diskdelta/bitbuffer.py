import builtins
from bitarray import bitarray


class BitReader:
    def __init__(self, file_path):
        self.file = builtins.open(file_path, "rb")
        self.buffer = 0
        self.buffer_index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def read(self, num_bits) -> bitarray:
        """
        Read the given number of bits from the file.
        """
        bits = bitarray()
        for _ in range(num_bits):
            bit = self.read_bit()
            if bit is None:
                return None
            bits.append(bit)
        return bits

    def read_bit(self) -> bool:
        if self.buffer_index == 0:
            byte = self.file.read(1)
            if byte == b'':
                return None
            self.buffer = ord(byte)
            self.buffer_index = 8
        bit = (self.buffer >> 7) & 1
        self.buffer <<= 1
        self.buffer_index -= 1
        return bit

    def close(self):
        self.file.close()


class BitWriter:
    def __init__(self, file_path):
        self.file = builtins.open(file_path, "wb")
        self.buffer = 0
        self.buffer_index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def write(self, bits: bitarray):
        """
        Write the given bits to the file.
        """
        for bit in bits:
            self.write_bit(bit)

    def write_bit(self, bit: bool):
        self.buffer |= bit << (7 - self.buffer_index)
        self.buffer_index += 1
        if self.buffer_index == 8:
            self.file.write(bytes([self.buffer]))
            self.buffer = 0
            self.buffer_index = 0

    def close(self):
        if self.buffer_index > 0:
            self.file.write(bytes([self.buffer]))
        self.file.close()


def open(file_path, mode):
    if mode == "r":
        return BitReader(file_path)
    elif mode == "w":
        return BitWriter(file_path)
    else:
        raise ValueError(f"Invalid mode: {mode}")
