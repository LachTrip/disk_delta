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
        self.file.close()

    def read(self, num_bits) -> bitarray | None:
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

    def read_bit(self) -> bool | None:
        if self.buffer_index == 0:
            byte = self.file.read(1)
            if byte == b"":
                return None
            self.buffer = ord(byte)
            self.buffer_index = 8
        bit = (self.buffer >> 7) & 1
        self.buffer <<= 1
        self.buffer_index -= 1
        return bool(bit)


class BitWriter:
    def __init__(self, file_path):
        self.file = builtins.open(file_path, "wb")
        self.bit_index = 0
        self.current_byte = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.bit_index > 0:
            self.file.write(bytes([self.current_byte]))
        self.file.close()

    def write(self, bits: bitarray):
        """
        Write the given bits to the file.
        """

        # write bits to remaining bits the current byte
        if self.bit_index > 0:
            new_bits = bits[: 8 - self.bit_index]
            self.current_byte |= new_bits.tobytes()[0] >> self.bit_index
            self.bit_index += len(new_bits)
            if self.bit_index == 8:
                self.file.write(bytes([self.current_byte]))
                self.current_byte = 0
                self.bit_index = 0
            bits = bits[len(new_bits) :]

        # write full bytes
        bytes_to_write = len(bits) // 8
        self.file.write(bits[: bytes_to_write * 8].tobytes())
        bits = bits[bytes_to_write * 8 :]

        # write remaining bits to the last byte
        if len(bits) > 0:
            self.current_byte = bits.tobytes()[0]
            self.bit_index = len(bits)
        
        bit_index = 0


def open(file_path, mode) -> BitReader | BitWriter:
    if mode == "r":
        return BitReader(file_path)
    elif mode == "w":
        return BitWriter(file_path)
    else:
        raise ValueError(f"Invalid mode: {mode}")
