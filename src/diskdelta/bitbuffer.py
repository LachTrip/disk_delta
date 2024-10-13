import builtins
import os
from typing import cast
from bitarray import bitarray


class BitReader:
    def __init__(self, file_path):
        self.file = builtins.open(file_path, "rb")
        self.buffer_size = 1024 * 1024  # bytes -> Mib
        self.buffer = self.file.read(self.buffer_size)
        self.buffer_index = 0
        self.buffer_bit_index = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()

    def read(self, num_bits) -> bitarray | None:
        """
        Read the given number of bits from the file.
        """
        bits = bitarray()

        # read remaining bits from the current byte
        if self.buffer_bit_index > 0:
            current_byte = self.buffer[self.buffer_index]
            current_bits = bitarray()
            current_bits.frombytes(current_byte.to_bytes())

            if num_bits < len(current_bits) - self.buffer_bit_index:
                bits = current_bits[
                    self.buffer_bit_index : self.buffer_bit_index + num_bits
                ]
                self.buffer_bit_index += num_bits
                return bits

            bits = current_bits[self.buffer_bit_index :]
            num_bits -= len(bits)
            self.buffer_bit_index = 0
            self.buffer_index += 1

        # read full bytes
        bytes_to_read = num_bits // 8
        num_bits -= bytes_to_read * 8

        # read buffer_size chunks at a time
        while self.buffer_index + bytes_to_read > len(self.buffer):
            new_bits = bitarray()
            new_bits.frombytes(self.buffer[self.buffer_index :])
            bits.extend(new_bits)
            bytes_to_read -= (len(self.buffer) - self.buffer_index)
            
            self.buffer = self.file.read(self.buffer_size)
            self.buffer_index = 0
            if len(self.buffer) == 0:
                return None

        # read rest of needed bytes from buffer
        if bytes_to_read > 0:
            new_bits = bitarray()
            new_bits.frombytes(
                self.buffer[self.buffer_index : self.buffer_index + bytes_to_read]
            )
            bits.extend(new_bits)
            self.buffer_index += bytes_to_read
        
        # ensure buffer is not empty
        if self.buffer_index == len(self.buffer):
            self.buffer = self.file.read(self.buffer_size)
            self.buffer_index = 0
            if num_bits > 0 and len(self.buffer) == 0:
                raise ValueError("Not enough bits to read")

        # read rest of needed bits from next byte in buffer
        if num_bits > 0:
            current_byte = self.buffer[self.buffer_index]
            current_bits = bitarray()
            current_bits.frombytes(current_byte.to_bytes())
            bits.extend(current_bits[:num_bits])
            self.buffer_bit_index = num_bits

        return bits


class BitWriter:
    def __init__(self, file_path):
        directory = os.path.dirname(file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
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
