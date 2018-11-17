from bitarray import bitarray
import struct


class BaseStorage(object):
    def convert_key(self, key):
        return key.encode("utf-8")

    def __setitem__(self, key, val):
        key = self.convert_key(key)
        self.storage[key] = val

    def __getitem__(self, key):
        key = self.convert_key(key)
        return self.storage[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def convert_to_integer_key(self, key):
        return key + ":int"

    def convert_to_string_key(self, key):
        return key + ":string"

    def convert_to_bitarray_key(self, key):
        return key + ":bitarray"

    def convert_to_bitarray_len_key(self, key):
        return key + ":length_of_bitarray"

    def set_integer(self, key, value):
        key = self.convert_to_integer_key(key)
        self[key] = struct.pack("Q", int(value))

    def get_integer(self, key):
        key = self.convert_to_integer_key(key)
        return struct.unpack("Q", self[key])[0]

    def set_string(self, key, value):
        assert isinstance(value, str)
        key = self.convert_to_string_key(key)
        self[key] = value.encode("utf-8")

    def get_string(self, key):
        key = self.convert_to_string_key(key)
        return self[key].decode("utf-8")

    def set_bitarray_length(self, key, value):
        assert isinstance(value, bitarray)
        lkey = self.convert_to_bitarray_len_key(key)
        self.set_integer(lkey, len(value))

    def get_bitarray_length(self, key):
        lkey = self.convert_to_bitarray_len_key(key)
        return self.get_integer(lkey)

    def set_bitarray(self, key, value):
        assert isinstance(value, bitarray)
        _key = self.convert_to_bitarray_key(key)
        self[_key] = value.tobytes()
        self.set_bitarray_length(key, value)

    def set_bitarray(self, keys, values):
        # Takes advantage of batching in storage engine if available
        raise NotImplementedError("Implemented in subclass")

    def get_bitarray(self, key):
        _key = self.convert_to_bitarray_key(key)
        value = bitarray()
        value.frombytes(self[_key])
        return value[: self.get_bitarray_length(key)]

    def get_bitarrays(self, keys):
        # Takes advantage of batching in storage engine if available
        raise NotImplementedError("Implemented in subclass")

    def set_bit(self, key, pos, bit):
        ba = self.get_bitarray(key)
        ba[pos] = bit
        self.set_bitarray(key, ba)

    def set_bits(self, keys, positions, bits):
        # Takes advantage of batching in storage engine if available
        raise NotImplementedError("Implemented in subclass")

    def get_bit(self, key, pos):
        return self.get_bitarray(key)[pos]

    def get_bits(self, keys, positions):
        # Takes advantage of batching in storage engine if available
        raise NotImplementedError("Implemented in subclass")

    def delete_all(self):
        raise NotImplementedError("Implemented in subclass")


class BitMatrix:

    ### Doesn't know the concept of a kmer
    def __init(self, number_of_rows):
        self.number_of_rows = number_of_rows

    def get_row(self, row_index):
        return self.get_bitarray(row_index)

    def get_rows(self, row_indexes):
        # Takes advantage of batching in storage engine if available
        return self.get_bitarrays(row_indexes)

    def set_row(self, row_index, bitarray):
        return self.set_bitarray(row_index, bitarray)

    def set_rows(self, row_indexes, bitarrays):
        # Takes advantage of batching in storage engine if available
        return self.set_bitarrays(row_indexes, bitarrays)

    def get_column(self, column_index):
        ## This is very slow, as we index row-wise. Need to know the number of rows, so must be done elsewhere
        return bitarray(
            "".join(
                [
                    str(int(i))
                    for i in self.get_bits(
                        list(range(row_indexes)), [column_index] * len(row_indexes)
                    )
                ]
            )
        )

    def insert_column(self, column_index, bitarray):
        ## This is very slow, as we index row-wise
        self.set_bits(
            list(range(row_indexes)), list(range(len(bitarray))), list(bitarray)
        )


class MetadataStorageMixin:

    # todo make property
    def bloomfilter():
        pass
