from bitarray import bitarray
import struct


class BaseStorage(object):
    def convert_key_to_bytes(self, key):
        return key.encode("utf-8")

    def __setitem__(self, key, val):
        if not isinstance(key, bytes):
            key = self.convert_key_to_bytes(key)
        self.storage[key] = val

    def __getitem__(self, key):
        if not isinstance(key, bytes):
            key = self.convert_key_to_bytes(key)
        return self.storage[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def convert_to_integer_key(self, key):
        return str(key) + ":int"

    def convert_to_string_key(self, key):
        return str(key) + ":string"

    def convert_to_bitarray_key(self, key):
        return str(key) + ":bitarray"

    def convert_to_bitarray_len_key(self, key):
        return str(key) + "_length"

    def int_to_bytes(self, value):
        return struct.pack("Q", int(value))

    def bytes_to_int(self, value):
        return struct.unpack("Q", value)[0]

    def batch_set(self, keys, values):
        for k, v in zip(keys, values):
            self[k] = v

    def batch_get(self, keys):
        return [self[k] for k in keys]

    def set_integer(self, key, value):
        key = self.convert_to_integer_key(key)
        self[key] = self.int_to_bytes(value)

    def get_integer(self, key):
        key = self.convert_to_integer_key(key)
        return self.bytes_to_int(self[key])

    def set_integers(self, keys, values):
        _keys = [
            self.convert_key_to_bytes(self.convert_to_integer_key(key)) for key in keys
        ]
        self.batch_set(_keys, [self.int_to_bytes(v) for v in values])

    def get_integers(self, keys):
        _keys = [
            self.convert_key_to_bytes(self.convert_to_integer_key(key)) for key in keys
        ]
        return [self.bytes_to_int(b) for b in self.batch_get(_keys)]

    def set_string(self, key, value):
        assert isinstance(value, str)
        key = self.convert_to_string_key(key)
        self[key] = value.encode("utf-8")

    def get_string(self, key):
        key = self.convert_to_string_key(key)
        return self[key].decode("utf-8")

    def set_bitarray_length(self, key, value):
        assert isinstance(value, int)
        lkey = self.convert_to_bitarray_len_key(key)
        self.set_integer(lkey, value)

    def get_bitarray_length(self, key):
        lkey = self.convert_to_bitarray_len_key(key)
        return self.get_integer(lkey)

    def set_bitarray(self, key, value):
        assert isinstance(value, bitarray)
        _key = self.convert_to_bitarray_key(key)
        self[_key] = value.tobytes()
        self.set_bitarray_length(key, len(value))

    def set_bitarrays(self, keys, values):
        _keys = [
            self.convert_key_to_bytes(self.convert_to_bitarray_key(key)) for key in keys
        ]
        self.batch_set(_keys, [v.tobytes() for v in values])

        _lkeys = [self.convert_to_bitarray_len_key(key) for key in keys]
        self.set_integers(_lkeys, [len(v) for v in values])

    def get_bitarray(self, key):
        _key = self.convert_to_bitarray_key(key)
        value = bitarray()
        value.frombytes(self[_key])
        return value[: self.get_bitarray_length(key)]

    def get_bitarrays(self, keys):
        # Takes advantage of batching in storage engine if available
        _keys = [
            self.convert_key_to_bytes(self.convert_to_bitarray_key(key)) for key in keys
        ]
        _lkeys = [self.convert_to_bitarray_len_key(key) for key in keys]
        lengths = self.get_integers(_lkeys)
        results = []
        for result, length in zip(self.batch_get(_keys), lengths):
            ba = bitarray()
            ba.frombytes(result)
            results.append(ba[:length])
        return results

    def set_bit(self, key, pos, bit):
        length = self.get_bitarray_length(key)
        ba = self.get_bitarray(key)
        try:
            ba[pos] = bit
        except IndexError:
            ba.append(bit)  ## Assuming setbit is always in the next available column
        self.set_bitarray(key, ba)
        if len(ba) > length:
            self.set_bitarray_length(key, len(ba))

    def set_bits(self, keys, positions, bits):
        # Takes advantage of batching in storage engine if available
        for key, pos, bit in zip(keys, positions, bits):
            self.set_bit(key, pos, bit)

    def get_bit(self, key, pos):
        return self.get_bitarray(key)[pos]

    def get_bits(self, keys, positions):
        # Takes advantage of batching in storage engine if available
        for key, pos in zip(keys, positions):
            yield self.get_bit(key, pos)

    def delete_all(self):
        raise NotImplementedError("Implemented in subclass")


class BitMatrix(object):

    ### Doesn't know the concept of a kmer
    def __init__(self, number_of_rows):
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
                        list(range(self.number_of_rows)),
                        [column_index] * self.number_of_rows,
                    )
                ]
            )
        )

    def get_columns(self, column_indexes):
        for column_index in column_indexes:
            yield self.get_column(column_index)

    def insert_column(self, column_index, bitarray):
        ## This is very slow, as we index row-wise
        self.set_bits(
            list(range(len(bitarray))), [column_index] * len(bitarray), list(bitarray)
        )


class MetadataStorageMixin:

    # todo make property
    def bloomfilter():
        pass
