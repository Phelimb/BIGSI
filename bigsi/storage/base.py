from bitarray import bitarray
import struct
import gc


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

    def convert_integer_batch_keys(self, keys):
        return [
            self.convert_key_to_bytes(self.convert_to_integer_key(key)) for key in keys
        ]

    def convert_bitarray_batch_keys(self, keys):
        return [
            self.convert_key_to_bytes(self.convert_to_bitarray_key(key)) for key in keys
        ]

    def convert_bitarray_length_batch_keys(self, keys):
        return [self.convert_to_bitarray_len_key(key) for key in keys]

    def int_to_bytes(self, value):
        return str(value).encode("utf-8")

    def bytes_to_int(self, value):
        return int(value.decode("utf-8"))

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
        _keys = self.convert_integer_batch_keys(keys)
        self.batch_set(_keys, [self.int_to_bytes(v) for v in values])

    def get_integers(self, keys):
        _keys = self.convert_integer_batch_keys(keys)
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
        _keys = self.convert_bitarray_batch_keys(keys)
        self.batch_set(_keys, [v.tobytes() for v in values])

        _lkeys = self.convert_bitarray_length_batch_keys(keys)
        self.set_integers(_lkeys, [len(v) for v in values])

    def load_bitarray(self, _bytes):
        ba = bitarray()
        ba.frombytes(_bytes)
        return ba

    def get_bitarray(self, key):
        _key = self.convert_to_bitarray_key(key)
        value = self.load_bitarray(self[_key])[: self.get_bitarray_length(key)]
        return value

    def get_bitarrays(self, keys):
        # Takes advantage of batching in storage engine if available
        _keys = self.convert_bitarray_batch_keys(keys)
        _lkeys = self.convert_bitarray_length_batch_keys(keys)
        lengths = self.get_integers(_lkeys)
        results = []
        for result, length in zip(self.batch_get(_keys), lengths):
            ba = self.load_bitarray(result)[:length]
            results.append(ba)
        return results

    def set_bit(self, key, pos, bit):
        ba = self.get_bitarray(key)
        try:
            ba[pos] = bit
        except IndexError:
            ba.append(bit)  ## Assuming setbit is always in the next available column
        self.set_bitarray(key, ba)

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

    def incr(self, key):
        try:
            i = self.get_integer(key)
            i += 1
            self.set_integer(key, i)
            return i
        except KeyError:
            i = 1
            self.set_integer(key, i)
            return i

    def sync(self):
        pass

    def close(self):
        del self.storage
        gc.collect()
