DELETION_SPECIAL_SAMPLE_NAME = "D3L3T3D"


class SampleMetadata:
    def __init__(self, storage):
        self.storage = storage

    @property
    def colour_count_key(self):
        return "colour_count"

    @property
    def num_samples(self):
        try:
            return self._get_integer(self.colour_count_key)
            # we could distinguish between the number of samples,
            # and the number of colours,
            # but it adds unuseful complexity
        except KeyError:
            return 0

    def add_sample(self, sample_name):
        self._validate_sample_name(sample_name)
        colour = self.num_samples
        self._set_sample_colour(sample_name, colour)
        self._set_colour_sample(colour, sample_name)
        return self._increment_colour_count()

    def add_samples(self, sample_names):
        for sample_name in sample_names:
            self.add_sample(sample_name)

    def delete_sample(self, sample_name):
        ## Deleting samples just changes it's name to a reserved deleted string
        colour = self.sample_to_colour(sample_name)
        self._set_colour_sample(colour, DELETION_SPECIAL_SAMPLE_NAME)
        self._set_sample_colour(sample_name, -1)
        ## We don't decrement the count, as the number of colours is the same

    def sample_name_exists(self, sample_name):
        try:
            self._get_integer(sample_name)
            return True
        except KeyError:
            return False

    def sample_to_colour(self, sample_name):
        try:
            colour = self._get_integer(sample_name)
            if colour < 0:
                return None
            else:
                return colour
        except KeyError:
            return None

    def colour_to_sample(self, colour):
        ## Ignores deleted samples
        sample_name = self._get_string(colour)
        return sample_name

    def samples_to_colours(self, sample_names):
        return {
            s: self.sample_to_colour(s)
            for s in sample_names
            if self.sample_to_colour(s) is not None
        }

    def colours_to_samples(self, colours):
        return {
            c: self.colour_to_sample(c) for c in colours if self.colour_to_sample(c)
        }

    def merge_metadata(self, sm):
        for c in range(sm.num_samples):
            sample = sm.colour_to_sample(c)
            try:
                self.add_sample(sample)
            except ValueError:
                self.add_sample(sample + "_duplicate_in_merge")

    def _set_integer(self, key, value):
        _key = self._add_key_prefix(key)
        self.storage.set_integer(_key, value)

    def _get_integer(self, key):
        _key = self._add_key_prefix(key)
        return self.storage.get_integer(_key)

    def _set_string(self, key, value):
        _key = self._add_key_prefix(key)
        self.storage.set_string(_key, value)

    def _get_string(self, key):
        _key = self._add_key_prefix(key)
        return self.storage.get_string(_key)

    def _incr(self, key):
        _key = self._add_key_prefix(key)
        return self.storage.incr(_key)

    def _set_sample_colour(self, sample_name, colour):
        self._set_integer(sample_name, colour)

    def _set_colour_sample(self, colour, sample_name):
        self._set_string(colour, sample_name)

    def _increment_colour_count(self):
        return self._incr(self.colour_count_key)

    def _add_key_prefix(self, key):
        return ":".join(["metadata", str(key)])

    def _validate_sample_name(self, sample_name):
        if sample_name == DELETION_SPECIAL_SAMPLE_NAME:
            raise ValueError(
                "You can't call a sample %s" % DELETION_SPECIAL_SAMPLE_NAME
            )
        if self.sample_name_exists(sample_name):
            raise ValueError("You can't insert two samples with the same name")
