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
            return self.__get_integer(self.colour_count_key)
            # we could distinguish between the number of samples,
            # and the number of colours,
            # but it adds unuseful complexity
        except KeyError:
            return 0

    def add_sample(self, sample_name):
        self.__validate_sample_name(sample_name)
        colour = self.num_samples
        self.__set_sample_colour(sample_name, colour)
        self.__set_colour_sample(colour, sample_name)
        return self.__increment_colour_count()

    def add_samples(self, sample_names):
        for sample_name in sample_names:
            self.add_sample(sample_name)

    def delete_sample(self, sample_name):
        ## Deleting samples just changes it's name to a reserved deleted string
        colour = self.sample_to_colour(sample_name)
        self.__set_colour_sample(colour, DELETION_SPECIAL_SAMPLE_NAME)
        self.__set_sample_colour(sample_name, -1)
        ## We don't decrement the count, as the number of colours is the same

    def sample_name_exists(self, sample_name):
        try:
            self.__get_integer(sample_name)
            return True
        except KeyError:
            return False

    def sample_to_colour(self, sample_name):
        try:
            colour = self.__get_integer(sample_name)
            if colour < 0:
                return None
            else:
                return colour
        except KeyError:
            return None

    def colour_to_sample(self, colour):
        ## Ignores deleted samples
        sample_name = self.__get_string(colour)
        if sample_name == DELETION_SPECIAL_SAMPLE_NAME:
            sample_name = "DELETED_SAMPLE"
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

    def __set_integer(self, key, value):
        __key = self.__add_key_prefix(key)
        self.storage.set_integer(__key, value)

    def __get_integer(self, key):
        __key = self.__add_key_prefix(key)
        return self.storage.get_integer(__key)

    def __set_string(self, key, value):
        __key = self.__add_key_prefix(key)
        self.storage.set_string(__key, value)

    def __get_string(self, key):
        __key = self.__add_key_prefix(key)
        return self.storage.get_string(__key)

    def __incr(self, key):
        __key = self.__add_key_prefix(key)
        return self.storage.incr(__key)

    def __set_sample_colour(self, sample_name, colour):
        self.__set_integer(sample_name, colour)

    def __set_colour_sample(self, colour, sample_name):
        self.__set_string(colour, sample_name)

    def __increment_colour_count(self):
        return self.__incr(self.colour_count_key)

    def __add_key_prefix(self, key):
        return ":".join(["metadata", str(key)])

    def __validate_sample_name(self, sample_name):
        if sample_name == DELETION_SPECIAL_SAMPLE_NAME:
            raise ValueError(
                "You can't call a sample %s" % DELETION_SPECIAL_SAMPLE_NAME
            )
        if self.sample_name_exists(sample_name):
            raise ValueError("You can't insert two samples with the same name")
