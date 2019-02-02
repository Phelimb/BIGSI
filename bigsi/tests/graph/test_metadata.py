from bigsi.graph.metadata import SampleMetadata
import pytest


from bigsi.tests.base import get_test_storages


def get_storages():
    return get_test_storages()


def test_add_sample_metadata():
    for storage in get_storages():
        storage.delete_all()
        sm = SampleMetadata(storage=storage)
        sample_name = "sample_name"
        colour = sm.add_sample(sample_name)
        assert sm.samples_to_colours([sample_name]) == {sample_name: 0}
        assert sm.colours_to_samples([0]) == {0: sample_name}
        assert sm.num_samples == 1
        assert sm.sample_name_exists(sample_name)
        assert not sm.sample_name_exists("sample_name2")

        sample_name = "sample_name2"
        colour = sm.add_sample(sample_name)
        assert sm.sample_to_colour(sample_name) == 1
        assert sm.colour_to_sample(1) == sample_name
        assert sm.num_samples == 2


def test_delete_sample():
    ## Add 2 samples
    for storage in get_storages():
        storage.delete_all()
        sm = SampleMetadata(storage=storage)
        sample_name1 = "sample_name"
        colour = sm.add_sample(sample_name1)
        sample_name2 = "sample_name2"
        colour = sm.add_sample(sample_name2)

    ## Ensure both samples are these
    assert sm.samples_to_colours([sample_name1, sample_name2]) == {
        sample_name1: 0,
        sample_name2: 1,
    }
    assert sm.colours_to_samples([0, 1]) == {0: sample_name1, 1: sample_name2}

    ## Delete one sample
    sm.delete_sample(sample_name1)

    ## Ensure only one sample is return
    assert sm.samples_to_colours([sample_name1, sample_name2]) == {sample_name2: 1}
    assert sm.colours_to_samples([0, 1]) == {0: "DELETED_SAMPLE", 1: sample_name2}


def test_unique_sample_names():
    for storage in get_storages():
        storage.delete_all()
        sm = SampleMetadata(storage=storage)
        sample_name = "sample_name"
        colour = sm.add_sample(sample_name)
        with pytest.raises(ValueError):
            sm.add_sample(sample_name)
