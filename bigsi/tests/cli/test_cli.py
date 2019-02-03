import glob
import hug
import os
import string
import random
import pytest
import json
from bitarray import bitarray

from bigsi import BIGSI
import bigsi.__main__

# CONFIG_FILES = ["bigsi/tests/configs/redis.yaml"]
CONFIG_FILES = []

try:
    import rocksdb
except ModuleNotFoundError:
    pass
else:
    CONFIG_FILES.append("bigsi/tests/configs/rocks.yaml")
try:
    import bsddb3
except ModuleNotFoundError:
    pass
else:
    CONFIG_FILES.append("bigsi/tests/configs/berkeleydb.yaml")


def test_bloom_cmd():
    for config_file in CONFIG_FILES:
        f = "/tmp/test_kmers.bloom"
        response = hug.test.post(
            bigsi.__main__,
            "bloom",
            {
                "config": config_file,
                "ctx": "bigsi/tests/data/test_kmers.ctx",
                "outfile": f,
            },
        )
        a = bitarray()
        with open("/tmp/test_kmers.bloom/test_kmers.bloom", "rb") as inf:
            a.fromfile(inf)
        assert sum(a) > 0
        os.remove("/tmp/test_kmers.bloom/test_kmers.bloom")


def test_build_cmd():
    for config_file in CONFIG_FILES:
        N = 3
        bloomfilter_filepaths = ["bigsi/tests/data/test_kmers.bloom"] * N
        samples = []
        for i in range(N):
            samples.append(
                "".join(
                    random.choice(string.ascii_uppercase + string.digits)
                    for _ in range(6)
                )
            )
        response = hug.test.post(
            bigsi.__main__,
            "build",
            {
                "config": config_file,
                "bloomfilters": bloomfilter_filepaths,
                "samples": samples,
            },
        )
        # TODO fix below
        seq = "GATCGTTTGCGGCCACAGTTGCCAGAGATGA"
        response = hug.test.post(
            bigsi.__main__, "search", {"config": config_file, "seq": seq}
        )

        assert response.data
        # assert "score" in list(response.data.get(seq).get('results').values())[0]
        seq = "GATCGTTTGCGGCCACAGTTGCCAGAGATGAAAG"
        response = hug.test.post(
            bigsi.__main__,
            "search",
            {"config": config_file, "seq": seq, "threshold": 0.1},
        )
        assert len(json.loads(response.data)) == 4
        response = hug.test.delete(bigsi.__main__, "", {"config": config_file})


def test_insert_search_cmd():
    for config_file in CONFIG_FILES:
        try:
            response = hug.test.delete(bigsi.__main__, "", {"config": config_file})
        except:
            pass

        N = 3
        bloomfilter_filepaths = ["bigsi/tests/data/test_kmers.bloom"] * N
        samples = []
        for i in range(N):
            samples.append(
                "".join(
                    random.choice(string.ascii_uppercase + string.digits)
                    for _ in range(6)
                )
            )
        response = hug.test.post(
            bigsi.__main__,
            "build",
            {
                "config": config_file,
                "bloomfilters": bloomfilter_filepaths,
                "samples": samples,
            },
        )

        # Returns a Response object
        response = hug.test.post(
            bigsi.__main__,
            "insert",
            {
                "config": config_file,
                "bloomfilter": "bigsi/tests/data/test_kmers.bloom",
                "sample": "s3",
            },
        )
        assert response.data.get("result") == "success"
        seq = "GATCGTTTGCGGCCACAGTTGCCAGAGATGA"
        response = hug.test.post(
            bigsi.__main__, "search", {"config": config_file, "seq": seq}
        )

        assert "s3" in [r["sample_name"] for r in json.loads(response.data)["results"]]
        response = hug.test.delete(bigsi.__main__, "", {"config": config_file})


@pytest.mark.skip(reason="TODO, fix test to work on single config")
def test_merge_search_cmd():
    for config_file in CONFIG_FILES:
        try:
            response = hug.test.delete(bigsi.__main__, "", {"config": config_file})
        except:
            pass

        N = 3
        bloomfilter_filepaths = ["bigsi/tests/data/test_kmers.bloom"] * N
        samples = []
        for i in range(N):
            samples.append(
                "".join(
                    random.choice(string.ascii_uppercase + string.digits)
                    for _ in range(6)
                )
            )
        response = hug.test.post(
            bigsi.__main__,
            "build",
            {
                "config": config_file,
                "bloomfilters": bloomfilter_filepaths,
                "samples": samples,
            },
        )

    # Returns a Response object
    response = hug.test.post(
        bigsi.__main__,
        "merge",
        {"config": CONFIG_FILES[0], "merge_config": CONFIG_FILES[1]},
    )
    assert response.data.get("result")
    seq = "GATCGTTTGCGGCCACAGTTGCCAGAGATGA"
    response = hug.test.post(
        bigsi.__main__, "search", {"config": CONFIG_FILES[0], "seq": seq}
    )
    assert len([r["sample_name"] for r in response.data]) == 6
