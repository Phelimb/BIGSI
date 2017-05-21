#! /usr/bin/env python
from __future__ import print_function
import shutil
import logging
logger = logging.getLogger(__name__)
from cbg.storage.base import BerkeleyDBStorage


def rowjoin(partitioned_data, out_db, N=25000000):
    N = int(N)
    db_out = BerkeleyDBStorage(config={'filename': out_db})
    for x in ["colour_to_sample_lookup", "sample_to_colour_lookup", "metadata"]:
        shutil.copy("".join([partitioned_data, "_0", x]), "".join([out_db, x]))
    batch = 0
    logger.info("Loading %s" % "".join([partitioned_data, "_", str(batch)]))
    db = BerkeleyDBStorage(
        config={'filename': "".join([partitioned_data, "_", str(batch)])})
    for i in range(N):
        if i % 10000 == 0 and not i == 0:
            logger.info("%i of %i %f%% " % (i, N, 100*i/N))
            db.storage.close()
            batch += 1
            db = BerkeleyDBStorage(
                config={'filename': "".join([partitioned_data, "_", str(batch)])})
        db_out[i] = db[i]
    return {'graph': out_db}
