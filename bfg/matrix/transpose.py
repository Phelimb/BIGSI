from bitarray import bitarray
import copy
import logging
logger = logging.getLogger(__name__)
from bfg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)


def transpose(bitarrays):
        # Takes a list of bitarrays and returns the transpose as a list of
        # bitarrays
    x = len(bitarrays)
    y = len(bitarrays[0])
    logger.info("BFM dims %i %i" % (x, y))

    tbitarrays = []
    for ii in range(y):
        tbitarrays.append(bitarray('0'*x))
    for i in range(x):
        for j in range(y):
            tbitarrays[j][i] = bitarrays[i][j]
    return tbitarrays
