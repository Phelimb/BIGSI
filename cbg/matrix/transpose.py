from bitarray import bitarray
import copy
import logging
logger = logging.getLogger(__name__)
from cbg.utils import DEFAULT_LOGGING_LEVEL
logger.setLevel(DEFAULT_LOGGING_LEVEL)
import numpy as np

# def transpose(bitarrays):
#         # Takes a list of bitarrays and returns the transpose as a list of
#         # bitarrays
#     x = len(bitarrays)
#     y = bitarrays[0].length()
#     logger.info("BFM dims %i %i" % (x, y))

#     tbitarrays = []
#     for ii in range(y):
#         tbitarrays.append(bitarray('0'*x))
#     for i in range(x):
#         for j in range(y):
#             tbitarrays[j][i] = bitarrays[i][j]
#     return tbitarrays


def transpose(bitarrays):
    # Takes a list of bitarrays and returns the transpose as a list of
    # bitarrays
    X = np.array(bitarrays).transpose()
    tbitarrays=[bitarray(i.tolist()) for i in X]  
    return tbitarrays
