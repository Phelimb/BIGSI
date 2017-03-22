from bitarray import bitarray
import copy


def transpose(bitarrays):
        # Takes a list of bitarrays and returns the transpose as a list of
        # bitarrays
    x = len(bitarrays)
    y = len(bitarrays[0])
    tbitarrays = []
    for ii in range(y):
        tbitarrays.append(bitarray('0'*x))
    for i in range(x):
        for j in range(y):
            tbitarrays[j][i] = bitarrays[i][j]
    return tbitarrays
