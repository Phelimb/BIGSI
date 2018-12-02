from hypothesis import given
from hypothesis import example
import hypothesis.strategies as st
import numpy as np
from bigsi.matrix import transpose
from bitarray import bitarray


def create_bitarrays(list_of_boolean_lists):
    out = []
    for boolean_list in list_of_boolean_lists:
        ba = bitarray()
        ba.extend(boolean_list)
        out.append(ba)
    return out


SIZE = 10


@given(booleans=st.lists(st.lists(st.booleans(), min_size=SIZE, max_size=SIZE), min_size=5, max_size=10))
def test_transpose(booleans):
    for lowmem in [True, False]:
        npmatrix = np.array(booleans).transpose()
        bitarrays = create_bitarrays(booleans)
        tbitarrays = list(transpose(bitarrays, lowmem))
        for j in range(len(booleans)):
            for i in range(SIZE):
                assert npmatrix[i, j] == tbitarrays[i][j]
