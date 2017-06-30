"""
iter.py  - Iterate through the BloomFilterMatrix rows
"""

import sys
import bsddb3.db as db
import bitarray
def main():
    infile = sys.argv[1]

    in_db = db.DB()
    in_db.set_cachesize(4,0)
    in_db.open(infile, flags=db.DB_RDONLY)

    for i in range(25*10**6):
        key = str.encode(str(i))
        key = (i).to_bytes(4, byteorder='big')
        val=bitarray.bitarray()
        val.frombytes(in_db[key])
        print(i,val)
    in_db.close()

main()
