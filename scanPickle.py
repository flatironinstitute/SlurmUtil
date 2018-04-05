
from _pickle import loads as PL
from zlib import decompress as ZD

import sys

z8 = '00000000'

pFile = open(sys.argv[1])

while 1:
    loc = pFile.tell()
    b = pFile.read(40)
    if not b: break

    if not b.startswith(z8):
        print('Got lost at', loc)
        pFile.seek(loc)
        while 1:
            tloc = pFile.tell()
            b = pFile.read(1000000)
            if not b: sys.exit(0)
            x = b.find(z8)
            if x != -1:
                pFile.seek(tloc+x)
                break
        print('Skipping', pFile.tell() - loc)
        continue
    
    print('%20d'%int(b[:20]) + '\t' + '%20d'%int(b[20:]) + '\t' + '%20d'%loc)
    pFile.seek(int(b[20:]), 1)
