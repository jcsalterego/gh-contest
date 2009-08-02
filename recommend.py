#!/usr/bin/env python

import sys
from matchmaker.database import *
from matchmaker.engine import *

def main(argv):
    if 'production' in argv:
        return production(argv)
    else:
        return testing(argv)

def production(argv):
    db = Database('data')
    e = Engine(db)
    results = e.results()

    resf = open('results.txt', 'w')
    resf.write(results)
    resf.close()
    return 0

def testing(argv):
    db = Database('minidata')
    if 'stats' in argv:
        db.summary()
    e = Engine(db)
    print(e.results())
    return 0

if __name__ == '__main__':
    sys.exit(main(sys.argv))
