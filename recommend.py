#!/usr/bin/env python

import sys
from matchmaker.database import *

def main(argv):
    db = Database('minidata')
    if 'stats' in argv:
        db.summary()

if __name__ == '__main__':
    sys.exit(main(sys.argv))
