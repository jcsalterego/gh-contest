#!/usr/bin/env python

from pprint import pprint
from matchmaker.database import *
import sys

def main(argv):
    if len(argv) == 1:
        return

    line = argv[1]
    if line[0] in '+-':
        line = line[1:]

    user, repos = line.split(":")
    user = int(user)
    repos = [int(r) for r in repos.split(",")]

    print("Loading database...")
    db = Database("data")

    print("original watchlist")
    watching = sorted(db.u_watching[user])
    for r in watching:
        print "%6d" % r,
        if r in db.r_info:
            print("%18s - %20s - %10s"
                  % tuple([x[:20] for x in db.r_info[r]]))
        else:
            print("")

    print("")
    print("new additions")
    watching = sorted(repos)
    for r in watching:
        print "%6d" % r,
        if r in db.r_info:
            print("%18s - %20s - %10s"
                  % tuple([x[:20] for x in db.r_info[r]]))
        else:
            print("")

if __name__ == '__main__':
    sys.exit(main(sys.argv))
