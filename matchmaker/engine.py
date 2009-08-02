#!/usr/bin/env python

import sys
import random
from collections import defaultdict

class Engine:
    def __init__(self, database):
        """Constructor
        """
        self.database = database
        self.recommended = defaultdict(list)
        self.process()

    def process(self):
        db = self.database

        r_list = db.r_info.keys()
        maxrep = len(r_list) / 10 / 2

        i = 0
        random.shuffle(r_list)
        for u in db.test_u:
            i += 1
            if i % maxrep == 0:
                random.shuffle(r_list)
            start = i % maxrep * 10
            end = start + 10
            self.recommended[u] = [str(x) for x in r_list[start:end]]

    def results(self):
        lines = []
        for u in sorted(self.recommended.keys()):
            r_list = self.recommended[u]
            lines.append(':'.join((str(u), ','.join(r_list))))
        return "\n".join(lines)
