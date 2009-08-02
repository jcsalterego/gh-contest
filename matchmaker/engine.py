#!/usr/bin/env python

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
        for u in db.test_u:
            random.shuffle(r_list)
            self.recommended[u] = [str(x) for x in r_list[:10]]

    def results(self):
        lines = []
        for u, r_list in self.recommended.items():
            lines.append(':'.join((str(u), ','.join(r_list))))
        return "\n".join(lines)
