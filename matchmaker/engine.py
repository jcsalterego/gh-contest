#!/usr/bin/env python

import sys
import random
from math import log
from collections import defaultdict
from pprint import pprint

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
            self.recommended[u] = self.user_process(u)

    def user_process(self, user):
        """Returns ten recommendations
        """
        db = self.database
        r_info = db.r_info
        r_langs = db.r_langs
        lang_by_r = db.lang_by_r
        u_watching = db.u_watching
        forks_of_r = db.forks_of_r
        parent_of_r = db.parent_of_r
        gparent_of_r = db.parent_of_r
        u_authoring = db.u_authoring

        scores = defaultdict(int)
        for r in u_watching[user]:
            # loop through all watched repositories
            
            # find forks
            for r1 in forks_of_r[r]:
                scores[r1] += 2 / log(2 + len(u_watching[r1]))

            # find parents and siblings
            if parent_of_r[r] > 0:
                parent = parent_of_r[r]
                scores[parent] += 2
                for r1 in forks_of_r[parent]:
                    scores[r1] += 2 / log(2 + len(u_watching[r1]))

                    # find others by author of parent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += 1 / log(2 + len(u_watching[r2]))

            # find grandparents and uncles/aunts
            if gparent_of_r[r] > 0:
                gparent = gparent_of_r[r]
                scores[gparent] += 2
                for r1 in forks_of_r[gparent]:
                    scores[r1] += 2 / log(2 + len(u_watching[r1]))

                    # find others by author of gparent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += 1 / log(2 + len(u_watching[r2]))

            # find others by author
            if r in r_info:
                author = r_info[r][0]
                for r1 in u_authoring[author]:
                    scores[r1] += 2 / log(2 + len(u_watching[r1]))

            # find similar projects by language
            for lang, lnlog in r_langs[r]:
                num = 0
                for lnlog2, r1 in lang_by_r[lang]:
                    if lnlog == lnlog2:
                        scores[r1] += 1 / log(2 + len(u_watching[r1]))
                        num += 1
                    if num == 10:
                        break

        # cleanup
        for r in u_watching[user] + [0]:
            if r in scores:
                del scores[r]

        scores = [(lambda (x,y): (y,x))(score) for score in scores.items()]
        scores.sort(reverse=True)
        final = [s[1] for s in scores[:10]]
        if 'production' not in sys.argv:
            pprint(scores[:10])
        return final

    def results(self):
        lines = []
        for u in sorted(self.recommended.keys()):
            r_list = self.recommended[u]
            lines.append(':'.join((str(u),
                                   ','.join([str(v) for v in r_list]))))
        return "\n".join(lines)
