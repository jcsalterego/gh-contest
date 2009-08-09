#!/usr/bin/env python

import sys
import random
from math import log
from collections import defaultdict
from pprint import pprint
from matchmaker import msg
from matchmaker.kmeans import *

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

        msg("Beginning recommendations")
        total = len(db.test_u)
        i = 0
        for u in db.test_u:
            self.recommended[u] = self.user_process(u)
            i += 1
            if i % 10 == 0:
                msg("[%3.2f%%] %d/%d processed"
                    % (float(i)/float(total)*100.0, i, total))
    
    def user_process(self, user):
        """Returns ten recommendations
        """
        db = self.database
        r_info = db.r_info
        r_name = db.r_name
        r_langs = db.r_langs
        r_lang_tuple = db.r_lang_tuple
        r_lang_clusters = db.r_lang_clusters
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
                scores[gparent] += 3
                for r1 in forks_of_r[gparent]:
                    scores[r1] += 2 / log(2 + len(u_watching[r1]))

                    # find others by author of gparent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += 2 / log(2 + len(u_watching[r2]))

            # find others by author
            if r in r_info:
                author = r_info[r][0]
                for r1 in u_authoring[author]:
                    scores[r1] += 2 / log(2 + len(u_watching[r1]))

        # cleanup
        for r in u_watching[user] + [0]:
            if r in scores:
                del scores[r]

        scores = [repos[0] for repos in
                  sorted(scores.items(), reverse=True, key=lambda x:x[1])]
        return scores[:10]

    def results(self):
        lines = []
        for u in sorted(self.recommended.keys()):
            r_list = self.recommended[u]
            lines.append(':'.join((str(u),
                                   ','.join([str(v) for v in r_list]))))
        return "\n".join(lines)
