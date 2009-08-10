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
        r_matrix = db.r_matrix
        top_repos = db.top_repos
        lang_by_r = db.lang_by_r
        u_watching = db.u_watching
        u_matrix = db.u_matrix
        watching_r = db.watching_r
        forks_of_r = db.forks_of_r
        parent_of_r = db.parent_of_r
        gparent_of_r = db.parent_of_r
        u_authoring = db.u_authoring

        scores = defaultdict(int)

        # find favorite author (by simple majority)
        fav_author = None
        authors = defaultdict(int)
        for r in u_watching[user]:
            if r in r_info:
                author = r_info[r][0]
                authors[author] += 1
        authors = sorted(authors.items(), reverse=True, key=lambda x:x[1])
        if len(authors) > 1 and authors[0][1] > authors[1][1]:
            fav_author = authors[0][0]

        if user in u_matrix:
            neighbors = [neighbor[0]
                         for neighbor
                         in sorted(u_matrix[user].items(),
                                   reverse=True,
                                   key=lambda x:x[1])[:10]]
            for u1 in neighbors:
                for r1 in u_watching[u1]:
                    scores[r1] += 2 / log(2)

        # generate language profile
        num_lang_r = 0
        lang_r = defaultdict(int)
        for r in u_watching[user]:
            if r in r_langs:
                num_lang_r += 1
                for lang, lnloc in r_langs[r]:
                    lang_r[lang] += lnloc
        for lang in lang_r:
            lnloc = lang_r[lang] = num_lang_r
            for r1, lnloc2 in lang_by_r[lang]:
                if abs(lnloc2 - lnloc) <= 1:
                    scores[r1] += 5

        matrix_repos = defaultdict(int)
        if user in u_matrix:
            for u1 in u_matrix[user]:
                for r1 in u_watching[u1]:
                    matrix_repos[r1] += 1

        for r in u_watching[user]:
            if r in r_matrix:
                for r1 in r_matrix[r]:
                    matrix_repos[r1] += 1

        matrix_repos = [r
                        for r
                        in matrix_repos.items()
                        if r[1] > 1]
        for r, score in matrix_repos:
            scores[r] += 2

        for r in u_watching[user]:
            # loop through all watched repositories

            # find forks
            for r1 in forks_of_r[r]:
                scores[r1] += 2 + log(2 + len(watching_r[r1]))

            # find parents and siblings
            if parent_of_r[r] > 0:
                parent = parent_of_r[r]
                scores[parent] += 2
                for r1 in forks_of_r[parent]:
                    scores[r1] += 2 + log(2 + len(watching_r[r1]))

                    # find others by author of parent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += 1 + log(2 + len(watching_r[r2]))

            # find grandparents and uncles/aunts
            if gparent_of_r[r] > 0:
                gparent = gparent_of_r[r]
                scores[gparent] += 3
                for r1 in forks_of_r[gparent]:
                    scores[r1] += 2 + log(2 + len(watching_r[r1]))

                    # find others by author of gparent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += 2 + log(2 + len(watching_r[r2]))

            # find others by author
            if r in r_info:
                author = r_info[r][0]
                for r1 in sorted(u_authoring[author], reverse=True):
                    if author == fav_author:
                        scores[r1] += 9.0 + log(2 + len(watching_r[r1]))
                    else:
                        scores[r1] += 3.0 + log(2 + len(watching_r[r1]))

        # cleanup
        for r in u_watching[user] + [0]:
            if r in scores:
                del scores[r]

        orig_scores = scores
        scores = sorted(scores.items(), reverse=True, key=lambda x:x[1])
        authors = defaultdict(int)
        names = defaultdict(int)
        iter = 0
        for r, score in scores:
            if r in r_info:
                author, name, _ = r_info[r]
                authors[author] += 1
                names[name] += 1

                if authors[author] > 3:
                    del scores[iter]
                elif names[name] > 5:
                    del scores[iter]

                iter += 1

        top_scores = [repos[0] for repos in scores[:10]]
        num_scores = len(top_scores)
        
        if not num_scores:
            msg("  no scores!")
        else:
            avg_score = (float(sum([repos[1]
                                    for repos in scores[:num_scores]]))
                         / num_scores)
            msg("  avg: %6.2f - 1st: %6.2f - last: %6.2f"
                % (avg_score, scores[0][1], scores[num_scores - 1][1]))

        if num_scores < 10:
            for r in top_repos:
                if r not in top_scores:
                    top_scores.append(r)
                if len(top_scores) >= 10:
                    break

        return top_scores

    def results(self):
        lines = []
        for u in sorted(self.recommended.keys()):
            r_list = self.recommended[u]
            lines.append(':'.join((str(u),
                                   ','.join([str(v) for v in r_list]))))
        return "\n".join(lines)
