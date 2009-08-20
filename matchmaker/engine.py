#!/usr/bin/env python

from datetime import date
import MySQLdb as mysqldb
import random
import sys
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

        partition = 10
        if partition > 1:
            new_len = len(db.test_u) / partition
            msg("Partitioning 1/%d [%d]" % (partition, new_len))
            db.test_u = sorted(db.test_u)[:new_len]

        msg("Beginning recommendations")
        total = len(db.test_u)
        i = 0
        for u in sorted(db.test_u, reverse=True):
            self.recommended[u] = self.user_process(u)
            i += 1
            if i % 10 == 0:
                msg("[%3.2f%%] %d/%d processed"
                    % (float(i)/float(total)*100.0, i, total))
    
    def user_process(self, user):
        """Returns ten recommendations
        """
        db = self.database

        if user not in db.u_watching:
            # blank son of a gun!
            msg("making local top_repos")
            top_repos = sorted(db.watching_r.items(),
                               key=lambda x:sum([1 for y in x[1]
                                                 if abs(user - y) < 250]),
                               reverse=True)
            return [x[0] for x in top_repos][:10]

        r_info = db.r_info
        r_name = db.r_name
        r_langs = db.r_langs
        r_lang_tuple = db.r_lang_tuple
        r_prefixes = db.r_prefixes
        top_repos = db.top_repos
        lang_by_r = db.lang_by_r
        u_matrix = db.u_matrix
        u_watching = db.u_watching
        watching_r = db.watching_r
        forks_of_r = db.forks_of_r
        parent_of_r = db.parent_of_r
        gparent_of_r = db.parent_of_r
        u_authoring = db.u_authoring

        scores = defaultdict(int)

        # find favorite author (by simple majority)
        fav_authors = {}
        """ # ignore fav_authors
        authors = defaultdict(int)
        for r in u_watching[user]:
            if r in r_info:
                author = r_info[r][0]
                authors[author] += 1

        # grab top 3 authors
        authors = sorted(authors.items(), reverse=True, key=lambda x:x[1])[:3]
        if len(authors) > 1:
            total = float(sum([x[1] for x in authors]))
            
            for a_name, a_score in authors:
                if a_score == 1:
                    continue

                # partition 16 appropriately
                fav_authors[a_name] = float(a_score) / float(total) * 16.0

        msg(fav_authors.items())
        msg("-" * 78)
        """        
        
        """
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
                    scores[r1] += 2.5
        """

        conn = mysqldb.connect(host='127.0.0.1',
                               user='root',
                               passwd='',
                               db='matrix')
        c = conn.cursor()

        results = []
        c.execute(("SELECT u2, val "
                   "FROM u_matrix2 "
                   "WHERE u1=%d "
                   "ORDER BY val DESC")
                  % user)
        results += list(c.fetchall())
        c.execute(("SELECT u1, val "
                   "FROM u_matrix2 "
                   "WHERE u2=%d "
                   "ORDER BY val DESC")
                  % user)
        results += list(c.fetchall())
        r_neighbors = set()
        top_neighbors = {}
        for u1, _ in results:
            if u1 in u_watching:
                for r1 in u_watching[u1]:
                    r_neighbors.add(r1)
        for r1 in r_neighbors:
            top_neighbors[r1] = db.r_idf_avg[r1]
        top_neighbors = sorted(top_neighbors.items(),
                               key=lambda x:x[1],
                               reverse=True)
        for r1, val in top_neighbors[:5]:
            scores[r1] += log(val + len(watching_r[r1]), 10)

        for r in u_watching[user]:
            # loop through all watched repositories

            # check r_matrix

            results = []
            c.execute(("SELECT r2, val "
                       "FROM r_matrix_fwd "
                       "WHERE r1=%d "
                       "ORDER BY val DESC "
                       "LIMIT 5")
                      % r)
            results += list(c.fetchall())
            c.execute(("SELECT r2, val "
                       "FROM r_matrix_bkwd "
                       "WHERE r1=%d "
                       "ORDER BY val DESC "
                       "LIMIT 5")
                      % r)
            results += list(c.fetchall())
            results.sort(reverse=True, key=lambda x:x[1])
            for r1, val in results[:5]:
                scores[r1] += log(val + len(watching_r[r1]), 10)

            # find forks
            for r1 in forks_of_r[r]:
                scores[r1] += log(2 + len(watching_r[r1]), 10)

            # find parents and siblings
            if parent_of_r[r] > 0:
                parent = parent_of_r[r]
                scores[parent] += 2
                for r1 in forks_of_r[parent]:
                    scores[r1] += log(2 + len(watching_r[r1]), 10)

                    # find others by author of parent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += 0.5 * log(2 + len(watching_r[r2]), 10)

            # find grandparents and uncles/aunts
            if gparent_of_r[r] > 0:
                gparent = gparent_of_r[r]
                scores[gparent] += 3
                for r1 in forks_of_r[gparent]:
                    scores[r1] += log(2 + len(watching_r[r1]), 10)

                    # find others by author of gparent
                    if r1 in r_info:
                        author = r_info[r1][0]
                        for r2 in u_authoring[author]:
                            scores[r2] += log(2 + len(watching_r[r2]), 10)

            # find others by author, name and prefixes
            if r in r_info:
                author, name = r_info[r][0], r_info[r][1]
                for r1 in sorted(u_authoring[author], reverse=True):
                    scores[r1] += 1.5 * log(2 + len(watching_r[r1]), 10)

                # check names
                if name in r_name:
                    for r1 in r_name[name]:
                        scores[r1] += log(1 + len(watching_r[r1]), 10)

                words = name.lower().replace("-", "_").replace(".", "_")
                words = words.split("_")
                prefixes = [w for w in words if len(w) > 2]
                if not prefixes:
                    continue

                for i in xrange(1, len(prefixes) - 1):
                    prefix = "-".join(prefixes[0:i])
                    if prefix in r_prefixes:
                        for r2 in r_prefixes[prefix]:
                            scores[r2] += (0.25 * i
                                           * log(1 + len(watching_r[r1]), 10))

        if len(u_watching[user]) > 7:
            dates = [r_info[r][2]
                     for r in u_watching[user]
                     if r in r_info]
            msg(dates)
            mean = sum(dates) / len(dates)
            msg("mean is %s" % date(1,1,1).fromordinal(mean))

            std_dev = (sum([(x - mean) ** 2 for x in dates])
                       / len(dates)) ** 0.5
            threshold = std_dev * 2.5
            msg("std_dev is %f" % std_dev)

            for r1 in scores:
                if r1 not in r_info:
                    continue
                
                created = r_info[r1][2]
                if abs(created - mean) > threshold:
                    scores[r1] -= 10.0

        if True:
            output = []
            fh = file("debug.txt", "a")
            output.append("user: %5d" % user)
            output.append("watching: %5d, scores: %5d" % (len(u_watching[user]), len(scores)))
            output.append("")

            for r in u_watching[user]:
                if r in r_info:
                    output.append("     WATCH %8d: %-20s %-50s %s %s"
                                  % (r,
                                     r_info[r][0],
                                     r_info[r][1],
                                     r_info[r][2],
                                     date(1, 1, 1).fromordinal(r_info[r][2])))
                else:
                    output.append("     WATCH %8d" % r)
            output.append("-")

            scores_ = sorted(scores.items(),
                             key=lambda x:(1 if x in u_watching[user] else 0, x[1]),
                             reverse=True)
            for r, score in scores_:
                if r in u_watching[user]:
                    if r in r_info:
                        output.append("   %6.3f %8d: %-20s %-50s %s %s"
                                      % (score, r,
                                         r_info[r][0],
                                         r_info[r][1],
                                         r_info[r][2],
                                         date(1, 1, 1).fromordinal(r_info[r][2])))
                    else:
                        output.append("   %4.2f %8d"
                                      % (score, r))

                else:
                    if r in r_info:
                        output.append("++ %6.3f %8d: %-20s %-50s %s %s"
                                      % (score, r,
                                         r_info[r][0],
                                         r_info[r][1],
                                         r_info[r][2],
                                         date(1, 1, 1).fromordinal(r_info[r][2])))
                    else:
                        output.append("++ %4.2f %8d"
                                      % (score, r))
            output.append("")
            fh.write("\n".join(output))
            fh.close()

        # cleanup
        for r in u_watching[user] + [0]:
            try:
                del scores[r]
            except:
                pass

        orig_scores = scores
        scores = sorted(scores.items(), reverse=True, key=lambda x:x[1])
        authors = defaultdict(int)
        names = defaultdict(int)
        purge = []
        iter = 0
        for r, score in scores:
            if r in r_info:
                author, name, _ = r_info[r]
                authors[author] += 1
                names[name] += 1

                if authors[author] > 3:
                    purge.append(iter)
                elif names[name] > 5:
                    purge.append(iter)
            iter += 1

        for i in sorted(purge, reverse=True):
            del scores[i]

        if len(scores) > 3000:
            mean = sum([x[1] for x in scores]) / len(scores)
            std_dev = (sum([(x[1] - mean) ** 2
                            for x in scores])
                       / len(scores)) ** 0.5
            cutoff = mean + std_dev * 2.5
            scores_ = sorted([x for x in scores if x[1] > cutoff])
            if scores_:
                scores = scores_
    
        top_scores = [repos[0] for repos in scores[:10]]
        num_scores = len(top_scores)
        
        if not num_scores:
            msg("  no scores! so, making local top_repos")
            top_repos = sorted(db.watching_r.items(),
                               key=lambda x:sum([1 for y in x[1]
                                                 if abs(user - y) < 250]),
                               reverse=True)
            return [x[0] for x in top_repos][:10]
        else:
            avg_score = (float(sum([repos[1]
                                    for repos in scores[:num_scores]]))
                         / num_scores)
            msg("  avg: %6.2f - 1st: %6.2f - last: %6.2f"
                % (avg_score, scores[0][1], scores[num_scores - 1][1]))

        if num_scores < 10:
            msg("making local top_repos since num_scores < 10")
            top_repos = sorted(db.watching_r.items(),
                               key=lambda x:sum([1 for y in x[1]
                                                 if abs(user - y) < 250]),
                               reverse=True)
            top_repos = [x[0] for x in top_repos][:10]
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
