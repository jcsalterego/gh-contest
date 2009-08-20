#!/usr/bin/env python

try:
    import cPickle as pickle
except:
    import pickle
try:
    from itertools import permutations
except:
    from matchmaker.utils import permutations
import MySQLdb as mysqldb
import os

from math import log
from collections import defaultdict
from pprint import pprint
from matchmaker import msg
from matchmaker.kmeans import *

class Database:
    def __init__(self, datadir):
        """Constructor
        """
        self.datadir = datadir
        self.test_u = []
        self.top_repos = []
        self.r_matrix = {} # special pickling
        self.u_matrix = {} # special pickling
        self.r_idf_avg = {}
        self.fields = ['test_u', 'top_repos', 'r_idf_avg']
        self.save_db = True

        if self.pickle_jar():
            return

        fields = (
            # dict          key        = values
            ("watching_r    repos      = user",                   list),
            ("u_watching    user       = repos",                  list),
            ("r_info        repos      = author, name, creation", list),
            ("r_name        repos_name = repos",                  list),
            ("r_langs       repos      = lang, kloc",             list),
            ("r_lang_tuple  repos      = tuple_of_langs",         list),
            ("r_prefixes    prefix     = repos",                  list),
            ("r_idf         repos      = user, tf_idf",           list),
            ("forks_of_r    parent     = child",                  list),
            ("parent_of_r   child      = parent",                 int),
            ("gparent_of_r  child      = grandparent",            int),
            ("lang_by_r     lang       = kloc, repos",            list),
            ("u_authoring   author     = repos",                  list),
        )
        for defn, datatype in fields:
            name, key, _ = defn.split(None, 2)
            setattr(self, name, defaultdict(datatype))
            self.fields.append(name)
        self.fields.sort()

        # collect data
        self.parse_test()
        self.parse_watching()
        self.parse_repos()
        self.parse_lang()

        self.fill_pickle_jar()

    def pickle_jar(self):

        jar = '/'.join((self.datadir, "pickle.jar"))
        if os.path.exists(jar):
            try:
                jarf = open(jar, 'r')
                d = pickle.load(jarf)
                jarf.close()
            except:
                return False

            self.fields = d['fields']
            for field in self.fields:
                setattr(self, field, d[field])
            return True
        else:
            return False

    def fill_pickle_jar(self):
        jar = '/'.join((self.datadir, "pickle.jar"))
        d = {}

        msg("Filling pickle jar '%s'" % jar)

        for field in self.fields:
            d[field] = getattr(self, field)
        d['fields'] = self.fields

        jarf = open(jar, 'w')
        pickle.dump(d, jarf)
        jarf.close()

    def summary(self, unabridged=False):
        props = ("watching_r "
                 "u_watching "
                 "r_info "
                 "r_name "
                 "r_langs "
                 "forks_of_r "
                 "parent_of_r "
                 "gparent_of_r "
                 "lang_by_r "
                 "u_authoring ").split()
        for prop in props:
            print(">> %s" % prop)
            if unabridged:
                pprint(dict(getattr(self, prop).items()))
            else:
                pprint(dict(getattr(self, prop).items()[:5]))
            print("")

        msg("test_u")
        if unabridged:
            pprint(self.test_u)
        else:
            pprint(self.test_u[:5])

    def parse_watching(self):
        """Parse data.txt which has main user-repository relationships
        """

        msg("parsing data.txt")
        lines = file('/'.join((self.datadir, "data.txt"))).read().split("\n")

        test_r = set()
        pairs = [[int(x) for x in line.split(":")] for line in lines if line]
        for user, repos in pairs:
            self.watching_r[repos].append(user)
            self.u_watching[user].append(repos)

            if user in self.test_u:
                test_r.add(repos)

        msg("calculating tf-idf")
        iter = 0
        total_users = float(len(self.u_watching))
        for repos, users in self.watching_r.items():
            idf_repos = log(total_users / (1.0 + len(self.watching_r[repos])))
            tf_idf_avg = 0.0
            for user in users:
                tf_user = 1.0 / len(self.u_watching[user])
                tf_idf = tf_user * idf_repos
                tf_idf_avg += tf_idf
                self.r_idf[repos].append((user, tf_idf))

                # counter
                iter += 1
                if iter % 10000 == 0:
                    msg("tf-idf iter %d" % iter)
            self.r_idf_avg[repos] = tf_idf_avg / len(users)

        msg("making top_repos")
        top_repos = sorted(self.watching_r.items(),
                           key=lambda x:len(x[1]),
                           reverse=True)
        self.top_repos = [repos[0] for repos in top_repos[:50]]

        if not self.save_db:
            return

        conn = mysqldb.connect(host='127.0.0.1',
                               user='root',
                               passwd='',
                               db='matrix')
        c = conn.cursor()

        """
        iter = 0
        msg("making u_matrix")
        for users in self.watching_r.values():
            users.sort()
            for i in xrange(len(users)):
                for j in xrange(i + 1, len(users)):
                    u_i, u_j = users[i], users[j]

                    if u_i not in self.u_matrix:
                        self.u_matrix[u_i] = {u_j: 1}
                    elif u_j not in self.u_matrix[u_i]:
                        self.u_matrix[u_i][u_j] = 1
                    else:
                        self.u_matrix[u_i][u_j] += 1

                    iter += 1
                    if iter % 100000 == 0:
                        msg("[] iter %d" % iter)

        iter = 0
        msg("saving u_matrix")
        values = []
        for u_i in self.u_matrix:
            for u_j in self.u_matrix[u_i]:
                values.append("(%d,%d,%d)"
                              % (u_i, u_j, self.u_matrix[u_i][u_j]))

                iter += 1
                if iter % 5000 == 0:
                    sql = "".join(("INSERT INTO u_matrix(u1,u2,val) VALUES",
                                   ",".join(values)))
                    c.execute(sql)
                    values = []
                if iter % 10000 == 0:
                    msg("DB iter %d" % iter)
                    conn.commit()
        if values:
            sql = "".join(("INSERT INTO u_matrix ",
                           ",".join(values)))
            c.execute(sql)
        """

        iter = 0
        msg("making r_matrix_fwd")
        for repos in self.u_watching.values():
            repos.sort()
            for i in xrange(len(repos)):
                for j in xrange(i + 1, len(repos)):
                    r_i, r_j = repos[i], repos[j]

                    if r_i not in self.r_matrix:
                        self.r_matrix[r_i] = {r_j: 1}
                    elif r_j not in self.r_matrix[r_i]:
                        self.r_matrix[r_i][r_j] = 1
                    else:
                        self.r_matrix[r_i][r_j] += 1

                    iter += 1
                    if iter % 100000 == 0:
                        msg("[] iter %d" % iter)
        iter = 0
        msg("saving r_matrix_fwd")
        values = []
        for r_i in self.r_matrix:
            for r_j in self.r_matrix[r_i]:
                values.append("(%d,%d,%d)"
                              % (r_i, r_j, self.r_matrix[r_i][r_j]))
                iter += 1
                if iter % 5000 == 0:
                    sql = "".join(("INSERT INTO r_matrix_fwd(r1,r2,val) VALUES",
                                   ",".join(values)))
                    c.execute(sql)
                    values = []
                if iter % 10000 == 0:
                    msg("DB iter %d" % iter)
                    conn.commit()
        if values:
            sql = "".join(("INSERT INTO r_matrix_fwd(r1,r2,val) VALUES",
                           ",".join(values)))
            c.execute(sql)


        iter = 0
        msg("making r_matrix_bkwd")
        for repos in self.u_watching.values():
            repos.sort(reverse=True)
            for i in xrange(len(repos)):
                for j in xrange(i + 1, len(repos)):
                    r_i, r_j = repos[i], repos[j]

                    if r_i not in self.r_matrix:
                        self.r_matrix[r_i] = {r_j: 1}
                    elif r_j not in self.r_matrix[r_i]:
                        self.r_matrix[r_i][r_j] = 1
                    else:
                        self.r_matrix[r_i][r_j] += 1

                    iter += 1
                    if iter % 100000 == 0:
                        msg("[] iter %d" % iter)
        iter = 0
        msg("saving r_matrix_bkwd")
        values = []
        for r_i in self.r_matrix:
            for r_j in self.r_matrix[r_i]:
                values.append("(%d,%d,%d)"
                              % (r_i, r_j, self.r_matrix[r_i][r_j]))
                iter += 1
                if iter % 5000 == 0:
                    sql = "".join(("INSERT INTO r_matrix_bkwd(r1,r2,val) VALUES",
                                   ",".join(values)))
                    c.execute(sql)
                    values = []
                if iter % 10000 == 0:
                    msg("DB iter %d" % iter)
                    conn.commit()
        if values:
            sql = "".join(("INSERT INTO r_matrix_bkwd(r1,r2,val) VALUES",
                           ",".join(values)))
            c.execute(sql)







    def parse_repos(self):
        """Parse repos.txt which has repository lineage information
        """

        msg("parsing repos.txt")
        lines = file('/'.join((self.datadir, "repos.txt"))).read().split("\n")

        pairs = [line.replace(":", ",").split(",") for line in lines if line]
        pairs = [tuple([int(pair[0]),
                        int(pair[3]) if pair[3:4] else 0,
                        pair[1],
                        pair[2]])
                 for pair in pairs]

        for repos, parent, name, creation in pairs:
            if parent > 0:
                self.forks_of_r[parent].append(repos)
                self.parent_of_r[repos] = parent
            author, name = name.split("/")
            self.r_info[repos] = (author, name, creation)
            self.u_authoring[author].append(repos)
            self.r_name[name].append(repos)

            words = name.lower().replace("-", "_").replace(".", "_")
            words = words.split("_")
            prefixes = [w for w in words if len(w) > 2][:-1]
            if not prefixes:
                continue

            for i in xrange(1, len(prefixes)):
                prefix = "-".join(prefixes[0:i])
                if prefix in ('the', 'test', 'php', 'acts'):
                    continue
                self.r_prefixes[prefix].append(repos)

        for repos_gen1, repos_gen2 in self.parent_of_r.items():
            if repos_gen2 in self.parent_of_r:
                repos_gen3 = self.parent_of_r[repos_gen2]
                self.gparent_of_r[repos_gen1] = repos_gen3

    def parse_lang(self):
        """Get lang.txt which has language composition information
        """

        msg("parsing lang.txt")
        lines = file('/'.join((self.datadir, "lang.txt"))).read().split("\n")

        pairs = [line.split(":") for line in lines if line]
        pairs = [(int(pair[0]),
                  [tuple(x.split(";")) for x in pair[1].split(",")])
                 for pair in pairs]
        pairs = [(x, tuple([(int(z[1]), z[0].lower()) for z in y]))
                 for (x, y) in pairs]

        all_langs = defaultdict(bool)
        for repos, langs in pairs:
            for kloc, lang in langs:
                all_langs[lang] = True
        all_langs = sorted(all_langs.keys())

        msg("build lang_by_r and r_langs")
        for repos, langs in pairs:
            for kloc, lang in langs:
                lnloc = int(log(kloc + 1, 10))
                self.lang_by_r[lang].append((lnloc, repos))
                self.r_langs[repos].append((lang, lnloc))

        for lang in self.lang_by_r.keys():
            self.lang_by_r[lang].sort(key=lambda x:x[1])

    def parse_test(self):
        """Parse test.txt which has test subjects
        """

        msg("parsing test.txt")
        lines = file('/'.join((self.datadir, "test.txt"))).read().split("\n")
        self.test_u = sorted([int(line) for line in lines if line])
