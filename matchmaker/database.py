#!/usr/bin/env python

try:
    import cPickle as pickle
except:
    import pickle
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
        self.r_lang_clusters = []
        self.fields = ['test_u', 'r_lang_clusters']

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
        self.parse_watching()
        self.parse_repos()
        self.parse_lang()
        self.parse_test()

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

        pairs = [[int(x) for x in line.split(":")] for line in lines if line]
        for user, repos in pairs:
            self.watching_r[repos].append(user)
            self.u_watching[user].append(repos)

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

#         points = []
#         msg("build r_lang_tuple")
#         for repos, langs in pairs:
#             r_langs = defaultdict(int)
#             for kloc, lang in langs:
#                 lnlog = int(log(kloc + 1))
#                 r_langs[lang] = lnlog
#             tval = tuple([r_langs[lang] for lang in all_langs])

#             points.append(Point(tval, repos))
#             self.r_lang_tuple[repos] = tval

#         # k = sample size
#         k, cutoff = 50, 2

#         msg("build r_lang_clusters of %d points" % len(points))
#         self.r_lang_clusters = kmeans(points, k, cutoff)
#         msg("clusters = %d!" % len(self.r_lang_clusters))

#         msg("normalizing cluster")
#         r_lang_clusters = []
#         for cluster in self.r_lang_clusters:
#             points = []
#             for p in cluster.points:
#                 points.append(p.reference)
#             r_lang_clusters.append(sorted(points))
#         self.r_lang_clusters = r_lang_clusters

        msg("build lang_by_r and r_langs")
        for repos, langs in pairs:
            for kloc, lang in langs:
                lnlog = int(log(kloc + 1))
                self.lang_by_r[lang].append((lnlog, repos))
                self.r_langs[repos].append((lang, lnlog))

        for lang in self.lang_by_r.keys():
            self.lang_by_r[lang].sort(key=lambda x:x[1])

    def parse_test(self):
        """Parse test.txt which has test subjects
        """

        msg("parsing test.txt")
        lines = file('/'.join((self.datadir, "test.txt"))).read().split("\n")
        self.test_u = sorted([int(line) for line in lines if line])
