#!/usr/bin/env python

try:
    import cPickle as pickle
except:
    import pickle
import os

from collections import defaultdict
from pprint import pprint

class Database:
    def __init__(self, datadir):
        """Constructor
        """
        self.datadir = datadir

        self.watching_r = defaultdict(list)
        self.u_watching = defaultdict(list)

        self.r_info = {}
        self.forks_of_r = defaultdict(list)
        self.parent_of_r = defaultdict(int)
        self.gparent_of_r = defaultdict(int)

        self.lang_by_r = defaultdict(list)
        self.u_authoring = defaultdict(list)

        # collect data
        self.parse_watching()
        self.parse_repos()
        self.parse_lang()

    def parse_watching(self):
        """Parse data.txt which has main user-repository relationships
        """
        lines = file('/'.join((self.datadir, "data.txt"))).read().split("\n")

        pairs = [[int(x) for x in line.split(":")] for line in lines if line]
        for user, repos in pairs:
            self.watching_r[repos].append(user)
            self.u_watching[user].append(repos)

    def parse_repos(self):
        """Parse repos.txt which has repository lineage information
        """
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

        for repos_gen1, repos_gen2 in self.parent_of_r.items():
            if repos_gen2 in self.parent_of_r:
                repos_gen3 = self.parent_of_r[repos_gen2]
                self.gparent_of_r[repos_gen1] = repos_gen3

    def parse_lang(self):
        """Get lang.txt which has language composition information
        """
        lines = file('/'.join((self.datadir, "lang.txt"))).read().split("\n")

        pairs = [line.split(":") for line in lines if line]
        pairs = [(int(pair[0]),
                  [tuple(x.split(";")) for x in pair[1].split(",")])
                 for pair in pairs]
        pairs = [(x, tuple([(int(z[1]), z[0].lower()) for z in y]))
                 for (x, y) in pairs]

        for repos, langs in pairs:
            for kloc, lang in langs:
                self.lang_by_r[lang].append((kloc, repos))
                self.lang_by_r[lang].sort(reverse=True)
