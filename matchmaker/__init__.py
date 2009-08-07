#!/usr/bin/env python

import sys

__all__ = ['database', 'engine', 'kmeans']

def msg(info):
    """Debug output"""
    print >>sys.stderr, ">>", str(info)
