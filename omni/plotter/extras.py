#!/usr/bin/env python

def i2s2(*items,**kwargs): return kwargs.pop('delim','.').join([str(i) for i in items])
i2s = lambda *items: '.'.join([str(i) for i in items])
