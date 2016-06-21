#!/usr/bin/env python
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2016 California Institute of Technology.
# License: 3-clause BSD.  The full license text is available at:
#  - http://trac.mystic.cacr.caltech.edu/project/pathos/browser/dill/LICENSE
"""
global settings for Pickler
"""

try:
    from pickle import DEFAULT_PROTOCOL
except ImportError:
    from pickle import HIGHEST_PROTOCOL as DEFAULT_PROTOCOL

settings = {
   #'main' : None,
    'protocol' : DEFAULT_PROTOCOL,
    'byref' : False,
   #'strictio' : False,
    'fmode' : 0, #HANDLE_FMODE
    'recurse' : False,
}

del DEFAULT_PROTOCOL

