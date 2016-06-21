# -*- coding: utf-8 -*-
#
# Author: Mike McKerns (mmckerns @caltech and @uqfoundation)
# Copyright (c) 2008-2015 California Institute of Technology.
# License: 3-clause BSD.  The full license text is available at:
#  - http://trac.mystic.cacr.caltech.edu/project/pathos/browser/dill/LICENSE
"""
dill: a utility for serialization of python objects

Based on code written by Oren Tirosh and Armin Ronacher.
Extended to a (near) full set of the builtin types (in types module),
and coded to the pickle interface, by <mmckerns@caltech.edu>.
Initial port to python3 by Jonathan Dobson, continued by mmckerns.
Test against "all" python types (Std. Lib. CH 1-15 @ 2.7) by mmckerns.
Test against CH16+ Std. Lib. ... TBD.
"""
__all__ = ['dump','dumps','load','loads','dump_session','load_session',
           'Pickler','Unpickler','register','copy','pickle','pickles',
           'check','HIGHEST_PROTOCOL','DEFAULT_PROTOCOL','PicklingError',
           'UnpicklingError','HANDLE_FMODE','CONTENTS_FMODE','FILE_FMODE']

import logging
log = logging.getLogger("dill")
log.addHandler(logging.StreamHandler())
def _trace(boolean):
    """print a trace through the stack when pickling; useful for debugging"""
    if boolean: log.setLevel(logging.INFO)
    else: log.setLevel(logging.WARN)
    return

stack = set()  # record of 'recursion-sensitive' pickled objects

import os
import sys
diff = None
_use_diff = False
PY3 = (sys.hexversion >= 0x30000f0)
if PY3: #XXX: get types from .objtypes ?
    import builtins as __builtin__
    from pickle import _Pickler as StockPickler, _Unpickler as StockUnpickler
    from _thread import LockType
   #from io import IOBase
    from types import CodeType, FunctionType, MethodType, GeneratorType, \
        TracebackType, FrameType, ModuleType, BuiltinMethodType
    BufferType = memoryview #XXX: unregistered
    ClassType = type # no 'old-style' classes
    EllipsisType = type(Ellipsis)
   #FileType = IOBase
    NotImplementedType = type(NotImplemented)
    SliceType = slice
    TypeType = type # 'new-style' classes #XXX: unregistered
    XRangeType = range
    DictProxyType = type(object.__dict__)
else:
    import __builtin__
    from pickle import Pickler as StockPickler, Unpickler as StockUnpickler
    from thread import LockType
    from types import CodeType, FunctionType, ClassType, MethodType, \
         GeneratorType, DictProxyType, XRangeType, SliceType, TracebackType, \
         NotImplementedType, EllipsisType, FrameType, ModuleType, \
         BufferType, BuiltinMethodType, TypeType
from pickle import HIGHEST_PROTOCOL, PicklingError, UnpicklingError
try:
    from pickle import DEFAULT_PROTOCOL
except ImportError:
    DEFAULT_PROTOCOL = HIGHEST_PROTOCOL
import __main__ as _main_module
import marshal
import gc
# import zlib
from weakref import ReferenceType, ProxyType, CallableProxyType
from functools import partial
from operator import itemgetter, attrgetter
# new in python2.5
if sys.hexversion >= 0x20500f0:
    from types import MemberDescriptorType, GetSetDescriptorType
# new in python3.3
if sys.hexversion < 0x03030000:
    FileNotFoundError = IOError
try:
    import ctypes
    HAS_CTYPES = True
    # if using `pypi`, pythonapi is not found
    IS_PYPY = hasattr(ctypes, 'pythonapi')
except ImportError:
    HAS_CTYPES = False
    IS_PYPY = False
try:
    import imp
    imp.find_module('numpy')
    NumpyUfuncType = True
    NumpyArrayType = True
except ImportError:
    NumpyUfuncType = None
    NumpyArrayType = None
def __hook__():
    global NumpyArrayType, NumpyUfuncType
    from numpy import ufunc as NumpyUfuncType
    from numpy import ndarray as NumpyArrayType
    return True
if NumpyArrayType: # then has numpy
    def ndarraysubclassinstance(obj):
        if type(obj) in (TypeType, ClassType):
            return False # all classes return False
        try: # check if is ndarray, and elif is subclass of ndarray
            cls = getattr(obj, '__class__', None)
            if cls is None: return False
            elif cls is TypeType: return False
            elif 'numpy.ndarray' not in str(getattr(cls, 'mro', int.mro)()):
                return False
        except ReferenceError: return False # handle 'R3' weakref in 3.x
        except TypeError: return False
        # anything below here is a numpy array (or subclass) instance
        __hook__() # import numpy (so the following works!!!)
        # verify that __reduce__ has not been overridden
        NumpyInstance = NumpyArrayType((0,),'int8')
        if id(obj.__reduce_ex__) == id(NumpyInstance.__reduce_ex__) and \
           id(obj.__reduce__) == id(NumpyInstance.__reduce__): return True
        return False
    def numpyufunc(obj):
        if type(obj) in (TypeType, ClassType):
            return False # all classes return False
        try: # check if is ufunc
            cls = getattr(obj, '__class__', None)
            if cls is None: return False
            elif cls is TypeType: return False
            if 'numpy.ufunc' not in str(getattr(cls, 'mro', int.mro)()):
                return False
        except ReferenceError: return False # handle 'R3' weakref in 3.x
        except TypeError: return False
        # anything below here is a numpy ufunc
        return True
else:
    def ndarraysubclassinstance(obj): return False
    def numpyufunc(obj): return False

# make sure to add these 'hand-built' types to _typemap
if PY3:
    CellType = type((lambda x: lambda y: x)(0).__closure__[0])
else:
    CellType = type((lambda x: lambda y: x)(0).func_closure[0])
WrapperDescriptorType = type(type.__repr__)
MethodDescriptorType = type(type.__dict__['mro'])
MethodWrapperType = type([].__repr__)
PartialType = type(partial(int,base=2))
SuperType = type(super(Exception, TypeError()))
ItemGetterType = type(itemgetter(0))
AttrGetterType = type(attrgetter('__repr__'))
FileType = type(open(os.devnull, 'rb', buffering=0))
TextWrapperType = type(open(os.devnull, 'r', buffering=-1))
BufferedRandomType = type(open(os.devnull, 'r+b', buffering=-1))
BufferedReaderType = type(open(os.devnull, 'rb', buffering=-1))
BufferedWriterType = type(open(os.devnull, 'wb', buffering=-1))
try:
    from _pyio import open as _open
    PyTextWrapperType = type(_open(os.devnull, 'r', buffering=-1))
    PyBufferedRandomType = type(_open(os.devnull, 'r+b', buffering=-1))
    PyBufferedReaderType = type(_open(os.devnull, 'rb', buffering=-1))
    PyBufferedWriterType = type(_open(os.devnull, 'wb', buffering=-1))
except ImportError:
    PyTextWrapperType = PyBufferedRandomType = PyBufferedReaderType = PyBufferedWriterType = None
try:
    from cStringIO import StringIO, InputType, OutputType
except ImportError:
    if PY3:
        from io import BytesIO as StringIO
    else:
        from StringIO import StringIO
    InputType = OutputType = None
try:
    __IPYTHON__ is True # is ipython
    ExitType = None     # IPython.core.autocall.ExitAutocall
    singletontypes = ['exit', 'quit', 'get_ipython']
except NameError:
    try: ExitType = type(exit) # apparently 'exit' can be removed
    except NameError: ExitType = None
    singletontypes = []

### File modes
# Pickles the file handle, preserving mode. The position of the unpickled
# object is as for a new file handle.
HANDLE_FMODE = 0
# Pickles the file contents, creating a new file if on load the file does
# not exist. The position = min(pickled position, EOF) and mode is chosen
# as such that "best" preserves behavior of the original file.
CONTENTS_FMODE = 1
# Pickles the entire file (handle and contents), preserving mode and position.
FILE_FMODE = 2

### Shorthands (modified from python2.5/lib/pickle.py)
def copy(obj, *args, **kwds):
    """use pickling to 'copy' an object"""
    return loads(dumps(obj, *args, **kwds))

def dump(obj, file, protocol=None, byref=None, fmode=None, recurse=None):#, strictio=None):
    """pickle an object to a file"""
    from .settings import settings
    strictio = False #FIXME: strict=True needs cleanup
    if protocol is None: protocol = settings['protocol']
    if byref is None: byref = settings['byref']
    if fmode is None: fmode = settings['fmode']
    if recurse is None: recurse = settings['recurse']
    stack.clear()  # clear record of 'recursion-sensitive' pickled objects
    pik = Pickler(file, protocol)
    pik._main = _main_module
    # apply kwd settings
    pik._byref = bool(byref)
    pik._strictio = bool(strictio)
    pik._fmode = fmode
    pik._recurse = bool(recurse)
    # register if the object is a numpy ufunc
    # thanks to Paul Kienzle for pointing out ufuncs didn't pickle
    if NumpyUfuncType and numpyufunc(obj):
        @register(type(obj))
        def save_numpy_ufunc(pickler, obj):
            log.info("Nu: %s" % obj)
            StockPickler.save_global(pickler, obj)
            log.info("# Nu")
            return
        # NOTE: the above 'save' performs like:
        #   import copy_reg
        #   def udump(f): return f.__name__
        #   def uload(name): return getattr(numpy, name)
        #   copy_reg.pickle(NumpyUfuncType, udump, uload)
    # register if the object is a subclassed numpy array instance
    if NumpyArrayType and ndarraysubclassinstance(obj):
        @register(type(obj))
        def save_numpy_array(pickler, obj):
            log.info("Nu: (%s, %s)" % (obj.shape,obj.dtype))
            npdict = getattr(obj, '__dict__', None)
            f, args, state = obj.__reduce__()
            pik.save_reduce(_create_array, (f, args, state, npdict), obj=obj)
            log.info("# Nu")
            return
    # end hack
    pik.dump(obj)
    stack.clear()  # clear record of 'recursion-sensitive' pickled objects
    return

def dumps(obj, protocol=None, byref=None, fmode=None, recurse=None):#, strictio=None):
    """pickle an object to a string"""
    file = StringIO()
    dump(obj, file, protocol, byref, fmode, recurse)#, strictio)
    return file.getvalue()

def load(file):
    """unpickle an object from a file"""
    pik = Unpickler(file)
    pik._main = _main_module
    obj = pik.load()
    if type(obj).__module__ == _main_module.__name__: # point obj class to main
        try: obj.__class__ == getattr(pik._main, type(obj).__name__)
        except AttributeError: pass # defined in a file
   #_main_module.__dict__.update(obj.__dict__) #XXX: should update globals ?
    return obj

def loads(str):
    """unpickle an object from a string"""
    file = StringIO(str)
    return load(file)

# def dumpzs(obj, protocol=None):
#     """pickle an object to a compressed string"""
#     return zlib.compress(dumps(obj, protocol))

# def loadzs(str):
#     """unpickle an object from a compressed string"""
#     return loads(zlib.decompress(str))

### End: Shorthands ###

### Pickle the Interpreter Session
def _module_map():
    """get map of imported modules"""
    from collections import defaultdict
    modmap = defaultdict(list)
    items = 'items' if PY3 else 'iteritems'
    for name, module in getattr(sys.modules, items)():
        if module is None:
            continue
        for objname, obj in module.__dict__.items():
            modmap[objname].append((obj, name))
    return modmap

def _lookup_module(modmap, name, obj, main_module): #FIXME: needs work
    """lookup name if module is imported"""
    for modobj, modname in modmap[name]:
        if modobj is obj and modname != main_module.__name__:
            return modname

def _stash_modules(main_module):
    modmap = _module_map()
    imported = []
    original = {}
    items = 'items' if PY3 else 'iteritems'
    for name, obj in getattr(main_module.__dict__, items)():
        source_module = _lookup_module(modmap, name, obj, main_module)
        if source_module:
            imported.append((source_module, name))
        else:
            original[name] = obj
    if len(imported):
        import types
        newmod = types.ModuleType(main_module.__name__)
        newmod.__dict__.update(original)
        newmod.__dill_imported = imported
        return newmod
    else:
        return original

def _restore_modules(main_module):
    if '__dill_imported' not in main_module.__dict__:
        return
    imports = main_module.__dict__.pop('__dill_imported')
    for module, name in imports:
        exec("from %s import %s" % (module, name), main_module.__dict__)

#NOTE: 06/03/15 renamed main_module to main
def dump_session(filename='/tmp/session.pkl', main=None, byref=False):
    """pickle the current state of __main__ to a file"""
    from .settings import settings
    protocol = settings['protocol']
    if main is None: main = _main_module
    f = open(filename, 'wb')
    try:
        if byref:
            main = _stash_modules(main)
        pickler = Pickler(f, protocol)
        pickler._main = main
        pickler._byref = False   # disable pickling by name reference
        pickler._recurse = False # disable pickling recursion for globals
        pickler._session = True  # is best indicator of when pickling a session
        pickler.dump(main)
    finally:
        f.close()
    return

def load_session(filename='/tmp/session.pkl', main=None):
    """update the __main__ module with the state from the session file"""
    if main is None: main = _main_module
    f = open(filename, 'rb')
    try:
        unpickler = Unpickler(f)
        unpickler._main = main
        unpickler._session = True
        module = unpickler.load()
        unpickler._session = False
        main.__dict__.update(module.__dict__)
        _restore_modules(main)
    finally:
        f.close()
    return

### End: Pickle the Interpreter

class MetaCatchingDict(dict):
    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def __missing__(self, key):
        if issubclass(key, type):
            return save_type
        else:
            raise KeyError()


### Extend the Picklers
class Pickler(StockPickler):
    """python's Pickler extended to interpreter sessions"""
    dispatch = MetaCatchingDict(StockPickler.dispatch.copy())
    _main = None
    _session = False
    from .settings import settings
    _byref = settings['byref']
    _strictio = False
    _fmode = settings['fmode']
    _recurse = settings['recurse']

    def __init__(self, *args, **kwds):
        _byref = kwds.pop('byref', Pickler._byref)
       #_strictio = kwds.pop('strictio', Pickler._strictio)
        _fmode = kwds.pop('fmode', Pickler._fmode)
        _recurse = kwds.pop('recurse', Pickler._recurse)
        StockPickler.__init__(self, *args, **kwds)
        self._main = _main_module
        self._diff_cache = {}
        self._byref = _byref
       #self._strictio = _strictio
        self._fmode = _fmode
        self._recurse = _recurse
    pass

class Unpickler(StockUnpickler):
    """python's Unpickler extended to interpreter sessions and more types"""
    _main = None
    _session = False

    def find_class(self, module, name):
        if (module, name) == ('__builtin__', '__main__'):
            return self._main.__dict__ #XXX: above set w/save_module_dict
        elif (module, name) == ('__builtin__', 'NoneType'):
            return type(None) #XXX: special case: NoneType missing
        return StockUnpickler.find_class(self, module, name)

    def __init__(self, *args, **kwds):
        StockUnpickler.__init__(self, *args, **kwds)
        self._main = _main_module
    pass

'''
def dispatch_table():
    """get the dispatch table of registered types"""
    return Pickler.dispatch
'''

pickle_dispatch_copy = StockPickler.dispatch.copy()

def pickle(t, func):
    """expose dispatch table for user-created extensions"""
    Pickler.dispatch[t] = func
    return

def register(t):
    def proxy(func):
        Pickler.dispatch[t] = func
        return func
    return proxy

def _revert_extension():
    for type, func in list(StockPickler.dispatch.items()):
        if func.__module__ == __name__:
            del StockPickler.dispatch[type]
            if type in pickle_dispatch_copy:
                StockPickler.dispatch[type] = pickle_dispatch_copy[type]

def use_diff(on=True):
    """
    reduces size of pickles by only including object which have changed.
    Decreases pickle size but increases CPU time needed.
    Also helps avoid some unpicklable objects.
    MUST be called at start of script, otherwise changes will not be recorded.
    """
    global _use_diff, diff
    _use_diff = on
    if _use_diff and diff is None:
        try:
            from . import diff as d
        except:
            import diff as d
        diff = d

def _create_typemap():
    import types
    if PY3:
        d = dict(list(__builtin__.__dict__.items()) + \
                 list(types.__dict__.items())).items()
        builtin = 'builtins'
    else:
        d = types.__dict__.iteritems()
        builtin = '__builtin__'
    for key, value in d:
        if getattr(value, '__module__', None) == builtin \
        and type(value) is type:
            yield key, value
    return
_reverse_typemap = dict(_create_typemap())
_reverse_typemap.update({
    'CellType': CellType,
    'WrapperDescriptorType': WrapperDescriptorType,
    'MethodDescriptorType': MethodDescriptorType,
    'MethodWrapperType': MethodWrapperType,
    'PartialType': PartialType,
    'SuperType': SuperType,
    'ItemGetterType': ItemGetterType,
    'AttrGetterType': AttrGetterType,
    'FileType': FileType,
    'BufferedRandomType': BufferedRandomType,
    'BufferedReaderType': BufferedReaderType,
    'BufferedWriterType': BufferedWriterType,
    'TextWrapperType': TextWrapperType,
    'PyBufferedRandomType': PyBufferedRandomType,
    'PyBufferedReaderType': PyBufferedReaderType,
    'PyBufferedWriterType': PyBufferedWriterType,
    'PyTextWrapperType': PyTextWrapperType,
})
if ExitType:
    _reverse_typemap['ExitType'] = ExitType
if InputType:
    _reverse_typemap['InputType'] = InputType
    _reverse_typemap['OutputType'] = OutputType
if PY3:
    _typemap = dict((v, k) for k, v in _reverse_typemap.items())
else:
    _typemap = dict((v, k) for k, v in _reverse_typemap.iteritems())

def _unmarshal(string):
    return marshal.loads(string)

def _load_type(name):
    return _reverse_typemap[name]

def _create_type(typeobj, *args):
    return typeobj(*args)

def _create_function(fcode, fglobals, fname=None, fdefaults=None, \
                                      fclosure=None, fdict=None):
    # same as FunctionType, but enable passing __dict__ to new function,
    # __dict__ is the storehouse for attributes added after function creation
    if fdict is None: fdict = dict()
    func = FunctionType(fcode, fglobals, fname, fdefaults, fclosure)
    func.__dict__.update(fdict) #XXX: better copy? option to copy?
    return func

def _create_ftype(ftypeobj, func, args, kwds):
    if kwds is None:
        kwds = {}
    if args is None:
        args = ()
    return ftypeobj(func, *args, **kwds)

def _create_lock(locked, *args):
    from threading import Lock
    lock = Lock()
    if locked:
        if not lock.acquire(False):
            raise UnpicklingError("Cannot acquire lock")
    return lock

# thanks to matsjoyce for adding all the different file modes
def _create_filehandle(name, mode, position, closed, open, strictio, fmode, fdata): # buffering=0
    # only pickles the handle, not the file contents... good? or StringIO(data)?
    # (for file contents see: http://effbot.org/librarybook/copy-reg.htm)
    # NOTE: handle special cases first (are there more special cases?)
    names = {'<stdin>':sys.__stdin__, '<stdout>':sys.__stdout__,
             '<stderr>':sys.__stderr__} #XXX: better fileno=(0,1,2) ?
    if name in list(names.keys()):
        f = names[name] #XXX: safer "f=sys.stdin"
    elif name == '<tmpfile>':
        f = os.tmpfile()
    elif name == '<fdopen>':
        import tempfile
        f = tempfile.TemporaryFile(mode)
    else:
        # treat x mode as w mode
        if "x" in mode and sys.hexversion < 0x03030000:
            raise ValueError("invalid mode: '%s'" % mode)
        try:
            exists = os.path.exists(name)
        except:
            exists = False
        if not exists:
            if strictio:
                raise FileNotFoundError("[Errno 2] No such file or directory: '%s'" % name)
            elif "r" in mode and fmode != FILE_FMODE:
                name = '<fdopen>' # or os.devnull?
            current_size = 0 # or maintain position?
        else:
            current_size = os.path.getsize(name)

        if position > current_size:
            if strictio:
                raise ValueError("invalid buffer size")
            elif fmode == CONTENTS_FMODE:
                position = current_size
        # try to open the file by name
        # NOTE: has different fileno
        try:
            #FIXME: missing: *buffering*, encoding, softspace
            if fmode == FILE_FMODE:
                f = open(name, mode if "w" in mode else "w")
                f.write(fdata)
                if "w" not in mode:
                    f.close()
                    f = open(name, mode)
            elif name == '<fdopen>': # file did not exist
                import tempfile
                f = tempfile.TemporaryFile(mode)
            elif fmode == CONTENTS_FMODE \
               and ("w" in mode or "x" in mode):
                # stop truncation when opening
                flags = os.O_CREAT
                if "+" in mode:
                    flags |= os.O_RDWR
                else:
                    flags |= os.O_WRONLY
                f = os.fdopen(os.open(name, flags), mode)
                # set name to the correct value
                if PY3:
                    r = getattr(f, "buffer", f)
                    r = getattr(r, "raw", r)
                    r.name = name
                else:
                    if not HAS_CTYPES:
                        raise ImportError("No module named 'ctypes'")
                    class FILE(ctypes.Structure):
                        _fields_ = [("refcount", ctypes.c_long),
                                    ("type_obj", ctypes.py_object),
                                    ("file_pointer", ctypes.c_voidp),
                                    ("name", ctypes.py_object)]

                    class PyObject(ctypes.Structure):
                        _fields_ = [
                            ("ob_refcnt", ctypes.c_int),
                            ("ob_type", ctypes.py_object)
                            ]
                    ctypes.cast(id(f), ctypes.POINTER(FILE)).contents.name = name
                    ctypes.cast(id(name), ctypes.POINTER(PyObject)).contents.ob_refcnt += 1
                assert f.name == name
            else:
                f = open(name, mode)
        except (IOError, FileNotFoundError):
            err = sys.exc_info()[1]
            raise UnpicklingError(err)
    if closed:
        f.close()
    elif position >= 0 and fmode != HANDLE_FMODE:
        f.seek(position)
    return f

def _create_stringi(value, position, closed):
    f = StringIO(value)
    if closed: f.close()
    else: f.seek(position)
    return f

def _create_stringo(value, position, closed):
    f = StringIO()
    if closed: f.close()
    else:
       f.write(value)
       f.seek(position)
    return f

class _itemgetter_helper(object):
    def __init__(self):
        self.items = []
    def __getitem__(self, item):
        self.items.append(item)
        return

class _attrgetter_helper(object):
    def __init__(self, attrs, index=None):
        self.attrs = attrs
        self.index = index
    def __getattribute__(self, attr):
        attrs = object.__getattribute__(self, "attrs")
        index = object.__getattribute__(self, "index")
        if index is None:
            index = len(attrs)
            attrs.append(attr)
        else:
            attrs[index] = ".".join([attrs[index], attr])
        return type(self)(attrs, index)

if PY3:
    def _create_cell(contents):
        return (lambda y: contents).__closure__[0]
else:
    def _create_cell(contents):
        return (lambda y: contents).func_closure[0]

def _create_weakref(obj, *args):
    from weakref import ref
    if obj is None: # it's dead
        if PY3:
            from collections import UserDict
        else:
            from UserDict import UserDict
        return ref(UserDict(), *args)
    return ref(obj, *args)

def _create_weakproxy(obj, callable=False, *args):
    from weakref import proxy
    if obj is None: # it's dead
        if callable: return proxy(lambda x:x, *args)
        if PY3:
            from collections import UserDict
        else:
            from UserDict import UserDict
        return proxy(UserDict(), *args)
    return proxy(obj, *args)

def _eval_repr(repr_str):
    return eval(repr_str)

def _create_array(f, args, state, npdict=None):
   #array = numpy.core.multiarray._reconstruct(*args)
    array = f(*args)
    array.__setstate__(state)
    if npdict is not None: # we also have saved state in __dict__
        array.__dict__.update(npdict)
    return array

def _getattr(objclass, name, repr_str):
    # hack to grab the reference directly
    try: #XXX: works only for __builtin__ ?
        attr = repr_str.split("'")[3]
        return eval(attr+'.__dict__["'+name+'"]')
    except:
        attr = getattr(objclass,name)
        if name == '__dict__':
            attr = attr[name]
        return attr

def _get_attr(self, name):
    # stop recursive pickling
    return getattr(self, name, None) or getattr(__builtin__, name)

def _dict_from_dictproxy(dictproxy):
    _dict = dictproxy.copy() # convert dictproxy to dict
    _dict.pop('__dict__', None)
    _dict.pop('__weakref__', None)
    return _dict

def _import_module(import_name, safe=False):
    try:
        if '.' in import_name:
            items = import_name.split('.')
            module = '.'.join(items[:-1])
            obj = items[-1]
        else:
            return __import__(import_name)
        return getattr(__import__(module, None, None, [obj]), obj)
    except (ImportError, AttributeError):
        if safe:
            return None
        raise

def _locate_function(obj, session=False):
    if obj.__module__ in ['__main__', None]: # and session:
        return False
    found = _import_module(obj.__module__ + '.' + obj.__name__, safe=True)
    return found is obj

#@register(CodeType)
#def save_code(pickler, obj):
#    log.info("Co: %s" % obj)
#    pickler.save_reduce(_unmarshal, (marshal.dumps(obj),), obj=obj)
#    log.info("# Co")
#    return

# The following function is based on 'save_codeobject' from 'cloudpickle'
# Copyright (c) 2012, Regents of the University of California.
# Copyright (c) 2009 `PiCloud, Inc. <http://www.picloud.com>`_.
# License: https://github.com/cloudpipe/cloudpickle/blob/master/LICENSE
@register(CodeType)
def save_code(pickler, obj):
    log.info("Co: %s" % obj)
    if PY3:
        args = (
            obj.co_argcount, obj.co_kwonlyargcount, obj.co_nlocals,
            obj.co_stacksize, obj.co_flags, obj.co_code, obj.co_consts,
            obj.co_names, obj.co_varnames, obj.co_filename, obj.co_name,
            obj.co_firstlineno, obj.co_lnotab, obj.co_freevars, obj.co_cellvars
        )
    else:
        args = (
            obj.co_argcount, obj.co_nlocals, obj.co_stacksize, obj.co_flags,
            obj.co_code, obj.co_consts, obj.co_names, obj.co_varnames,
            obj.co_filename, obj.co_name, obj.co_firstlineno, obj.co_lnotab,
            obj.co_freevars, obj.co_cellvars
        )
    pickler.save_reduce(CodeType, args, obj=obj)
    log.info("# Co")
    return

@register(FunctionType)
def save_function(pickler, obj):
    if not _locate_function(obj): #, pickler._session):
        log.info("F1: %s" % obj)
        if getattr(pickler, '_recurse', False):
            # recurse to get all globals referred to by obj
            from .detect import globalvars
            globs = globalvars(obj, recurse=True, builtin=True)
            # remove objects that have already been serialized
           #stacktypes = (ClassType, TypeType, FunctionType)
           #for key,value in list(globs.items()):
           #    if isinstance(value, stacktypes) and value in stack:
           #        del globs[key]
            # ABORT: if self-references, use _recurse=False
            if obj in globs.values(): # or obj in stack:
                globs = obj.__globals__ if PY3 else obj.func_globals
        else:
            globs = obj.__globals__ if PY3 else obj.func_globals
       #stack.add(obj)
        if PY3:
            pickler.save_reduce(_create_function, (obj.__code__,
                                globs, obj.__name__,
                                obj.__defaults__, obj.__closure__,
                                obj.__dict__), obj=obj)
        else:
            pickler.save_reduce(_create_function, (obj.func_code,
                                globs, obj.func_name,
                                obj.func_defaults, obj.func_closure,
                                obj.__dict__), obj=obj)
        log.info("# F1")
    else:
        log.info("F2: %s" % obj)
        StockPickler.save_global(pickler, obj) #NOTE: also takes name=...
        log.info("# F2")
    return

@register(dict)
def save_module_dict(pickler, obj):
    if is_dill(pickler) and obj == pickler._main.__dict__ and not pickler._session:
        log.info("D1: <dict%s" % str(obj.__repr__).split('dict')[-1]) # obj
        if PY3:
            pickler.write(bytes('c__builtin__\n__main__\n', 'UTF-8'))
        else:
            pickler.write('c__builtin__\n__main__\n')
        log.info("# D1")
    elif not is_dill(pickler) and obj == _main_module.__dict__:
        log.info("D3: <dict%s" % str(obj.__repr__).split('dict')[-1]) # obj
        if PY3:
            pickler.write(bytes('c__main__\n__dict__\n', 'UTF-8'))
        else:
            pickler.write('c__main__\n__dict__\n')   #XXX: works in general?
        log.info("# D3")
    elif '__name__' in obj and obj != _main_module.__dict__ \
    and obj is getattr(_import_module(obj['__name__'],True), '__dict__', None):
        log.info("D4: <dict%s" % str(obj.__repr__).split('dict')[-1]) # obj
        if PY3:
            pickler.write(bytes('c%s\n__dict__\n' % obj['__name__'], 'UTF-8'))
        else:
            pickler.write('c%s\n__dict__\n' % obj['__name__'])
        log.info("# D4")
    else:
        log.info("D2: <dict%s" % str(obj.__repr__).split('dict')[-1]) # obj
        if is_dill(pickler) and pickler._session:
            # we only care about session the first pass thru
            pickler._session = False
        StockPickler.save_dict(pickler, obj)
        log.info("# D2")
    return

@register(ClassType)
def save_classobj(pickler, obj): #FIXME: enable pickler._byref
   #stack.add(obj)
    if obj.__module__ == '__main__': #XXX: use _main_module.__name__ everywhere?
        log.info("C1: %s" % obj)
        pickler.save_reduce(ClassType, (obj.__name__, obj.__bases__,
                                        obj.__dict__), obj=obj)
                                       #XXX: or obj.__dict__.copy()), obj=obj) ?
        log.info("# C1")
    else:
        log.info("C2: %s" % obj)
        StockPickler.save_global(pickler, obj)
        log.info("# C2")
    return

@register(LockType)
def save_lock(pickler, obj):
    log.info("Lo: %s" % obj)
    pickler.save_reduce(_create_lock, (obj.locked(),), obj=obj)
    log.info("# Lo")
    return

@register(ItemGetterType)
def save_itemgetter(pickler, obj):
    log.info("Ig: %s" % obj)
    helper = _itemgetter_helper()
    obj(helper)
    pickler.save_reduce(type(obj), tuple(helper.items), obj=obj)
    log.info("# Ig")
    return

@register(AttrGetterType)
def save_attrgetter(pickler, obj):
    log.info("Ag: %s" % obj)
    attrs = []
    helper = _attrgetter_helper(attrs)
    obj(helper)
    pickler.save_reduce(type(obj), tuple(attrs), obj=obj)
    log.info("# Ag")
    return

def _save_file(pickler, obj, open_):
    if obj.closed:
        position = None
    else:
        obj.flush()
        if obj in (sys.__stdout__, sys.__stderr__, sys.__stdin__):
            position = -1
        else:
            position = obj.tell()
    if is_dill(pickler) and pickler._fmode == FILE_FMODE:
        f = open_(obj.name, "r")
        fdata = f.read()
        f.close()
    else:
        fdata = ""
    if is_dill(pickler):
        strictio = pickler._strictio
        fmode = pickler._fmode
    else:
        strictio = False
        fmode = 0 # HANDLE_FMODE
    pickler.save_reduce(_create_filehandle, (obj.name, obj.mode, position,
                                             obj.closed, open_, strictio,
                                             fmode, fdata), obj=obj)
    return


@register(FileType) #XXX: in 3.x has buffer=0, needs different _create?
@register(BufferedRandomType)
@register(BufferedReaderType)
@register(BufferedWriterType)
@register(TextWrapperType)
def save_file(pickler, obj):
    log.info("Fi: %s" % obj)
    f = _save_file(pickler, obj, open)
    log.info("# Fi")
    return f

if PyTextWrapperType:
    @register(PyBufferedRandomType)
    @register(PyBufferedReaderType)
    @register(PyBufferedWriterType)
    @register(PyTextWrapperType)
    def save_file(pickler, obj):
        log.info("Fi: %s" % obj)
        f = _save_file(pickler, obj, _open)
        log.info("# Fi")
        return f

# The following two functions are based on 'saveCStringIoInput'
# and 'saveCStringIoOutput' from spickle
# Copyright (c) 2011 by science+computing ag
# License: http://www.apache.org/licenses/LICENSE-2.0
if InputType:
    @register(InputType)
    def save_stringi(pickler, obj):
        log.info("Io: %s" % obj)
        if obj.closed:
            value = ''; position = None
        else:
            value = obj.getvalue(); position = obj.tell()
        pickler.save_reduce(_create_stringi, (value, position, \
                                              obj.closed), obj=obj)
        log.info("# Io")
        return

    @register(OutputType)
    def save_stringo(pickler, obj):
        log.info("Io: %s" % obj)
        if obj.closed:
            value = ''; position = None
        else:
            value = obj.getvalue(); position = obj.tell()
        pickler.save_reduce(_create_stringo, (value, position, \
                                              obj.closed), obj=obj)
        log.info("# Io")
        return

@register(PartialType)
def save_functor(pickler, obj):
    log.info("Fu: %s" % obj)
    pickler.save_reduce(_create_ftype, (type(obj), obj.func, obj.args,
                                        obj.keywords), obj=obj)
    log.info("# Fu")
    return

@register(SuperType)
def save_functor(pickler, obj):
    log.info("Su: %s" % obj)
    pickler.save_reduce(super, (obj.__thisclass__, obj.__self__), obj=obj)
    log.info("# Su")
    return

@register(BuiltinMethodType)
def save_builtin_method(pickler, obj):
    if obj.__self__ is not None:
        if obj.__self__ is __builtin__:
            module = 'builtins' if PY3 else '__builtin__'
            _t = "B1"
            log.info("%s: %s" % (_t, obj))
        else:
            module = obj.__self__
            _t = "B3"
            log.info("%s: %s" % (_t, obj))
        if is_dill(pickler):
            _recurse = pickler._recurse
            pickler._recurse = False
        pickler.save_reduce(_get_attr, (module, obj.__name__), obj=obj)
        if is_dill(pickler):
            pickler._recurse = _recurse
        log.info("# %s" % _t)
    else:
        log.info("B2: %s" % obj)
        StockPickler.save_global(pickler, obj)
        log.info("# B2")
    return

@register(MethodType) #FIXME: fails for 'hidden' or 'name-mangled' classes
def save_instancemethod0(pickler, obj):# example: cStringIO.StringI
    log.info("Me: %s" % obj) #XXX: obj.__dict__ handled elsewhere?
    if PY3:
        pickler.save_reduce(MethodType, (obj.__func__, obj.__self__), obj=obj)
    else:
        pickler.save_reduce(MethodType, (obj.im_func, obj.im_self,
                                         obj.im_class), obj=obj)
    log.info("# Me")
    return

if sys.hexversion >= 0x20500f0:
    @register(MemberDescriptorType)
    @register(GetSetDescriptorType)
    @register(MethodDescriptorType)
    @register(WrapperDescriptorType)
    def save_wrapper_descriptor(pickler, obj):
        log.info("Wr: %s" % obj)
        pickler.save_reduce(_getattr, (obj.__objclass__, obj.__name__,
                                       obj.__repr__()), obj=obj)
        log.info("# Wr")
        return

    @register(MethodWrapperType)
    def save_instancemethod(pickler, obj):
        log.info("Mw: %s" % obj)
        pickler.save_reduce(getattr, (obj.__self__, obj.__name__), obj=obj)
        log.info("# Mw")
        return
else:
    @register(MethodDescriptorType)
    @register(WrapperDescriptorType)
    def save_wrapper_descriptor(pickler, obj):
        log.info("Wr: %s" % obj)
        pickler.save_reduce(_getattr, (obj.__objclass__, obj.__name__,
                                       obj.__repr__()), obj=obj)
        log.info("# Wr")
        return

@register(CellType)
def save_cell(pickler, obj):
    log.info("Ce: %s" % obj)
    pickler.save_reduce(_create_cell, (obj.cell_contents,), obj=obj)
    log.info("# Ce")
    return

# The following function is based on 'saveDictProxy' from spickle
# Copyright (c) 2011 by science+computing ag
# License: http://www.apache.org/licenses/LICENSE-2.0
if IS_PYPY:
    @register(DictProxyType)
    def save_dictproxy(pickler, obj):
        log.info("Dp: %s" % obj)
        attr = obj.get('__dict__')
       #pickler.save_reduce(_create_dictproxy, (attr,'nested'), obj=obj)
        if type(attr) == GetSetDescriptorType and attr.__name__ == "__dict__" \
        and getattr(attr.__objclass__, "__dict__", None) == obj:
            pickler.save_reduce(getattr, (attr.__objclass__,"__dict__"),obj=obj)
            log.info("# Dp")
            return
        # all bad below... so throw ReferenceError or TypeError
        from weakref import ReferenceError
        raise ReferenceError("%s does not reference a class __dict__" % obj)

@register(SliceType)
def save_slice(pickler, obj):
    log.info("Sl: %s" % obj)
    pickler.save_reduce(slice, (obj.start, obj.stop, obj.step), obj=obj)
    log.info("# Sl")
    return

@register(XRangeType)
@register(EllipsisType)
@register(NotImplementedType)
def save_singleton(pickler, obj):
    log.info("Si: %s" % obj)
    pickler.save_reduce(_eval_repr, (obj.__repr__(),), obj=obj)
    log.info("# Si")
    return

def _proxy_helper(obj): # a dead proxy returns a reference to None
    """get memory address of proxy's reference object"""
    try: #FIXME: has to be a smarter way to identify if it's a proxy
        address = int(repr(obj).rstrip('>').split(' at ')[-1], base=16)
    except ValueError: # has a repr... is thus probably not a proxy
        address = id(obj)
    return address

def _locate_object(address, module=None):
    """get object located at the given memory address (inverse of id(obj))"""
    special = [None, True, False] #XXX: more...?
    for obj in special:
        if address == id(obj): return obj
    if module:
        if PY3:
            objects = iter(module.__dict__.values())
        else:
            objects = module.__dict__.itervalues()
    else: objects = iter(gc.get_objects())
    for obj in objects:
        if address == id(obj): return obj
    # all bad below... nothing found so throw ReferenceError or TypeError
    from weakref import ReferenceError
    try: address = hex(address)
    except TypeError:
        raise TypeError("'%s' is not a valid memory address" % str(address))
    raise ReferenceError("Cannot reference object at '%s'" % address)

@register(ReferenceType)
def save_weakref(pickler, obj):
    refobj = obj()
    log.info("R1: %s" % obj)
   #refobj = ctypes.pythonapi.PyWeakref_GetObject(obj) # dead returns "None"
    pickler.save_reduce(_create_weakref, (refobj,), obj=obj)
    log.info("# R1")
    return

@register(ProxyType)
@register(CallableProxyType)
def save_weakproxy(pickler, obj):
    refobj = _locate_object(_proxy_helper(obj))
    try:
        _t = "R2"
        log.info("%s: %s" % (_t, obj))
    except ReferenceError:
        _t = "R3"
        log.info("%s: %s" % (_t, sys.exc_info()[1]))
   #callable = bool(getattr(refobj, '__call__', None))
    if type(obj) is CallableProxyType: callable = True
    else: callable = False
    pickler.save_reduce(_create_weakproxy, (refobj, callable), obj=obj)
    log.info("# %s" % _t)
    return

@register(ModuleType)
def save_module(pickler, obj):
    if False: #_use_diff:
        if obj.__name__ != "dill":
            try:
                changed = diff.whats_changed(obj, seen=pickler._diff_cache)[0]
            except RuntimeError:  # not memorised module, probably part of dill
                pass
            else:
                log.info("M1: %s with diff" % obj)
                log.info("Diff: %s", changed.keys())
                pickler.save_reduce(_import_module, (obj.__name__,), obj=obj,
                                    state=changed)
                log.info("# M1")
                return

        log.info("M2: %s" % obj)
        pickler.save_reduce(_import_module, (obj.__name__,), obj=obj)
        log.info("# M2")
    else:
        # if a module file name starts with prefx, it should be a builtin
        # module, so should be pickled as a reference
        if hasattr(obj, "__file__"):
            names = ["base_prefix", "base_exec_prefix", "exec_prefix",
                     "prefix", "real_prefix"]
            builtin_mod = any([obj.__file__.startswith(getattr(sys, name))
                           for name in names if hasattr(sys, name)])
            builtin_mod = builtin_mod or 'site-packages' in obj.__file__
        else:
            builtin_mod = True
        if obj.__name__ not in ("builtins", "dill") \
           and not builtin_mod or is_dill(pickler) and obj is pickler._main:
            log.info("M1: %s" % obj)
            _main_dict = obj.__dict__.copy() #XXX: better no copy? option to copy?
            [_main_dict.pop(item, None) for item in singletontypes
                + ["__builtins__", "__loader__"]]
            pickler.save_reduce(_import_module, (obj.__name__,), obj=obj,
                                state=_main_dict)
            log.info("# M1")
        else:
            log.info("M2: %s" % obj)
            pickler.save_reduce(_import_module, (obj.__name__,), obj=obj)
            log.info("# M2")
        return
    return

@register(TypeType)
def save_type(pickler, obj):
   #stack.add(obj) #XXX: probably don't need object from all cases below
    if obj in _typemap:
        log.info("T1: %s" % obj)
        pickler.save_reduce(_load_type, (_typemap[obj],), obj=obj)
        log.info("# T1")
    elif obj.__module__ == '__main__':
        try: # use StockPickler for special cases [namedtuple,]
            [getattr(obj, attr) for attr in ('_fields','_asdict',
                                             '_make','_replace')]
            log.info("T6: %s" % obj)
            StockPickler.save_global(pickler, obj)
            log.info("# T6")
            return
        except AttributeError: pass
        if issubclass(type(obj), type):
        #   try: # used when pickling the class as code (or the interpreter)
            if is_dill(pickler) and not pickler._byref:
                # thanks to Tom Stepleton pointing out pickler._session unneeded
                _t = 'T2'
                log.info("%s: %s" % (_t, obj))
                _dict = _dict_from_dictproxy(obj.__dict__)
        #   except: # punt to StockPickler (pickle by class reference)
            else:
                log.info("T5: %s" % obj)
                StockPickler.save_global(pickler, obj)
                log.info("# T5")
                return
        else:
            _t = 'T3'
            log.info("%s: %s" % (_t, obj))
            _dict = obj.__dict__
       #print (_dict)
       #print ("%s\n%s" % (type(obj), obj.__name__))
       #print ("%s\n%s" % (obj.__bases__, obj.__dict__))
        for name in _dict.get("__slots__", []):
            del _dict[name]
        pickler.save_reduce(_create_type, (type(obj), obj.__name__,
                                           obj.__bases__, _dict), obj=obj)
        log.info("# %s" % _t)
    # special cases: NoneType
    elif obj is type(None):
        log.info("T7: %s" % obj)
        if PY3:
            pickler.write(bytes('c__builtin__\nNoneType\n', 'UTF-8'))
        else:
            pickler.write('c__builtin__\nNoneType\n')
        log.info("# T7")
    else:
        log.info("T4: %s" % obj)
       #print (obj.__dict__)
       #print ("%s\n%s" % (type(obj), obj.__name__))
       #print ("%s\n%s" % (obj.__bases__, obj.__dict__))
        StockPickler.save_global(pickler, obj)
        log.info("# T4")
    return

@register(property)
def save_property(pickler, obj):
    log.info("Pr: %s" % obj)
    pickler.save_reduce(property, (obj.fget, obj.fset, obj.fdel, obj.__doc__),
                        obj=obj)
    log.info("# Pr")

@register(staticmethod)
@register(classmethod)
def save_classmethod(pickler, obj):
    log.info("Cm: %s" % obj)
    im_func = '__func__' if PY3 else 'im_func'
    try:
        orig_func = getattr(obj, im_func)
    except AttributeError:  # Python 2.6
        orig_func = obj.__get__(None, object)
        if isinstance(obj, classmethod):
            orig_func = getattr(orig_func, im_func) # Unbind
    pickler.save_reduce(type(obj), (orig_func,), obj=obj)
    log.info("# Cm")

# quick sanity checking
def pickles(obj,exact=False,safe=False,**kwds):
    """quick check if object pickles with dill"""
    if safe: exceptions = (Exception,) # RuntimeError, ValueError
    else:
        exceptions = (TypeError, AssertionError, PicklingError, UnpicklingError)
    try:
        pik = copy(obj, **kwds)
        try:
            result = bool(pik.all() == obj.all())
        except AttributeError:
            result = pik == obj
        if result: return True
        if not exact:
            result = type(pik) == type(obj)
            if result: return result
            # class instances might have been dumped with byref=False
            return repr(type(pik)) == repr(type(obj)) #XXX: InstanceType?
        return False
    except exceptions:
        return False

def check(obj, *args, **kwds):
    """check pickling of an object across another process"""
   # == undocumented ==
   # python -- the string path or executable name of the selected python
   # verbose -- if True, be verbose about printing warning messages
   # all other args and kwds are passed to dill.dumps
    verbose = kwds.pop('verbose', False)
    python = kwds.pop('python', None)
    if python is None:
        import sys
        python = sys.executable
    # type check
    isinstance(python, str)
    import subprocess
    fail = True
    try:
        _obj = dumps(obj, *args, **kwds)
        fail = False
    finally:
        if fail and verbose:
            print("DUMP FAILED")
    msg = "%s -c import dill; print(dill.loads(%s))" % (python, repr(_obj))
    msg = "SUCCESS" if not subprocess.call(msg.split(None,2)) else "LOAD FAILED"
    if verbose:
        print(msg)
    return

# use to protect against missing attributes
def is_dill(pickler):
    "check the dill-ness of your pickler"
    return 'dill' in pickler.__module__
   #return hasattr(pickler,'_main')

def _extend():
    """extend pickle with all of dill's registered types"""
    # need to have pickle not choke on _main_module?  use is_dill(pickler)
    for t,func in Pickler.dispatch.items():
        try:
            StockPickler.dispatch[t] = func
        except: #TypeError, PicklingError, UnpicklingError
            log.info("skip: %s" % t)
        else: pass
    return

del diff, _use_diff, use_diff

# EOF
