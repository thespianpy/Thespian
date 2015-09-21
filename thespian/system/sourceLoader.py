try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO
from io import BytesIO
from zipfile import ZipFile
from os import path as ospath
import logging
import imp
import sys
from thespian.actors import InvalidActorSourceHash
from thespian.system.utilis import thesplog


if sys.version_info >= (3,0):
    exec("def do_exec(co, loc): exec(co, loc)\n")
else:
    exec("def do_exec(co, loc): exec co in loc\n")


class HashLoader(object):
    def __init__(self, finder, isModuleDir=False):
        self.finder = finder
        self.isModuleDir = isModuleDir
    def load_module(self, moduleName):
        if not moduleName.startswith(self.finder.hashRoot()) and \
           moduleName != 'six':   # six has it's own loader; adding the hashRoot will confound it
            moduleName = self.finder.hashRoot() + '.' + moduleName
        if moduleName in sys.modules:
            return sys.modules[moduleName]
        mod = sys.modules.setdefault(moduleName, imp.new_module(moduleName))
        mod.__file__ = moduleName
        mod.__loader__ = self
        if self.isModuleDir:
            mod.__path__ = moduleName
            mod.__package__ = moduleName
        else:
            mod.__package__ = moduleName.rpartition('.')[0]
        if self.isModuleDir:
            name = ospath.join(*tuple(moduleName.split('.')[1:] + ['__init__.py']))
        elif '.' in moduleName:
            name = ospath.join(*tuple(moduleName.split('.')[1:])) + '.py'
        else:
            name = moduleName + '.py'
        try:
            # Ensure the file ends in a carriage-return.  The path
            # importer does this automatically and no trailing
            # whitespace results in SyntaxError or IndentError
            # exceptions.  In addition, using "universal newlines"
            # mode to read the file is not always effective
            # (e.g. ntlm.HTTPNtlmAuthHandler.py, so explicitly ensure
            # the proper line endings for the compiler.
            if sys.version_info >= (3,0):
                converter = lambda s: compile(s + b'\n', mod.__file__, "exec")
            else:
                converter = lambda s: compile(s.replace('\r\n', '\n')+'\n', mod.__file__, "exec")
            code = self.finder.withZipElementSource(
                name,
                converter)
            do_exec(code, mod.__dict__)
        except Exception as ex:
            thesplog('sourceload realization failure: %s', ex)
            del sys.modules[moduleName]
            #return None
            raise
        return mod

class HashRootLoader(object):
    """The SourceHashFinder below inserts the hashRoot at the beginning of the
       import path to ensure that the sources imported from the
       hashedSource are in a separate namespace.  This HashRootLoader
       object "eats" that top-level hashRoot namespace from the
       beginning of import paths.
    """
    def __init__(self, finder):
        self.finder = finder
    def load_module(self, moduleName):
        if moduleName != self.finder.hashRoot(): return None
        mod             = sys.modules.setdefault(moduleName, imp.new_module(moduleName))
        mod.__file__    = moduleName
        mod.__loader__  = HashLoader(self.finder)
        mod.__path__    = moduleName
        mod.__package__ = moduleName
        code = compile('', mod.__file__, "exec")
        do_exec(code, mod.__dict__)
        return mod


class SourceHashFinder(object):
    """This module finder looks in the specified hashedSource for the
       indicated module to import and returns an appropriate HashLoader object if
       the module is in that hashedSource.
    """
    def __init__(self, srcHash, decryptor, enczfsrc):
        self.decryptor = decryptor
        self.enczfsrc = enczfsrc
        self.srcHash = srcHash
    def hashRoot(self):
        # All imports that come from a hashedSource will have the
        # hashRoot automatically inserted as the start of the import
        # namespace.  This helps to keep imports from different
        # hashedSource from conflicting or polluting the caller's
        # namespace (which is also a good security measure against
        # malware injection).
        return '{{' + self.srcHash + '}}'
    def _getFromZipFile(self, getter):
        plainsrc = self.decryptor(self.enczfsrc)
        z = ZipFile(BytesIO(plainsrc))
        try:
            return getter(z)
        finally:
            # Try to be hygenic.  This is an interpreted language, but do what we can...
            z.close()
            del z
            # Strings in Python are typically immutable; attempts to
            # modify the string will likely just make more copies, so just
            # tell the interpreter to get rid of the main copy asap.
            del plainsrc
    def getZipNames(self):
        return self._getFromZipFile(lambda z: z.namelist())
    def getZipDirectory(self):
        return self._getFromZipFile(lambda z: z.infolist())
    def withZipElementSource(self, elementname, onSrcFunc):
        return self._getFromZipFile(lambda z: onSrcFunc(z.open(elementname, 'rU').read()))
    def find_module(self, fullname, path=None):
        # The fullname indicates which module is to be loaded.  If
        # this import request comes from a module already in the
        # hashedSource, fullname will usually start with the hashRoot
        # (as will path).

        # If this is an unrelated import, path is either None or an
        # array of strings.  If this import is intended for a
        # hashedSource, then path may begin with the hashRoot
        # specification.  When specifically called for the initial
        # import from a Thespian hashedSource, the path will be
        # explicitly passed in as the hashRoot.

        pkgMark = self.hashRoot()
        if path:
            if not hasattr(path, 'startswith') or not path.startswith(pkgMark): return None
            # Both path and fullname may overlap.   For example:
            #   path     = {{hash}}.foo.bar
            #   fullname = {{hash}}.foo.bar.cow.moo
        skipCnt = len(pkgMark) if fullname.startswith(pkgMark) else 0
        pathname = ospath.join(*tuple(fullname[skipCnt:].split('.')))
        if not pathname:
            return HashRootLoader(self)
        for Z in self.getZipDirectory():
            B,E = ospath.splitext(Z.filename)
            if E == '.py':
                if B == pathname:
                    return HashLoader(self)
                if B == pathname + '/__init__':
                    return HashLoader(self, True)
        return None


def loadModuleFromHashSource(sourceHash, sources, modName, modClass):
    if sourceHash not in sources:
        # specified sourceHash does not exist
        logging.getLogger('Thespian').warning('Specified sourceHash %s is not currently loaded',
                                              sourceHash)
        raise InvalidActorSourceHash(sourceHash)

    for metapath in sys.meta_path:
        if getattr(metapath, 'srcHash', None) == sourceHash:
            return _loadModuleFromVerifiedHashSource(metapath, modName, modClass)

    edata = sources[sourceHash]
    f = SourceHashFinder(sourceHash, lambda v: v, edata)
    sys.meta_path.insert(0, f)
    return _loadModuleFromVerifiedHashSource(f, modName, modClass)


def _loadModuleFromVerifiedHashSource(hashFinder, modName, modClass):
    import thespian.importlib as importlib
    hRoot = hashFinder.hashRoot()
    impModName = modName if modName.startswith(hRoot + '.') else ('.' + modName)
    m = importlib.import_module(impModName, hRoot)
    return getattr(m, modClass)
