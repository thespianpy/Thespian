"""The sourceLoader is used to handle the import capabilities for hash-identified loaded sources.

Sources are loaded via the ActorSystem().loadSource(...) operation,
verified by the SourceAuthority, then made available for creating
actors in them via createActor("actorname", sourceHash=H).

1. The Actor generated from the loaded source may perform various
   imports or create other actors; those imports should be realized
   from within the loaded sources or from general packages, but not
   from other loaded sources (unless specifically identified by a
   createActor using a different source hash).

2. The Actor may exist in the current process.

3. Even if the Actor is created in a separate process, the Admin is
   likely to load the source containing the Actor in order to check
   the capabilities requirements for that Actor.

4. New versions of the source may be loaded; those new versions should
   exist independently of the old versions.

To accomplish the above, an importlib metapath Finder and Loader are
created.  These help to decrypt and load the requested sources on
demand.

To accomplish #4, #3, and #2, the Finder and Loader will ensure that
all modules loaded from the hashed source are marked as belonging to a
package identified by the hash.  This prevents the loaded sources from
leaking out into global namespace.

To accomplish #1 for both relative and absolute imports, the Python
AST package is used to modify the import statements in the loaded
source to implicitly specify this package (identified by hash).

Also note that there have been changes in the import machinery for
Python across several versions:

   * 2.6 -> 2.7 changes
   * 2.7 -> 3.1 changes
   * 3.2 -> 3.3 changes
   * 3.3 -> 3.4 changes

The code below supports all versions: 2.6, 2.7, 3.2, 3.3, 3.4, and 3.5
(and probably subsequent versions, unless the import machinery changes
again).

"""

try:
    from cStringIO import StringIO
except ImportError:
    try:
        from StringIO import StringIO
    except ImportError:
        from io import StringIO
from io import BytesIO
from zipfile import ZipFile
try:
    from zipfile import BadZipFile
except ImportError:
    from zipfile import BadZipfile
    BadZipFile = BadZipfile
import posixpath as ospath # because zip only uses posix notation
import logging
import imp
import ast
import sys
from thespian.actors import InvalidActorSourceHash
from thespian.system.utilis import thesplog


HashModuleName = lambda h,s: s

if sys.version_info < (2,7):
    import thespian.importlib as importlib
else:
    import importlib
if sys.version_info >= (3,1):
    import importlib.abc
    LoaderBase = importlib.abc.Loader
    FinderBase = importlib.abc.MetaPathFinder
    if sys.version_info >= (3,4):
        from importlib.machinery import ModuleSpec
        hmn = lambda hashMark, name: HashModuleName(hashMark, '' if hashMark == name else name)
        RootLoader = lambda fullname, finder, mark: ModuleSpec(fullname, HashRootLoader(finder), origin=mark, is_package=True)
        ModLoader = lambda fullname, finder, mark, isPkg: ModuleSpec(hmn(mark, fullname), HashLoader(finder, isPkg), origin=mark, is_package=isPkg)
    else:
        class ReprLoaderBase(importlib.abc.Loader):
            def module_repr(self, module):
                return '<module %s>'%module.__name__
        LoaderBase = ReprLoaderBase
        RootLoader = lambda fullname, finder, mark: HashRootLoader(finder)
        ModLoader = lambda fullname, finder, mark, isPkg: HashLoader(finder, isPkg)
else:
    LoaderBase = object
    FinderBase = object
    RootLoader = lambda fullname, finder, mark: HashRootLoader(finder)
    ModLoader = lambda fullname, finder, mark, isPkg: HashLoader(finder, isPkg)


if sys.version_info >= (3,0):
    exec("def do_exec(co, loc): exec(co, loc)\n")
else:
    exec("def do_exec(co, loc): exec co in loc\n")


class ImportRePackage(ast.NodeTransformer):
    def __init__(self, sourceHashDot, topnames):
        self._sourceHashDot = sourceHashDot
        self._topnames = [ospath.splitext(N)[0] for N in topnames]
    def visit_Import(self, node):  # Import(alias* names)
        newnames = []
        for A in node.names:
            firstName = A.name.partition('.')[0]
            if firstName in self._topnames:
                if A.asname is None:
                    # Normally "import foo.bar.bang" will cause foo to
                    # be added to globals.  This code converts "import
                    # x.y.z" to "import hash.x as x; import
                    # hash.x.y.z" to effect the same thing.
                    newnames.append(ast.copy_location(
                        ast.alias(self._sourceHashDot + firstName, firstName), A))
                    newnames.append(ast.copy_location(
                        ast.alias(self._sourceHashDot + A.name, None), A))
                else:
                    newnames.append(ast.copy_location(ast.alias(self._sourceHashDot + A.name, A.asname), A))
            else:
                newnames.append(A)
        return ast.copy_location(ast.Import(newnames), node)
    def visit_ImportFrom(self, node):  # ImportFrom(identifier? module, alias* names, int? level)
        modname = (self._sourceHashDot + node.module) \
                  if node.level == 0 and node.module and node.module.partition('.')[0] in self._topnames \
                  else node.module
        return ast.copy_location(ast.ImportFrom(modname, node.names, node.level), node)


def fix_imports(sourceCode, filename, sourceHashDot, toplevel):
    tree = ast.parse(sourceCode, filename)
    fixTree = ImportRePackage(sourceHashDot, toplevel).visit(tree)
    return compile(fixTree, filename, 'exec')


class HashLoader(LoaderBase):
    def __init__(self, finder, isModuleDir=False):
        self.finder = finder
        self.isModuleDir = isModuleDir

    def create_module(self, spec):
        # spec.name is what the module is registered under in sys.modules
        mod = sys.modules.setdefault(spec.name, imp.new_module(spec.name))
        mod.__file__ = spec.name
        mod.__loader__ = self
        if self.isModuleDir:
            mod.__path__ = []
            mod.__package__ = (spec.name if spec.name.startswith(spec.origin) else spec.origin + '.' + spec.name)
        else:
            pkgname = spec.name.rpartition('.')[0]
            if not pkgname.startswith(spec.origin):
                if not pkgname:
                    pkgname = spec.origin
                else:
                    pkgname = spec.origin + pkgname
            mod.__package__ = pkgname
        return mod

    def exec_module(self, module):
        moduleName = module.__name__
        hashRoot = self.finder.hashRoot()
        if moduleName.startswith(hashRoot):
            moduleName = moduleName[len(hashRoot):]
        if self.isModuleDir:
            name = ospath.join(*tuple(moduleName.split('.') + ['__init__.py']))
        elif '.' in moduleName:
            name = ospath.join(*tuple(moduleName.split('.'))) + '.py'
        else:
            name = moduleName + '.py'
        codeproc = lambda s: fix_imports(s, name, hashRoot, self.finder.getZipTopLevelNames())
        try:
            # Ensure the file ends in a carriage-return.  The path
            # importer does this automatically and no trailing
            # whitespace results in SyntaxError or IndentError
            # exceptions.  In addition, using "universal newlines"
            # mode to read the file is not always effective
            # (e.g. ntlm.HTTPNtlmAuthHandler.py, so explicitly ensure
            # the proper line endings for the compiler.
            if sys.version_info >= (3,0):
                converter = lambda s: codeproc(s + b'\n')
            else:
                converter = lambda s: codeproc(s.replace('\r\n', '\n')+'\n')
            code = self.finder.withZipElementSource(
                name,
                converter)
            do_exec(code, module.__dict__)
        except Exception as ex:
            thesplog('sourceload realization failure in %s: %s',
                     moduleName, ex, level=logging.ERROR)
            #return None
            raise

    def load_module(self, moduleName):
        hashRoot = self.finder.hashRoot()
        if not moduleName.startswith(self.finder.hashRoot()) and \
           moduleName != 'six':   # six has it's own loader; adding the hashRoot will confound it
            moduleName = hashRoot + moduleName
        if moduleName in sys.modules:
            return sys.modules[moduleName]
        mod = sys.modules.setdefault(moduleName, imp.new_module(moduleName))
        mod.__file__ = moduleName
        mod.__loader__ = self
        if self.isModuleDir:
            mod.__path__ = []
            mod.__package__ = moduleName
        else:
            mod.__package__ = moduleName.rpartition('.')[0]
        try:
            self.exec_module(mod)
        except Exception as ex:
            del sys.modules[moduleName]
            raise
        return mod


class HashRootLoader(LoaderBase):
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
        mod.__loader__  = self
        mod.__path__    = moduleName
        mod.__package__ = '{{RPKG}}' + moduleName
        code = compile('', mod.__file__, "exec")
        do_exec(code, mod.__dict__)
        return mod


class SourceHashFinder(FinderBase):
    """This module finder looks in the specified hashedSource for the
       indicated module to import and returns an appropriate HashLoader object if
       the module is in that hashedSource.
    """
    def __init__(self, srcHash, decryptor, enczfsrc):
        self.decryptor = decryptor
        self.enczfsrc = enczfsrc
        self.srcHash = srcHash
        super(SourceHashFinder, self).__init__()
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
        try:
            z = ZipFile(BytesIO(plainsrc))
        except BadZipFile as ex:
            logging.error('Invalid zip contents (%s) for source hash %s: %s',
                          str(plainsrc) if not plainsrc or len(plainsrc) < 100
                          else str(plainsrc[:97]) + '...',
                          self.srcHash, ex)
            raise
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
    def getZipTopLevelNames(self):
        return set([N.partition('/')[0] for N in self.getZipNames() if N != '__init__.py'])
    def getZipDirectory(self):
        return self._getFromZipFile(lambda z: z.infolist())
    def withZipElementSource(self, elementname, onSrcFunc):
        return self._getFromZipFile(lambda z: onSrcFunc(z.open(elementname, 'rU').read()))
    def find_spec(self, fullname, path=None, target=None):
        try:
            return self.find_module(fullname, path)
        except BadZipFile as ex:
            raise ImportError('Source hash %s: %s'%(self.srcHash, str(ex)))
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
            #   path     = {{hash}}foo.bar
            #   fullname = {{hash}}foo.bar.cow.moo
        skipCnt = len(pkgMark) if fullname.startswith(pkgMark) else 0
        pathname = ospath.join(*tuple(fullname[skipCnt:].split('.')))
        if not pathname:
            return RootLoader(fullname, self, pkgMark)
        for Z in self.getZipDirectory():
            B,E = ospath.splitext(Z.filename)
            if E == '.py':
                if B == pathname:
                    return ModLoader(fullname, self, pkgMark, False)
                if B == pathname + '/__init__':
                    myname = fullname if fullname.startswith(pkgMark) else (pkgMark + fullname)
                    return ModLoader(myname, self, pkgMark, True)
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
    hRoot = hashFinder.hashRoot()
    pkg = importlib.import_module(hRoot)
    #impModName = modName if modName.startswith(hRoot + '.') else (hRoot + '.' + modName)
    impModName = modName if modName.startswith(hRoot) else (hRoot + modName)
    try:
        m = importlib.import_module(impModName, hRoot)
    except (BadZipFile, SyntaxError) as ex:
        raise ImportError('Source hash %s: %s'%(hRoot, str(ex)))
    return getattr(m, modClass)
