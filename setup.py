from distutils.core import setup, Extension

# The next line is touched automatically by the release process.
VERSION = '0.1'

oocmap_module = Extension(
    'oocmap',
    sources=[
        'module.cpp',
        'oocmap.cpp',
        'lazytuple.cpp',
        'lazylist.cpp',
        'lazydict.cpp',
        'errors.cpp',
        'db.cpp',
        'mdb.c',
        'midl.c',
        'spooky.cpp'
    ],
    #extra_compile_args=["-O0", "-g"],    # DEBUG
)

setup(name='oocmap',
    version=VERSION,
    description='The out-of-core map',
    author='Dirk Groeneveld',
    author_email='dirkg@allenai.org',
    url='http://github.com/allenai/oocmap',
    ext_modules=[oocmap_module])
