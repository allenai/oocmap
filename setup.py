from distutils.core import setup, Extension

# The next line is touched automatically by the release process.
VERSION = '0.2'

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
    description='A file-backed dictionary for Python',
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author='Dirk Groeneveld',
    author_email='dirkg@allenai.org',
    license="Apache License 2.0",
    project_urls={
      'Source': 'http://github.com/allenai/oocmap'
    },
    url='http://github.com/allenai/oocmap',
    ext_modules=[oocmap_module])
