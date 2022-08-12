from distutils.core import setup, Extension

# The next line is touched automatically by the release process.
VERSION = '0.3'

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
    long_description="PyPi description upload is broken. Please go to GitHub to see the description.",
    long_description_content_type="text/text",
    author='Dirk Groeneveld',
    author_email='dirkg@allenai.org',
    project_urls={
      'Source': 'http://github.com/allenai/oocmap'
    },
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Intended Audience :: Developers",
        "Programming Language :: Python"
    ],
    url='http://github.com/allenai/oocmap',
    ext_modules=[oocmap_module])
