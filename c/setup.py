from distutils.core import setup, Extension

oocmap_module = Extension(
    'oocmap',
    sources=[
        'oocmap.cpp',
        'mdb.c',
        'midl.c',
        'spooky.cpp'
    ],
    extra_compile_args=["-O0", "-g"],    # DEBUG
)

setup(name='oocmap',
    version='0.1',
    description='The out-of-core map',
    author='Dirk Groeneveld',
    author_email='groeneveld@gmail.com',
    url='http://github.com/dirkgr/oocmap',
    ext_modules=[oocmap_module])
