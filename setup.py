# from distutils.core import setup
from setuptools import setup
import os
with open('requirements.txt') as f:
    required = f.read().splitlines()
from Cython.Build import cythonize

setup(
    name='bigsi',
    version='0.1.6.1',
    packages=[
        'bigsi',
        'bigsi.cmds',
        'bigsi.utils',
        'bigsi.graph',
        'bigsi.sketch',
        'bigsi.tasks',
        'bigsi.storage',
        'bigsi.matrix',
        'bigsi.scoring',
        'bigsi.variants',
        'bigsi.storage.graph',
    ],
    keywords='DBG coloured de bruijn graphs sequence search signture files signature index bitsliced',
    license='MIT',
    url='http://github.com/phelimb/bigsi',
    description='BItsliced Genomic Signature Index - Efficient indexing and search in very large collections of WGS data',
    author='Phelim Bradley',
    author_email='wave@phel.im',
    install_requires=required[4:],
    entry_points={
        'console_scripts': [
            'bigsi = bigsi.__main__:main',
        ]},
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # Pick your license as you wish (should match "license" above)
        'License :: OSI Approved :: MIT License',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ]
)
