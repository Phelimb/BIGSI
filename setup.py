# from distutils.core import setup
from setuptools import setup
import os


setup(
    name="bigsi",
    version="0.3.1",
    packages=[
        "bigsi",
        "bigsi.bloom",
        "bigsi.cmds",
        "bigsi.utils",
        "bigsi.graph",
        "bigsi.storage",
        "bigsi.matrix",
        "bigsi.scoring",
        "bigsi.tests",
    ],
    keywords="DBG coloured de bruijn graphs sequence search signture files signature index bitsliced",
    license="MIT",
    url="http://github.com/phelimb/bigsi",
    description="BItsliced Genomic Signature Index - Efficient indexing and search in very large collections of WGS data",
    author="Phelim Bradley",
    author_email="wave@phel.im",
    install_requires=[
        "cython",
        "hug",
        "numpy",
        "mmh3",
        "bitarray",
        "redis",
        "biopython",
        "pyyaml",
        "humanfriendly",
    ],
    entry_points={"console_scripts": ["bigsi = bigsi.__main__:main"]},
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        "Development Status :: 3 - Alpha",
        # Pick your license as you wish (should match "license" above)
        "License :: OSI Approved :: MIT License",
        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
    ],
)
