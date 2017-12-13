# from distutils.core import setup
from setuptools import setup
import os
from pip.req import parse_requirements
# with open('requirements.txt') as f:
#     required = f.read().splitlines()
# required = ['cython', 'bsddb3', 'numpy', 'credis',
#             'crc16', 'uwsgi', 'mmh3', 'HLL', 'bitarray', 'bitstring', 'redis',
#             'hiredis', 'flask', 'biopython', 'celery', 'psutil']
# dependency_links_list = ['git+git://github.com/Phelimb/atlas-var.git@9aee355b25d0f94b4603eb4bf7e8c47af793f5d4',
#                          'git+git://github.com/Grokzen/redis-py-cluster@9f9baeced12b7abac53b3680ea3f5a1b16a38038',
#                          'git+https://github.com/phelimb/pyseqfile',
#                          "git+git://github.com/timothycrosley/hug.git@e6e85e4e4332fba6d01273b3719c1c11abc644e0"]

install_reqs = parse_requirements('requirements.txt', session=False)
required = [str(ir.req) for ir in install_reqs if ir]
print(required)
from Cython.Build import cythonize

setup(
    name='bigsi',
    version='0.1.6',
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
    install_requires=required,
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
