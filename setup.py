#from distutils.core import setup
from setuptools import setup
import os
with open('requirements.txt') as f:
    required = f.read().splitlines()
from Cython.Build import cythonize

setup(
    name='atlasseq',
    version='0.0.1',
    packages=[
        'atlasseq',
        'atlasseq.cmds',
        'atlasseq.graph',
        'atlasseq.graph',
        'atlasseq.sketch',
        'atlasseq.storage',
        'atlasseq.storage.graph',
    ],
    license='MIT',
    url='http://github.com/phelimb/atlasseq',
    description='.',
    author='Phelim Bradley',
    author_email='wave@phel.im',
    ext_modules=cythonize("atlasseq/utils.pyx"),
    install_requires=required,
    entry_points={
        'console_scripts': [
            'atlasseq = atlasseq.main:main',
        ]})
