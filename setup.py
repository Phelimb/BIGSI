# from distutils.core import setup
from setuptools import setup
import os
with open('requirements.txt') as f:
    required = f.read().splitlines()
from Cython.Build import cythonize

setup(
    name='bfg',
    version='0.0.1',
    packages=[
        'bfg',
        'bfg.cmds',
        'bfg.utils',
        'bfg.graph',
        'bfg.sketch',
        'bfg.tasks',
        'bfg.storage',
        'bfg.storage.graph',
    ],
    license='MIT',
    url='http://github.com/phelimb/bfg',
    description='.',
    author='Phelim Bradley',
    author_email='wave@phel.im',
    ext_modules=cythonize("bfg/utils/fncts.pyx"),
    install_requires=required[3:],
    entry_points={
        'console_scripts': [
            'bfg = bfg.__main__:main',
        ]})
