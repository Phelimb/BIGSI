# from distutils.core import setup
from setuptools import setup
import os
with open('requirements.txt') as f:
    required = f.read().splitlines()
from Cython.Build import cythonize

setup(
    name='cbg',
    version='0.0.1',
    packages=[
        'cbg',
        'cbg.cmds',
        'cbg.utils',
        'cbg.graph',
        'cbg.sketch',
        'cbg.tasks',
        'cbg.storage',
        'cbg.storage.graph',
    ],
    license='MIT',
    url='http://github.com/phelimb/cbg',
    description='.',
    author='Phelim Bradley',
    author_email='wave@phel.im',
    ext_modules=cythonize("cbg/utils/fncts.pyx"),
    install_requires=required[3:],
    entry_points={
        'console_scripts': [
            'cbg = cbg.__main__:main',
        ]})
