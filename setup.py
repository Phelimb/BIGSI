#from distutils.core import setup
from setuptools import setup

from Cython.Build import cythonize

setup(
    name='remcdbg',
    version='0.1',
    packages=[
        'remcdbg',
        'remcdbg.cmds'
    ],
    license='MIT',
    url='http://github.com/phelimb/remcdbg',
    description='.',
    author='Phelim Bradley',
    author_email='wave@phel.im',
    ext_modules=cythonize("remcdbg/utils.pyx"),
    install_requires=[
            'redis',
            'hiredis'],
    entry_points={
        'console_scripts': [
            'remcdbg = remcdbg.main:main',
        ]})
