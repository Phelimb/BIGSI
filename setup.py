#from distutils.core import setup
from setuptools import setup

from Cython.Build import cythonize

setup(
    name='atlasseq',
    version='0.0.1',
    packages=[
        'atlasseq',
        'atlasseq.cmds'
    ],
    license='MIT',
    url='http://github.com/phelimb/atlasseq',
    description='.',
    author='Phelim Bradley',
    author_email='wave@phel.im',
    ext_modules=cythonize("atlasseq/utils.pyx"),
    install_requires=[
            'redis',
            'hiredis'],
    entry_points={
        'console_scripts': [
            'atlasseq = atlasseq.main:main',
        ]})
