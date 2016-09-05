FROM python:3.4.3-onbuild
RUN python setup.py install
CMD remcdbg --help