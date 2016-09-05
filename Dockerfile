FROM python:3.4.3-onbuild
RUN python setup.py build_ext --inplace
#RUN python setup.py install
#CMD remcdbg --help