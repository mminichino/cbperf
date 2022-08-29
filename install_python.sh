#!/bin/bash
#
curl -s -o /var/tmp/Python-3.9.13.tgz https://www.python.org/ftp/python/3.9.13/Python-3.9.13.tgz && \
cd /var/tmp && \
tar xvf Python-3.9.13.tgz && \
cd Python-3.9.13 && \
./configure --enable-optimizations && \
make altinstall

##