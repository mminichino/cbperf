#!/bin/bash
#

config_linux_by_type () {
  . /etc/os-release
  export LINUXTYPE=$ID
  case $ID in
  centos|rhel)
    if [ "$VERSION_ID" = "7" ]; then
      CWD=$(pwd)
      curl -s -o /var/tmp/openssl-1.1.1q.tar.gz https://www.openssl.org/source/openssl-1.1.1q.tar.gz
      cd /var/tmp
      tar xzf openssl-1.1.1q.tar.gz
      cd openssl-1.1.1q
      ./config --prefix=/usr/local/openssl --openssldir=/usr/local/openssl --libdir=/lib64 shared zlib-dynamic
      make -j4
      make install
      cd $CWD
      ./configure --enable-optimizations --with-openssl=/usr/local/openssl
    fi
    ;;
  ubuntu)
    ./configure --enable-optimizations
    ;;
  *)
    echo "Unknown Linux distribution $ID"
    exit 1
    ;;
  esac
}

curl -s -o /var/tmp/Python-3.9.13.tgz https://www.python.org/ftp/python/3.9.13/Python-3.9.13.tgz && \
cd /var/tmp && \
tar xvf Python-3.9.13.tgz && \
cd Python-3.9.13 && \
config_linux_by_type && \
make altinstall

##