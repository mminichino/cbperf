#!/bin/bash
#

config_linux_by_type () {
  . /etc/os-release
  export LINUXTYPE=$ID
  case $ID in
  centos|rhel)
    if [ "$VERSION_ID" = "7" ]; then
      if [ ! -f /usr/lib64/openssl11/libcrypto.so ]; then
        echo "Error: Install openssl11 packages."
        exit 1
      fi
      CWD=$(pwd)
      mkdir /usr/local/openssl11
      cd /usr/local/openssl11
      ln -s /usr/lib64/openssl11 lib
      ln -s /usr/include/openssl11 include
      cd $CWD
      ./configure --enable-optimizations --with-openssl=/usr/local/openssl11
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