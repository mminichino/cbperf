#!/bin/sh
#
SCRIPTDIR=$(cd $(dirname $0) && pwd)
REQUIREMENTS="requirements.txt"

while getopts "p:r:c" opt
do
  case $opt in
    p)
      PYTHON_OPT_BIN=$OPTARG
      ;;
    r)
      REQUIREMENTS=$OPTARG
      ;;
    c)
      for dirname in $(find . -maxdepth 1 -name 'venv[0-9]*')
      do
        echo "Removing $dirname"
        [ -n "$dirname" ] && [ "$dirname" != "." ] && [ "$dirname" != ".." ] && rm -rf $dirname
      done
      exit
      ;;
    \?)
      echo "Invalid Argument"
      exit 1
      ;;
  esac
done

PYTHON_BIN=${PYTHON_OPT_BIN:-python3}

VERSION_STRING=$($PYTHON_BIN -V | sed -n -e 's/^.* \([0-9]*\)\.\([0-9]*\)\.[0-9]*/\1\2/p')

if [ -z "$VERSION_STRING" ]; then
  echo "Can not determine Python release"
  exit 1
fi

echo "Python version: $VERSION_STRING"

VENV_NAME="venv${VERSION_STRING}"

printf "Creating virtual environment... "
$PYTHON_BIN -m venv $SCRIPTDIR/$VENV_NAME
if [ $? -ne 0 ]; then
  echo "Virtual environment setup failed."
  exit 1
fi
echo "Done."

printf "Activating virtual environment... "
. ${SCRIPTDIR:?}/${VENV_NAME:?}/bin/activate
echo "Done."

printf "Installing dependencies... "
$PYTHON_BIN -m pip install --upgrade pip setuptools wheel >> setup.log 2>&1
pip3 install --no-cache-dir -r $REQUIREMENTS >> setup.log 2>&1
if [ $? -ne 0 ]; then
  echo "Setup failed."
  rm -rf ${SCRIPTDIR:?}/${VENV_NAME:?}
  exit 1
else
  echo "Done."
  echo "Setup successful."
fi
