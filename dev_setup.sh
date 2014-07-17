#!/bin/sh
PREFIX="dev_setup.sh:"
echo "$PREFIX Instaling virtualenv"
sudo pip install virtualenv
echo "$PREFIX Creating virtualenv for project"
virtualenv --no-site-packages --python=python2.7 env
echo "$PREFIX Installing project's python dependencies"
. env/bin/activate
pip install -r requirements.txt
