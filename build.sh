#!/bin/sh

sudo rm -rf dist build
sudo pip uninstall -y couchtools
sudo python setup.py install
