#!/bin/bash
if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` version-string"
    exit -1
fi

VERSION=$1
#Check version here and abort if not there
BUILD_TARGET=/tmp
DIST_NAME=voltdb-client-python-$1
echo $DIST_NAME `git describe --long`   > .kit_version
echo "Making kit $BUILD_TARGET/$DIST_NAME.tar.gz"
THISDIR=${PWD##*/}
cd ..
ln -sf $THISDIR $DIST_NAME
tar cvzfh $BUILD_TARGET/$DIST_NAME.tar.gz $DIST_NAME   --exclude=.git --exclude=make_kit.sh --exclude=dist --exclude=voltdbclient.egg-info
rm -f $DIST_NAME
