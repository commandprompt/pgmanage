#!/bin/bash

# Checking environment
echo "REPO=$REPO"
echo "BRANCH=$BRANCH"
echo "VERSION=$VERSION"

# Cloning repo
git clone $REPO --depth 1 -b $BRANCH OmniDB

# Installing dependencies
cd OmniDB/
pip3 install -r requirements.txt

# Building server
cd OmniDB/
rm -f pgmanage.db pgmanage.log
touch pgmanage.db
pyinstaller OmniDB-lin.spec
mv dist/omnidb-server $HOME
rm -rf build dist
cd $HOME
mkdir pgmanage-server_$VERSION
cp omnidb-server pgmanage-server_$VERSION/
tar -czvf pgmanage-server_$VERSION.tar.gz pgmanage-server_$VERSION/
mv pgmanage-server_$VERSION.tar.gz /tmp/

# Building app
curl -LO https://dl.nwjs.io/v0.69.1/nwjs-v0.69.1-linux-x64.tar.gz
tar -xzvf nwjs-v0.69.1-linux-x64.tar.gz
mv nwjs-v0.69.1-linux-x64 pgmanage-app_$VERSION
cd pgmanage-app_$VERSION
rm ./lib/libEGL.so
rm ./lib/libGLESv2.so
rm ./lib/libvulkan.so.1
rm ./lib/libvk_swiftshader.so
rm ./lib/*.json
mkdir pgmanage-server
cp $HOME/omnidb-server ./pgmanage-server/
cp $HOME/OmniDB/deploy/app/* .
mv nw omnidb-app
cd $HOME
tar -czvf pgmanage-app_$VERSION.tar.gz pgmanage-app_$VERSION/
mv pgmanage-app_$VERSION.tar.gz /tmp/
