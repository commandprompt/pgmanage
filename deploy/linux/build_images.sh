#!/bin/bash

cd tarbuild
docker build -t pgmanage/tarbuild .
cd ..

#cd pkgbuild
#docker build -t omnidb/pkgbuild .
#cd ..
