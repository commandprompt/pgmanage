# Building PGManage server and desktop app

The deploy subdir of this projec has a number of helper scripts which should be used to build the release archive.

Here is the list of steps necessary involved in the build process:
cd ./depploy/linux
run ./build_images.sh
once build_images.sh completes run./deploy.sh

The build process takes about 2 minutes to complete. Once deploy.sh completes the current directory should contain pgmanage-app and pgmanage-server tar.gz archives
