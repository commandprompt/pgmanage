#this script is intended to be run under cygwin + win10 environment
#make sure you have installed NSIS and Resource Hacker software
#run as ./deploy.sh 1.0b, replace the actual version number with the correct one
set -e -e

APP_VERSION="$1"

# if version is not provided, we use last tag from repository
if [ -z "$APP_VERSION" ]
then
    # Fetch all tags from remote repository
    git fetch --all --tags

    # Get last tag version
    APP_VERSION=$(git describe --tags $(git rev-list --tags --max-count=1))
fi

APP_LONG_VERSION=PgManage.$APP_VERSION

echo "making release dir: release_$APP_VERSION"
rm -rf release_$APP_VERSION tmp
mkdir release_$APP_VERSION tmp

# Prepare temporary directory
echo "copying app files into tmp"
cp -R ../../pgmanage/* tmp
cd tmp/
rm -rf pgmanage.db pgmanage.log
touch pgmanage.db

# Install all required libraries
pip3 install -r ../../../requirements.txt
pip3 install pyinstaller

# set up versions in custom_settins.py
echo "setting app version in sources to $APP_VERSION"
sed -i "s/Dev/PgManage $APP_VERSION/" pgmanage/custom_settings.py
sed -i "s/dev/$APP_VERSION/" pgmanage/custom_settings.py

echo "running pyinstaller"
pyinstaller ./pgmanage-win.spec
pyinstaller ./process_executor-win.spec
mv dist/pgmanage-server ../release_$APP_VERSION/
mv dist/process_executor ../release_$APP_VERSION/pgmanage-server/

cd ..
curl -C - https://dl.nwjs.io/v0.69.1/nwjs-v0.69.1-win-x64.zip -o nwjs.zip
unzip -o nwjs.zip -d tmp/

mv tmp/nwjs-v0.69.1-win-x64/* release_$APP_VERSION/
mv release_$APP_VERSION/nw.exe release_$APP_VERSION/pgmanage-app.exe
cp ../app/index.html release_$APP_VERSION
cp ../app/package.json release_$APP_VERSION
cp ../app/pgmanage_icon.png release_$APP_VERSION

sed -i "s/version_placeholder/v$APP_VERSION/" release_$APP_VERSION/index.html
# install resource hacker with default path, then uncomment these lines to replace the default pyinstaller and nwjs exe icons

/cygdrive/c/Program\ Files\ \(x86\)/Resource\ Hacker/ResourceHacker.exe -open release_$APP_VERSION/pgmanage-app.exe -save release_$APP_VERSION/pgmanage-app.exe -action addoverwrite -res ./win-icon.ico -mask ICONGROUP,IDR_MAINFRAME,
/cygdrive/c/Program\ Files\ \(x86\)/Resource\ Hacker/ResourceHacker.exe -open release_$APP_VERSION/pgmanage-server/pgmanage-server.exe -save release_$APP_VERSION/pgmanage-server/pgmanage-server.exe -action addoverwrite -res ./win-icon.ico -mask ICONGROUP,IDR_MAINFRAME,


/cygdrive/c/Program\ Files\ \(x86\)/NSIS/makensisw.exe /DSRCDIR=release_$APP_VERSION install_script.nsi
rm -rf tmp