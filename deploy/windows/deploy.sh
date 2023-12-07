#this script is intended to be run under cygwin + win10 environment
#make sure you have installed NSIS and Resource Hacker software
#run as ./deploy.sh 1.0b, replace the actual version number with the correct one
set -e -e

APP_VERSION="$1"
REPO="https://github.com/commandprompt/pgmanage"
BRANCH="master"
DEPLOY_DIR=$(pwd)
TEMP_DIR=$DEPLOY_DIR/tmp

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
rm -rf release_$APP_VERSION $TEMP_DIR
mkdir release_$APP_VERSION $TEMP_DIR

cd $TEMP_DIR

echo "Cloning repository $REPO"
git clone $REPO --depth 1 -b $BRANCH .

echo "creating virtual env"
python -m venv venv

dos2unix venv/Scripts/activate
source venv/Scripts/activate

# Install all required libraries
echo "installing python dependencies"
pip3 install -r requirements.txt
pip3 install pyinstaller==5.13.0

cd pgmanage/

# set up versions in custom_settins.py
echo "setting app version in sources to $APP_VERSION"
sed -i "s/Dev/PgManage $APP_VERSION/" pgmanage/custom_settings.py
sed -i "s/dev/$APP_VERSION/" pgmanage/custom_settings.py

# building vite bundle
cd app/static/assets/js/pgmanage_frontend/

echo "installing javascript dependencies"
npm install

echo "building vite bundle"
npm run build

cd $TEMP_DIR/pgmanage

echo "running pyinstaller"
touch pgmanage.db
pyinstaller ./pgmanage-win.spec
pyinstaller ./process_executor-win.spec
mv dist/pgmanage-server $DEPLOY_DIR/release_$APP_VERSION/
mv dist/process_executor $DEPLOY_DIR/release_$APP_VERSION/pgmanage-server/

cd $DEPLOY_DIR
curl -C - https://dl.nwjs.io/v0.69.1/nwjs-v0.69.1-win-x64.zip -o nwjs.zip
unzip -o nwjs.zip -d $TEMP_DIR/

mv $TEMP_DIR/nwjs-v0.69.1-win-x64/* release_$APP_VERSION/
mv release_$APP_VERSION/nw.exe release_$APP_VERSION/pgmanage-app.exe
cp $TEMP_DIR/deploy/app/index.html release_$APP_VERSION
cp $TEMP_DIR/deploy/app/package.json release_$APP_VERSION
cp $TEMP_DIR/deploy/app/pgmanage_icon.png release_$APP_VERSION

sed -i "s/version_placeholder/v$APP_VERSION/" release_$APP_VERSION/index.html
# install resource hacker with default path, then uncomment these lines to replace the default pyinstaller and nwjs exe icons

/cygdrive/c/Program\ Files\ \(x86\)/Resource\ Hacker/ResourceHacker.exe -open release_$APP_VERSION/pgmanage-app.exe -save release_$APP_VERSION/pgmanage-app.exe -action addoverwrite -res ./win-icon.ico -mask ICONGROUP,IDR_MAINFRAME,
/cygdrive/c/Program\ Files\ \(x86\)/Resource\ Hacker/ResourceHacker.exe -open release_$APP_VERSION/pgmanage-server/pgmanage-server.exe -save release_$APP_VERSION/pgmanage-server/pgmanage-server.exe -action addoverwrite -res ./win-icon.ico -mask ICONGROUP,IDR_MAINFRAME,


/cygdrive/c/Program\ Files\ \(x86\)/NSIS/makensisw.exe /DSRCDIR=release_$APP_VERSION install_script.nsi

echo "deactivating virtual env and cleaning temp folder"

# deactivate virtualenv
deactivate

# clean temp folder
rm -rf $TEMP_DIR