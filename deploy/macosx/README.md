# PgManage macOS Build

The following software should be installed prior to running the build script:

Official Python 3.9.* macOs release

- Node.js
- npm

## Building
1. Simply run in command line deploy script:
```
./deploy.sh [pgmanage_version] [branch_name]
```
- `pgmanage_version`(*optional*): The version number that will be added to bundle and `.dmg` file.
If not provided, the script defaults to the last version tag from Git repository.
- `branch_name`(*optional*): The Git branch to build from.If not provided, the script defaults to the `master` branch.

This will create the python virtual environment and install all the required python modules mentioned in the requirements file using pip, build the runtime code and finally create the app bundle and the DMG in `./release_$pgmanage_version` directory.