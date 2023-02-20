# PgManage
We proudly leverage all of the great work Open Source work done by the original
(now dormant) project https://github.com/OmniDB/OmniDB 


**Website & Downloads**: https://github.com/commandprompt/pgmanage

**Full Documentation**: https://pgmanage.readthedocs.io/en/latest/

# Run it yourself from source
If you want or need a new feature, submit a pull request for us to consider.

## Pre-req's for Debian/Ubuntu
```
sudo apt install python3-dev python3-venv python3-wheel libpq-dev libldap2-dev libsasl2-dev
```

## Pre-req's for Enterprise Linux flavors
```
sudo yum install xxx yyy zzz
```

## Pre-req's for OSX using Homebrew
```
brew install python3 xxx yyy zzz
```

## Build instructions
```
git clone https://github.com/commandprompt/pgmanage
cd pgmanage
env PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install 3.8.10 --skip-existing
pyenv local 3.8.10
poetry env use 3.8
poetry install --with dev
poetry shell
```

## Start it up
```
cd pgmanage/pgmanage
python3 pgmanage-server.py
```

# PgManage 1.0Alpha

## Release Date: *pending*

## Release Notes

- New features:
  - new explain/analyze UI powered by pev2, including pev2 dark theme support
  - connection credential encryption
  - backported support for monitoring data-grid-based monitoring widgets
  - backported pie charts widgets for numbackends and database sizes
  - added password strength validation for user and master passwords
  - PostgreSQL 9.6, 10, 11, 12, 13, 14 and 15 support
  -
- Major Bugs fixed:
  - fixed data export to csv/xls format in the desktop version of the app
  - added superuser permission check on all user managepent APIs
  - extra validations added to prevent creation of unnamed connection groups
  - fixed extenal links not working in the desktop variant of the app
  - fixed posgres special commands on postgresql versions 12 and higher
  - fixed broken postgres documentation links available in database tree view menus
  - made all web/cdn app dependencies local so pgmanage can work properly without an internet connection

- UI/UX Improvements:
  - reorganized connection management menus in the left menu bar 
  - fixed ddl tab auto resizing
  - the top-right utilities menu now expands on click instead of mouse-hover
  - added ddl/properties tab resize limits to prevent it from becoming impossible to grab/resize back
  - unified tooltip appearance throughout the whole app
  - unified pictogram look and feed thoughout the whole app
  - improved database tree view navigation by adding smooth scroll to the newly expanded tree node. previously when some tree view node was expanded it jumped out of sight
  - improved data grid/table readability
  - improved database entity tree view readability
  - fixed date formatting in sql command history grid
  - fixed date formatting in db console command history grid
  - proper styling for dialog primary and secondary buttons. the secondary buttons in forms and dialogs were previously looked disable/grayed-out which was confusing.
  - the autocommit checkbox on query tab now stays visible despite of application window size
removed the option to make connections public in desktop variant of the app (which has only one user so shared/public connections make no sense)

- Other changes
  - added postgresql 14 and 15  support
  - application data directory and db/log file naming was changed from omnidb* to pgmanage*.

![](https://raw.githubusercontent.com/OmniDB/doc/master/img/omnidb_3/dashboard.png)
