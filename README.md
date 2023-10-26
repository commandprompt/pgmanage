# PgManage
PgManage is a Postgres management GUI, though we will be adding features for
developers the initial focus is on the "management" of Postgres.

We proudly leverage all of the great work Open Source work done by the original
(now dormant) project https://github.com/OmniDB/OmniDB


**Website & Downloads**: https://www.commandprompt.com/products/pgmanage/

**Full Documentation**: https://pgmanage.readthedocs.io/en/latest/

# Run your local development copy of PgManage on Ubuntu

## Install the necessary packages
```
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt-get update
sudo apt-get install git libssl-dev python-protobuf build-essential
sudo apt-get install python3.9 python3.9-dev python3.9-venv  python3-wheel libpq-dev libldap2-dev libsasl2-dev
```

## Set up app environment
clone pgmanage repository; change to the root directory of cloned repository,  activate virtualenv:
```
git clone https://github.com/commandprompt/pgmanage.git
cd pgmanage
python3.9 -mvenv .env
```
## Install Dependencies and Run the App

1. Activate the Python virtual environment created in the previous step:
    ```bash
    source .env/bin/activate
    ```

2. Install Python dependencies using pip:
    ```bash
    pip install -r requirements.txt
    ```

3. Navigate to the pgmanage/app/static/assets/js/pgmanage_frontend/ directory and install Node.js dependencies using npm:
    ```bash
    cd pgmanage/app/static/assets/js/pgmanage_frontend/
    npm install
    ```

4. Start a development front-end server:
    ```bash
    npm run dev
    ```

5. Once all app requirements are installed, navigate to the pgmanage subdirectory and start the application web service by running:
    ```bash
    ./manage.py runserver
    ```

6. When you see that the application server is ready, open http://localhost:8000 in your preferred web browser.

7. Login using the following credentials:
    - Username: admin
    - Password: admin

# PgManage 1.0 Beta 3

## Release Date: Nov 4 2023

## Release Notes

 - New features:
   - added UI for creating/altering DB tables (currently for Postgres only)
   - added new Entity Relationship Diagram for all supported databases
   - added PIGZ support for database backup and restore
   - added UI for PG Cron extension

 - Major Bugs fixed:
   - fixed the issue when "Test Connection" action fails on previously saved DB connection
   - fixed SQL autocomplete issues

 - UI/UX Improvements:
   - default TCP port in database connection form is now prepopulated based on selected database type
   - improved styling for Pev2 Query Explain component
   - major dark theme improvements
   - the data editor tab is rewritten in Vuejs with various UX improvements like revert changed, display number of changes made etc
   - the state of autocomplete toggle switch is now saved to application settings
   - in DB Query tab the Cancel Query button is now displayed for long running queries only (>1000ms)
   - various layout improvements on DB Query tab, application pane separators etc.
   - minimized UI visual clutter
  
 - Other changes
   - database object tree was fully rewritten in Vuejs
   - moved SQL formatting/indentation to front-end
   - refactored DB Object APIs
   - JS assets are now managed with NPM and bundled with Vite
   - Long-polling code cleaned up and refactored
   - DB console tab was fully rewritten in Vuejs
   - DB query tab was fully rewritten in Vuejs


# PgManage 1.0 Beta 2

## Release Date: Jun 15 2023

## Release Notes

 - New features:
   - ability to disable CSV header when exporting data grid contents
   - added UI for Postgres extension management
   - new hierarchical connections menu
   - use random TCP port number for the application back-end process so Pgmanage does not occupy ports commonly used by other applications
   - ability to select SSL connection options in Connection Management dialog
   - remember and restore application window position and size when the app starts
   - added configurable date/time display format in the application settings dialog
   - restore the last used database and query tabs when pgmanage starts

 - Major Bugs fixed:
   - if the query entered by the user contains explain keyword, clicking on explain/analyze button will no longer prepend the query with an extra explain keyword (previously this bug resulted in syntactically incorrect query)
   
 - UI/UX Improvements:
    - ability to work with multiple databases within a DB session without needing to select the "active" database
    - if query entered by the user contains explain keyword, the explain tab will be opened automatically when user clicks the "Run query" button
    - explain and analyze buttons are now grouped together and separated from other query buttons
    - pre-set database connection TCP port in the Connection Management dialog based on selected database type
    - add visually matching themes for query editor
  
 - Other changes
    - django has been updated from 2.2 to 3.2
    - code clean-up and refactoring
    - moved application shared data into globally accessible Pinia store
    - replace cx_Oracle library with oracledb
    
# PgManage 1.0 Beta

## Release Date: Apr 20 2023

## Release Notes

- New features:
  - added backup/restore support for Postgres
  - first version of PgManage Handbook was published to https://pgmanage.readthedocs.io/en/latest

- Major Bugs fixed:
  - fixed .AppImage compatibility issues for newer Linux distributions which do not have libcrypt installed
  - added logic to terminate stale back-end process if the front-end process crashes
  - fixed application UI process memory leaks


- UI/UX Improvements:
  - improved support for configuration options search in Postgres Server Configuration Management
  - automatically readjust query editor font size when the application font size changes
  - various application layout and UI improvements
  - limited minimum application window size to 1024x766
  - fixed splash screen flickering/position issues during the application startup
  - add PgManage Handbook links to application error modal dialogs
  - improved handling of drag-and-drop reordering for database operations tabs

- Other changes
  - added support for configurable Postgresql Client binary path in application settings
  - excluded SASS libraries and .sass files from the release builds
  - include EGL/GLES libraries into app release builds
  - pev2 upgraded to v1.7.0
  - removed "plugins" and other obsolete menu items from the application UI
  - removed unused files and dead code from the project
  - shred SSH keys stored in the app during the Master Password Reset

# PgManage 1.0 Alpha

## Release Date: Feb 21 2023

## Release Notes

- New features:
  - new connection management UI
  - added support for postgres server configuration management
  - new explain/analyze UI powered by pev2, including pev2 dark theme support
  - connection credential encryption
  - backported support for monitoring data-grid-based monitoring widgets
  - backported pie charts widgets for numbackends and database sizes
  - added password strength validation for user and master passwords
  - PostgreSQL 9.6, 10, 11, 12, 13, 14 and 15 support

- Major Bugs fixed:
  - fixed data export to csv/xls format in the desktop version of the app
  - added superuser permission check on all user management APIs
  - extra validations added to prevent creation of unnamed connection groups
  - fixed external links not working in the desktop variant of the app
  - fixed postgres special commands on postgresql versions 12 and higher
  - fixed broken postgres documentation links available in database tree view menus
  - made all web/cdn app dependencies local so pgmanage can work properly without an internet connection

- UI/UX Improvements:
  - reorganized connection management menus in the left menu bar
  - fixed DDL tab auto resizing
  - the top-right utilities menu now expands on click instead of mouse-hover
  - added DDL/properties tab resize limits to prevent it from becoming impossible to grab/resize back
  - unified tooltip appearance throughout the whole app
  - unified pictogram look and feel thoughout the whole app
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

![](https://pgmanage.readthedocs.io/en/latest/_images/intro.png)
