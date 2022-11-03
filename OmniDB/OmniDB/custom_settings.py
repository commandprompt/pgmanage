import os
import sys
from dotenv import load_dotenv

# this is for pyinstaller script to collect .env file
extDataDir = os.getcwd()
if getattr(sys, 'frozen', False):
    extDataDir = sys._MEIPASS
    load_dotenv(dotenv_path=os.path.join(extDataDir, '.env'))

# OmniDB settings
OMNIDB_VERSION = f"PgManage {os.getenv('VERSION', 'dev')}"
OMNIDB_SHORT_VERSION = os.getenv('VERSION', 'dev')
DEV_MODE = True
DESKTOP_MODE = False
APP_TOKEN = None
PATH = ''
HOME_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Django settings
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_SECURE = False
