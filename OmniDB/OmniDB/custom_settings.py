import os


# OmniDB settings
OMNIDB_VERSION = 'Dev'
OMNIDB_SHORT_VERSION = 'dev'
DEV_MODE = True
DESKTOP_MODE = False
APP_TOKEN = None
PATH = ''
HOME_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Django settings
SESSION_COOKIE_SECURE = False
CSRF_COOKIE_SECURE = False
