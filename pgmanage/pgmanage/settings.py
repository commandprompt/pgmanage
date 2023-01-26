import os
import sys
import shutil
import random
import string
import getpass
from . import custom_settings


# Development Mode
DEBUG = custom_settings.DEV_MODE
DESKTOP_MODE = custom_settings.DESKTOP_MODE
BASE_DIR = custom_settings.BASE_DIR
HOME_DIR = custom_settings.HOME_DIR
TEMP_DIR = os.path.join(BASE_DIR,'app','static','temp')
PLUGINS_DIR = os.path.join(BASE_DIR,'app','plugins')
PLUGINS_STATIC_DIR = os.path.join(BASE_DIR,'app','static','plugins')
APP_DIR = os.path.join(BASE_DIR,'app')

SESSION_COOKIE_SECURE = custom_settings.SESSION_COOKIE_SECURE
CSRF_COOKIE_SECURE = custom_settings.CSRF_COOKIE_SECURE
CSRF_TRUSTED_ORIGINS = []
SESSION_COOKIE_NAME = 'pgmanage_sessionid'
CSRF_COOKIE_NAME = 'pgmanage_csrftoken'
ALLOWED_HOSTS = ['*']

# Database
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': os.path.join(HOME_DIR, 'pgmanage.db')
    }
}

if DEBUG:
    SECRET_KEY = 'ijbq-+%n_(_^ct+qnqp%ir8fzu3n#q^i71j4&y#-6#qe(dx!h3'
else:
    SECRET_KEY = ''.join(random.choice(string.ascii_lowercase + string.digits) for i in range(50))

INSTALLED_APPS = [
    'app',
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
]

if DEBUG:
    INSTALLED_APPS += ['django_sass']

MIDDLEWARE = [
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

ROOT_URLCONF = 'pgmanage.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'pgmanage.wsgi.application'

AUTHENTICATION_BACKENDS = [
    'django.contrib.auth.backends.ModelBackend',
]


AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

LANGUAGE_CODE = 'en-us'
TIME_ZONE = 'UTC'
USE_I18N = True
USE_L10N = True
USE_TZ = True

PATH = custom_settings.PATH
# Processing PATH
if PATH == '/':
    PATH = ''
elif PATH != '':
    if PATH[0] != '/':
        PATH = '/' + PATH
    if PATH[len(PATH)-1] == '/':
        PATH = PATH[:-1]


LOGIN_URL = PATH + '/pgmanage_login'
LOGIN_REDIRECT_URL = PATH + '/'

STATIC_URL = PATH + '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, "app/static")

SESSION_SERIALIZER = 'django.contrib.sessions.serializers.PickleSerializer'

#PgManage LOGGING

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format' : "[%(asctime)s] %(levelname)s [%(name)s:%(lineno)s] %(message)s",
            'datefmt' : "%m/%d/%Y %H:%M:%S"
        },
    },
    'handlers': {
        'logfile_pgmanage': {
            'class':'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(HOME_DIR, 'pgmanage.log'),
            'maxBytes': 1024*1024*5, # 5 MB
            'backupCount': 5,
            'formatter': 'standard',
        },
        'logfile_django': {
            'class':'logging.handlers.RotatingFileHandler',
            'filename': os.path.join(HOME_DIR, 'pgmanage.log'),
            'maxBytes': 1024*1024*5, # 5 MB
            'backupCount': 5,
            'formatter': 'standard',
            'level':'ERROR',
        },
        'console_django':{
            'class':'logging.StreamHandler',
            'formatter': 'standard'
        },
        'console_app':{
            'class':'logging.StreamHandler',
            'formatter': 'standard',
            'level':'ERROR',
        },
    },
    'loggers': {
        'django': {
            'handlers':['logfile_django','console_django'],
            'propagate': False,
        },
        'app': {
            'handlers': ['logfile_pgmanage','console_app'],
            'propagate': False,
            'level':'INFO',
        },
        'cherrypy.error': {
            'handlers': ['logfile_django','console_app'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

#PgManage PARAMETERS
PGMANAGE_VERSION                 = custom_settings.PGMANAGE_VERSION
PGMANAGE_SHORT_VERSION           = custom_settings.PGMANAGE_SHORT_VERSION
CH_CMDS_PER_PAGE               = 20
PWD_TIMEOUT_TOTAL              = 1800
PWD_TIMEOUT_REFRESH            = 300
THREAD_POOL_MAX_WORKERS        = 2
