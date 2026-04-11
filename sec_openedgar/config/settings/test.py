from .base import *

DEBUG = False
SECRET_KEY = 'test-secret'
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.sqlite3',
        'NAME': ':memory:',
    }
}
CELERY_ALWAYS_EAGER = True
CELERY_BROKER_URL = 'memory://'

# Disable RAG pipeline for simple unit tests
HDB_PATH = ":memory:"
