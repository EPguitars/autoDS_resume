# celeryconfig.py
CELERY_BROKER_URL = 'pyamqp://guest@localhost//'
CELERY_RESULT_BACKEND = 'rpc://'
