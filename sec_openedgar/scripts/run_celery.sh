source .env
celery -A sec_openedgar.taskapp worker -E --loglevel=ERROR -f celery.log -c16 -l info
