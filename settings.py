import logging
import sys

# Максимальное количество задач для планировщика
MAX_TASKS_COUNT = 10

# файл для сохранения не выполненных тасков
SAVED_TASKS_FILE = 'saved_tasks.lock'

# Формат времени
TIME_FORMAT = '%Y-%m-%d %H:%M'

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s [%(filename)s:%(module)s:%(funcName)s:%(lineno)d] %(message)s',
    stream=sys.stdout
)
logging.addLevelName(logging.ERROR, "\033[1;41m%s\033[1;0m" % logging.getLevelName(logging.ERROR))
logging.addLevelName(logging.WARNING, "\033[1;31m%s\033[1;0m" % logging.getLevelName(logging.WARNING))
