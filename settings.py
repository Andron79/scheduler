import logging
import sys

# Максимальное количество задач для планировщика
MAX_TASKS_COUNT = 3

# Формат времени
TIME_FORMAT = '%Y-%m-%d %H:%M'

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] - %(message)s',
    stream=sys.stdout,
    datefmt=TIME_FORMAT)
