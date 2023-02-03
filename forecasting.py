from multiprocessing import Queue

from tasks import (DataFetchingTask, DataAggregationTask, DataAnalyzingTask)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    queue = Queue()
    processes = []
    data_aggregation = DataAggregationTask(queue)

    for city in CITIES.keys():
        process = DataFetchingTask(city, queue)
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    data_aggregation.start()
    data_aggregation.join()

    data_analyzing = DataAnalyzingTask()
    data_analyzing.start()
    data_analyzing.join()


if __name__ == "__main__":
    forecast_weather()
