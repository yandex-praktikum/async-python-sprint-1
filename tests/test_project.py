from datetime import date

import pytest

from tasks import (DataFetchingTask, CalculateDayAverageTemperature, WeatherData,
                   CalculateDayHoursWithoutPrecipitation)
from utils import TEMPERATURE_TASK_NAME, HOURS_WITHOUT_PRECIPITATION_TASK_NAME


@pytest.mark.parametrize(['city_name', 'has_error'],
                         [
                             ['MOSCOW', False],
                             ['MOSCOW1', True]
                         ]
                         )
def test_data_fetching_task_cls(city_name, has_error, que_fixture):
    # Проверяем что класс DataFetchingTask корректно инициализируется с верными данными и
    # отдает ошибку с некорректными данными
    if not has_error:
        DataFetchingTask(city_name, que_fixture)
    else:
        with pytest.raises(Exception):
            DataFetchingTask(city_name, que_fixture)


def test_data_fetching_task_receive_weather(data_fixture, que_fixture):
    # Проверяем то что корректно обрабатываются полученные данные о погоде
    data_task_cls = DataFetchingTask('MOSCOW', que_fixture)
    data_task_cls.data = data_fixture
    data_task_cls._receive_weather_forecast_data()


def test_calculate_day_average_temperature_cls(raw_data_fixture, que_fixture):
    weather_data = WeatherData(
        city='MOSCOW',
        data_type=TEMPERATURE_TASK_NAME,
        date=date(2022, 5, 18),
        value=13.090909090909092
    )
    cls = CalculateDayAverageTemperature(raw_data_fixture, que_fixture, 'MOSCOW')
    assert cls._get_temperature_day_value(raw_data_fixture[0]) == weather_data


def test_calculate_day_hours_without_precipitation(raw_data_fixture, que_fixture):
    weather_data = WeatherData(
        city='MOSCOW',
        data_type=HOURS_WITHOUT_PRECIPITATION_TASK_NAME,
        date=date(2022, 5, 18),
        value=11
    )
    cls = CalculateDayHoursWithoutPrecipitation(raw_data_fixture, que_fixture, 'MOSCOW')
    assert cls._count_hours_without_perceptions(raw_data_fixture[0]) == weather_data
