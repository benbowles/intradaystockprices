from ..imports import *
from . import constants

@lru_cache(maxsize=32)
def get_schedule(end_date=str(dt.datetime.now().date())):
    nyse = mcal.get_calendar('NYSE')
    schedule = nyse.schedule(start_date=constants.START_DATE, end_date=end_date)
    return schedule


@lru_cache(maxsize=32)
def get_days_dict():
    schedule = get_schedule()
    d = dict(schedule.apply(lambda x: x['market_open'].timestamp(), axis=1))
    d = {str(k.date()): int(v) for k, v in d.items()}
    return d


def business_dates():
    return get_days_dict().keys()


def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk

