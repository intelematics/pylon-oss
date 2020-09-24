import time
from datetime import datetime

import pytest

from pylon import utils


def test_defaultDict():
    dd = utils.defaultDict(a=1, b=2, default=15)
    assert dd['a'] == 1
    assert dd['b'] == 2
    assert dd['c'] == 15
    assert list(dd.keys()) == ['a', 'b']

    assert dd.pop('a') == 1
    assert dd.pop('b') == 2
    assert dd.pop('c') == 15
    assert list(dd.keys()) == []


@pytest.mark.parametrize(
    'inp,expected',
    [
        (([0, 1, 2, 3, 4, 5, 6], 2), [[0, 1], [2, 3], [4, 5], [6]]),
        (([0, 1, 2, 3, 4, 5, 6], 3), [[0, 1, 2], [3, 4, 5], [6]]),
        (([0, 1, 2, 3, 4, 5, 6], 4), [[0, 1, 2, 3], [4, 5, 6]]),
        (([0, 1, 2, 3, 4, 5, 6], 9), [[0, 1, 2, 3, 4, 5, 6]]),
    ]
)
def test_chunked(inp, expected):

    out = list(utils.chunked(*inp))
    assert out == expected

@pytest.mark.parametrize(
    'func',
    [
        utils.timed('simple', lambda: time.sleep(0.1)),
        utils.timed('decorated')(lambda: time.sleep(0.1))
    ]
)
def test_timed(caplog, func):
    func()

    # ensure that the decorated method still has the expected name
    # decorators can sometimes do funny business with names
    assert func.__name__ == '<lambda>'

    logMessage = caplog.records[0].message
    assert logMessage.startswith('timed_execution')
    execTime = float(logMessage.split()[-2])
    assert execTime == pytest.approx(100, rel=0.1)


def test_timed_class(caplog):
    class Dummy:
        def __init__(self, name):
            self.name = name

        @utils.timed('method')
        def wait(self):
            time.sleep(0.1)
            return self.name

    dd = Dummy('hi')
    out = dd.wait()

    assert out == 'hi'

    # ensure that the decorated method still has the expected name
    # decorators can sometimes do funny business with names
    assert dd.wait.__name__ == 'wait'

    logMessage = caplog.records[0].message
    assert logMessage.startswith('timed_execution')
    execTime = float(logMessage.split()[-2])
    assert execTime == pytest.approx(100, rel=0.1)


def test_catchAllExceptionsToLog(caplog):

    @utils.catchAllExceptionsToLog
    def bad():
        raise Exception('I am bad')

    bad()

    # check that the exception is printed to log
    assert 'Exception: I am bad' in caplog.text

    # check that the traceback is also printed to log
    assert 'Traceback' in caplog.text
    assert "raise Exception('I am bad')" in caplog.text


def test_currentTimestampUTC():
    """
    assert we get the correct type back and
    the time is further in the future than 1st Jan 2019
    """
    result = utils.currentTimestampUTC()
    assert(isinstance(result, int))
    assert(result > 1546300800)


@pytest.mark.parametrize(
    ('inp', 'exp'),
    [
        (0, '1970-01-01 00:00:00'),
        (1553741776, '2019-03-28 2:56:16')
    ]
)
def test_utcTimeFromUTCTimestamp(inp, exp):
    """
    simple tests for `pylon.utils.utcTimeFromUTCTimestamp`
    """
    dateFormat = '%Y-%m-%d %H:%M:%S'
    exp = datetime.strptime(exp, dateFormat)
    result = utils.utcTimeFromUTCTimestamp(inp)
    assert(result == exp)


def test_currentISOTimestampUTC():
    """
    checks `pylon.utils.currentISOTimestampUTC` returns something with the correct format
    """
    result = utils.currentISOTimestampUTC()
    # if the result does not match the format this will throw an error
    datetime.strptime(result, '%Y-%m-%dT%H:%M:%S.%f+00:00')


def test_currentDatetimeTupleUTC():
    """
    checks `pylon.utils.currentDatetimeTupleUTC` returns something with the correct format
    """
    result = utils.currentDatetimeTupleUTC()

    result.tm_year
    result.tm_mon
    result.tm_mday
    result.tm_hour
    result.tm_min
    result.tm_sec
    result.tm_wday
    result.tm_yday
    result.tm_isdst

    assert(len(result) == 9)
