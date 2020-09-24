import time
import typing
import signal
import datetime
import functools

from . import logging


class defaultDict(dict):
    """Use this instead of collections.defaultdict, that one doesn't make sense"""
    def __init__(self, *args, default=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.default = default

    def __getitem__(self, key):
        try:
            return super().__getitem__(key)
        except KeyError:
            return self.default

    def pop(self, key):
        try:
            return super().pop(key)
        except KeyError:
            return self.default


def chunked(iterable: typing.Iterable, chunkSize: int) -> typing.Iterable[list]:
    """
    For a given iterable (list, generator, bytes, etc...), yields elements of
    the iterable in chunks.

    Example:
    >>> seq = [0, 1, 2, 3, 4, 5, 6]
    >>> for chunk in chunked(seq, 3):
    ...     print(chunk)
    [0, 1, 2]
    [3, 4, 5]
    [6]
    """
    iterable = iter(iterable)

    while True:
        chunk = []
        try:
            for _ in range(chunkSize):
                chunk.append(next(iterable))
            yield chunk
        except StopIteration:
            break

    if len(chunk) > 0:
        yield chunk


def _currentTimeMillis():
    return int(round(time.time() * 1000))


def timed(name, f=None):
    """
    Usage:
        g = timed('f', f)

        OR

        @timed('f')
        def f():
            ...
    """
    def _timed(name, f):

        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            startTime = _currentTimeMillis()
            result = f(*args, **kwargs)
            endTime = _currentTimeMillis()
            total = endTime - startTime
            logging.info(f'timed_execution {name} {total} ms')

            # wow, did not realise this was possible
            # a function can set attributes on itself during run time!
            # so ripe for abuse
            wrapped.durationSeconds = total / 1000

            return result


        return wrapped

    if f is None:
        def timedDecorator(f):
            return _timed(name, f)

        return timedDecorator
    else:
        return _timed(name, f)


def catchAllExceptionsToLog(func):
    def wrapper(*args, **kwargs):
        if kwargs.pop('PYLON_ALLOW_EXCEPTIONS', False):
            return func(*args, **kwargs)

        try:
            return func(*args, **kwargs)
        except Exception as ex:
            logging.exception(ex)

    return wrapper


def getClassAttributes(cls):
    out = {}
    for attr in dir(cls):
        if attr.startswith('__'):
            continue

        value = getattr(cls, attr)
        if isinstance(value, typing.Callable):
            continue

        out[attr] = value
    return out


def currentTimestampUTC():
    """
    Returns the number of seconds since 1970-01-01 00:00:00 UTC
    """
    return int(datetime.datetime.now().timestamp())


def utcTimeFromUTCTimestamp(utcTimestamp: int):
    """

    Args:
        utcTimestamp: number of seconds since 1970-01-01 00:00:00 UTC

    Returns: a (non-timezone aware) datetime object representing the same time as utcTimestamp, in UTC time

    """
    return datetime.datetime.utcfromtimestamp(utcTimestamp)


def currentISOTimestampUTC():
    """
    Returns the current UTC time in ISO format e.g. '2019-03-28T13:18:34.149233+00:00'
    """
    return datetime.datetime.utcnow().isoformat() + '+00:00'


def currentDatetimeTupleUTC():
    """
    Returns a tuple representing the current utc time
    e.g. time.struct_time(tm_year=2019, tm_mon=3, tm_mday=28, tm_hour=2, tm_min=19, tm_sec=56, tm_wday=3, tm_yday=87, tm_isdst=0)
    """
    return datetime.datetime.utcnow().utctimetuple()


class GracefulLooper:
    """
    Handles graceful SIGTERM shutdown for loops. After receiving a shutdown
    signal, the process wait for the loop cycle to complete before shutting
    down.

    Usage:

    >>> looper = GracefulLooper()
    >>> looper.runForever(my_function, 'arg0', 'arg1', keywordArg='value')
    """

    def __init__(self, sleep=0):
        self.shutdown = False
        self.sleep = sleep

        def registerSignal(signum, frame):
            """Registers a signal for shutdown"""
            logging.warning(f'Signal {signum} received, waiting for loop...')
            self.shutdown = True

        # register hook for receiving signals
        signal.signal(signal.SIGINT, registerSignal)
        signal.signal(signal.SIGTERM, registerSignal)

    def runForever(self, func: typing.Callable, *args, **kwargs):
        """
        Runs a loop forever, gracefully terminates at the end of a cycle if a
        signal has been sent
        """
        # ~~ I wanna live forever ~~
        while not self.shutdown:
            func(*args, **kwargs)
            time.sleep(self.sleep)
