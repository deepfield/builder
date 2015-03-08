
import re
import datetime as dt
import dateutil as du

def _parse_frequency(freq):
    '''
    '13H' -> return (13, 'H'), 'D' -> return (1, 'D')
    '''
    m = re.search(r"\s*(?P<multiplier>-?\d*)\s*(?P<frequency>[a-zA-Z_]*)\s*", freq)
    multiplier = int(m.group("multiplier")) if m.group("multiplier") else 1
    frequency = m.group("frequency")
    return (multiplier, frequency)

def convert_to_timedelta(time_val):
    '''
    Returns a timedelta object representing the corresponding timedelta for
    a given frequency. E.g.

        "5 minutes" -> datetime.timedelta(0, 300)
        "2h" -> datetime.timedelta(0, 7200)
        "4 days" -> datetime.timedelta(4)
    '''
    if not time_val:
        return None

    try:
        time_val = int(time_val)
    except ValueError:
        pass
    if time_val == 10:
        time_val = '10sec'
    elif time_val == 300:
        time_val = '5min'
    elif time_val == 3600:
        time_val = '1h'
    elif time_val == 86400:
        time_val = '1d'

    mult, freq = _parse_frequency(time_val)
    freq = freq.lower()

    matches = {
        'minute': [
            'm',
            't', # 'T' is minute in pandas
            'min',
            'mins',
            'minute',
            'minutes'
        ],
        'hour': [
            'h',
            'hour',
            'hours'
        ],
        'day': [
            'd',
            'day',
            'days'
        ],
        'month': [
            'month',
            'months'
        ],
        'second': [
            's',
            'sec',
            'secs',
            'second',
            'seconds'
        ]
    }

    if any(freq.endswith(match) for match in matches['minute']):
        return dt.timedelta(minutes=mult)
    elif any(freq.endswith(match) for match in matches['month']):
        return du.relativedelta.relativedelta(months=mult)
    elif any(freq.endswith(match) for match in matches['hour']):
        return dt.timedelta(hours=mult)
    elif any(freq.endswith(match) for match in matches['day']):
        return dt.timedelta(days=mult)

    # This check must go last because endswith 's' will short circuit endswith
    # 'hours', 'days', etc.
    elif any(freq.endswith(match) for match in matches['second']):
        return dt.timedelta(seconds=mult)
