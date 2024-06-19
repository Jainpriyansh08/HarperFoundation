import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


def flatten(data, prefix=''):
    temp = {}
    for key, value in data.items():
        if isinstance(value, dict):
            temp.update(flatten(value, f"{prefix}{key}."))
        else:
            temp[f"{prefix}{key}"] = value
    return temp


def flatten_nested(data):
    new_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            flattened = flatten({key: value})
            new_data.update(flattened)
        else:
            new_data[key] = value
    return new_data


def get_requests_session(retries=3, backoff_factor=1, method_whitelist=None, status_forcelist=None):
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]

    session = requests.Session()

    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
        method_whitelist=method_whitelist
    )

    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def get_nested_filters(filters):
    result = {}
    for item in filters:
        if not item.get("filters", []):
            result[item["id"]] = {"label": item["label"], "labelSlug": item["labelSlug"]}
        else:
            result.update(get_nested_filters(item.get("filters", [])))
    return result


def group_filters_by_key(key, filters):
    for filter_item in filters:
        if key == filter_item.get("key"):
            return get_nested_filters(filter_item.get("filters", []))
    return


def get_time_diff(then, now):
    duration = now - then
    duration_in_s = duration.total_seconds()
    return duration_in_s


def get_time_diff_hours(then, now):
    """"""
    duration_in_s = get_time_diff(then, now)
    hours = divmod(duration_in_s, 3600)[0]
    return hours
