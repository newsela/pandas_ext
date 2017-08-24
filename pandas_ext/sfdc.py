"""SFDC handler for reading from SFDC."""
from concurrent.futures import ThreadPoolExecutor
from os import environ as env

from jinja2 import Environment
import pandas as pd
import requests


def _get_endpoint_payload():
    """Create route and key headers for API."""
    endpoint = env['SFDC_URL']
    key = env['SFDC_KEY']
    headers = {'x-api-key': key}
    return dict(headers=headers, route=endpoint)


def read_sfdc(sql: str, sql_params=None) -> str:
    """Given sql and params, run the sql."""
    payload = _get_endpoint_payload()
    route = payload['route'] + '/sfdc/query/v20.0'

    sql_params = sql_params if sql_params else {}
    rendered_sql = Environment().from_string(sql).render(**sql_params)

    params = dict(sql=rendered_sql)
    response = requests.get(
        route,
        headers=payload['headers'],
        params=params,
    )
    response.raise_for_status()
    return pd.read_json(response.json())


def sfdc_metadata(sobject: str, fetch_all=False) -> str:
    """Given a sfdc object, return its metadata."""
    payload = _get_endpoint_payload()
    route = payload['route'] + f'/sfdc/metadata/{sobject}/v20.0'

    response = requests.get(
        route,
        headers=payload['headers'],
    )

    response.raise_for_status()
    metadata = response.json()

    if fetch_all:
        return metadata

    return [meta['name'] for meta in metadata['fields']]


def patch_sfdc(sf_url, data):
    """Sync patch sfdc."""
    payload = _get_endpoint_payload()
    route = payload['route'] + '/sfdc/patch/v20.0'

    params = dict(sf_url=sf_url, data=data)
    response = requests.patch(
        route,
        headers=payload['headers'],
        params=params,
    )
    response.raise_for_status()
    return response.json()


def async_patch_sfdc(data):
    """Asynchronously patch sfdc."""
    payload = _get_endpoint_payload()
    route = payload['route'] + '/sfdc/patch/v20.0'

    params = [
        dict(sf_url=items['sf_url'], data=items['patch_data'])
        for items in data
    ]
    threaded_params = (dict(
        url=route,
        headers=payload['headers'],
        params=param

    ) for param in params)
    print(list(threaded_params))

    with ThreadPoolExecutor(max_workers=25) as executor:
        pass
        responses = executor.map(
            requests.patch, **(lambda x: x for x in threaded_params))
    print(list(responses))
    return responses
