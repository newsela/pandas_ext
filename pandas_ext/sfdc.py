"""SFDC handler for reading from SFDC."""
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
    """Given sql and params, run the sql. """
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