"""SQL handler for reading from SQL service.

>>> df = read_sql("rich_REDSHIFT", "SELECT 1;")
"""
from os import environ as env

from jinja2 import Environment
import pandas as pd
import requests


def _get_endpoint_payload():
    """Create route and key headers for API."""
    endpoint = env['SQL_URL']
    key = env['SQL_KEY']
    headers = {'x-api-key': key}
    return dict(headers=headers, route=endpoint)


def read_sql(backend: str, sql: str, sql_params=None) -> str:
    """Given a backend, sql and params, run the sql."""
    payload = _get_endpoint_payload()
    route = payload['route'] + f'/view/{backend}'

    sql_params = sql_params if sql_params else {}
    rendered_sql = Environment().from_string(sql).render(**sql_params)

    params = dict(sql=rendered_sql)
    response = requests.get(
        route,
        headers=payload['headers'],
        params=params,
    )
    response.raise_for_status()
    return pd.read_json(response.text)


def list_backends():
    """Retreive all backends registered."""
    payload = _get_endpoint_payload()
    route = payload['route'] + f'/backends'

    response = requests.get(route, headers=payload['headers'])
    response.raise_for_status()
    return response.json()
