from io import BytesIO

import pandas as pd
import requests

TYPE = ['event_time']
STATUS = "Error download while requesting {}."


class StatusException(Exception):
    def __init__(self, text):
        Exception.__init__(self, STATUS.format(text))


def mean_arpdau(url):
    response = requests.get(url)
    if response.status_code == 200:
        df = pd.read_csv(BytesIO(response.content),
                         compression='gzip', parse_dates=TYPE)
        df['event_date'] = df.event_time.dt.date
        res_data = pd.concat([df[df.event_name == 'purchase'].
                             groupby('event_date').agg({'event_value': sum}),
                              df[df.event_name == 'launch'].
                             groupby('event_date')[['user_id']].agg('nunique')],
                             axis=1)
        res = (res_data['event_value'] / res_data['user_id']).mean()
        return res
    else:
        raise StatusException(url) from ex
