import json, urllib
import hmac, hashlib
import time, os
from datetime import datetime, timedelta
import pandas as pd
from requests import Request, Session, Response
from typing import Optional, Dict, Any, List


class Rest_api():
    _ENDPOINT = 'https://ftx.com/api/'
    def __init__(self, subaccount_name='None'):
        self._session = Session()
        self._api_key = ''
        self._api_secret = ''
        self._subaccount_name = subaccount_name
        self.trading = True

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('GET', path, params=params)

    def _post(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('POST', path, json=params)

    def _delete(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        return self._request('DELETE', path, json=params)

    def _request(self, method: str, path: str, **kwargs) -> Any:
        request = Request(method, self._ENDPOINT + path, **kwargs)
        self._sign_request(request)
        response = self._session.send(request.prepare())
        return self._process_response(response)

    def _sign_request(self, request: Request) -> None:
        ts = int(time.time() * 1000)
        prepared = request.prepare()
        signature_payload = '{}{}{}'.format(ts, prepared.method, prepared.path_url).encode()
        if prepared.body:
            signature_payload += prepared.body
        signature = hmac.new(self._api_secret.encode(), signature_payload, 'sha256').hexdigest()
        request.headers['FTX-KEY'] = self._api_key
        request.headers['FTX-SIGN'] = signature
        request.headers['FTX-TS'] = str(ts)
        if self._subaccount_name:
            request.headers['FTX-SUBACCOUNT'] = self._subaccount_name

    def _process_response(self, response: Response) -> Any:
        try:
            data = response.json()
        except ValueError:
            print(data)
        else:
            if not data['success']:
                return None
            return data['result']

    def list_futures(self) -> List[dict]:
        return self._get('futures')

    def list_markets(self) -> List[dict]:
        return self._get('markets')

    def get_orderbook(self, market: str, depth: int = None) -> dict:
        return self._get('markets/{}/orderbook'.format(market), {'depth': depth})

    def get_trades(self, market: str) -> dict:
        return self._get('markets/{}/trades'.format(market))

    def get_account_info(self) -> dict:
        return self._get('account')

    def get_open_orders(self, market: str = None) -> List[dict]:
        return self._get('orders', {'market': market})

    def place_order(self, market: str, side: str, size: float, price: float, order_type: str = 'limit',
                    reduce_only: bool = False, ioc: bool = False, post_only: bool = False,
                    client_id: str = None) -> dict:
        if order_type == 'limit':
            price = self.add_range_limit(market, side, price)
        if self.trading:
            result =  self._post('orders', {'market': market,
                                            'side': side,
                                            'price': price,
                                            'size': size,
                                            'type': order_type,
                                            'reduceOnly': reduce_only,
                                            'ioc': ioc,
                                            'postOnly': post_only,
                                            'clientId': client_id,})


    def cancel_order(self, order_id: str) -> dict:
        return self._delete('orders/{}'.format(order_id))

    def cancel_orders(self, market_name: str = None, conditional_orders: bool = False,
                      limit_orders: bool = False) -> dict:
        return self._delete('orders', {'market': market_name,
                                        'conditionalOrdersOnly': conditional_orders,
                                        'limitOrdersOnly': limit_orders,
                                        })

    def get_fills(self) -> List[dict]:
        return self._get('fills')

    def get_balances(self) -> List[dict]:
        return self._get('wallet/balances')

    def get_deposit_address(self, ticker: str) -> dict:
        return self._get('wallet/deposit_address/{}'.format(ticker))

    def get_positions(self, show_avg_price: bool = True) -> List[dict]:
        return self._get('positions', {'showAvgPrice': show_avg_price})

    def get_position(self, name: str, show_avg_price: bool = True) -> dict:
        return next(filter(lambda x: x['future'] == name, self.get_positions(show_avg_price)), None)

    def get_history_data(self, symbol, backward):
        resolution = 86400  # 1 day
        x = datetime.now()
        reqs = self._get('/markets/{}/candles?resolution={}&limit={}'.format(
            symbol, resolution, backward+1))
        df = pd.DataFrame(columns=['open','high','low','close','volume'])
        try:
            for req in reqs:
                timestamp = req['startTime']
                timestamp = timestamp[:10]+' '+timestamp[11:19]
                timestamp = datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
                timestamp += timedelta(hours=8)  # 轉成臺灣時間
                df.at[timestamp,'open'] = req['open']
                df.at[timestamp,'high'] = req['high']
                df.at[timestamp,'low'] = req['low']
                df.at[timestamp,'close'] = req['close']
                df.at[timestamp,'volume'] = req['volume']
            df.index.name = 'date'
            if df.index[-1].day >= x.day and x.hour < 8:  # 今天的不能算，還沒完成
                df = df[:-1]
        except:
            pass
        return df
    
    def add_range_limit(self, symbol, side, price):
        market_limit_point = 0.0020
        if side == 'buy':
            price += price * market_limit_point
        else:
            price -= price * market_limit_point
        return price
    
if __name__=='__main__':
    ra = Rest_api()
    df = ra.list_markets()
    print(df)
    
    
