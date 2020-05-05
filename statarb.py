from function import *
import json, time
import websocket, os, sys
import hmac, hashlib
from datetime import datetime
from collections import deque


class Live_data():
    def __init__(self):
        # 初始化Websocket
        depth_address = "wss://ftx.com/ws/"
        websocket.enableTrace(True)
        self.ws = websocket.WebSocketApp(depth_address, on_message=self.on_message, on_error=self.on_error, on_close=self.on_close)
        self.ws.on_open = self.on_open
        self.key = ''
        self.secret = ''
        self.subaccount = ''
        self.ftx_service = websocket.WebSocketApp(self.key, self.secret, depth_address)
        self.tickers = list(['ETH-PERP', 'LINK-PERP', 'SHIT-PERP'])
        self.set_qty = 0.0                                                           # 設定下單量，0號位顆數爲基準單位
        self.backward = 90                                                           # 過去
        self.live_diff = np.nan                                                     # 創array來存資料
        self.diff_mid = np.nan
        self.trade_range = 0.0
        self.live_price = np.full((len(self.tickers)), np.nan, dtype='float64')    # 固定順序是永續先放，再依序放季期貨
        self.open_position = np.full((len(self.tickers)), 0, dtype='int')  
        self.entry_price = np.full((len(self.tickers)), 0.0, dtype='float64')      # 存進場價格
        self.qty = np.full((len(self.tickers)), 0.0, dtype='float64')              # 各商品的下單量，有未成交的狀況會改變下單量
        self.commision = 0.00252                             # 來回手續費
        self.slip_point = 0.0040                             # 可能出場滑價
        self.sure_point = 0.00448                            # 獲利保留
        self.profit_buff = self.commision + self.slip_point + self.sure_point  
        self.ra = Rest_api()
        self.min_update = False
        self.stop_add = False
        self.danger_zone = 0.10
        self.lambdaD = 0.0
        self.spread_range = np.full((len(self.tickers)), 0.0, dtype='float64')
        self.last_num = len(self.tickers) - 1
        self.std_seperate = 2

    # 啓動
    def run(self):
        self.ws.run_forever(ping_interval=60, ping_timeout=5)

    # WebSocket回傳
    def on_message(self, message):
        # 更新tick價格，判斷進場
        if 'update' in message and 'ticker' in message and "bid" in message and "ask" in message:
            self.tick_managing(message)
        
        # 成交回報後，才更新部位資訊
        if 'fills' in message and 'data' in message and 'fee' in message:
            with ThreadPoolExecutor() as executor:
                executor.submit(self.execution_managing, message)

        # pass
        
    def on_error(self, error):
        print("Websocket連接錯誤，%s" % (error))
 
    def on_close(self):
        print("Websocket連接關閉，5秒後重新連接！")
        sys.exit(0)

    def on_open(self):
        print("Websocket連接建立成功！")
        print("資料回填中...")
        self.subscribe_auth_parts()
        for ticker in self.tickers:
            self.subscribe_tick(ticker)
        self.price_init()
        self.daily_managing()
        self.positions_managing()
        print('部位資料回填完成！')
        self.min_managing()
    
    def subscribe_tick(self, symbol):
        tradeStr = json.dumps({"op": "subscribe","channel":"ticker", "market":"{}".format(symbol)})
        self.ws.send(tradeStr)

    def subscribe_auth_parts(self):
        ts = int(time.time() * 1000)
        signature = hmac.new(self.secret.encode(), f'{ts}websocket_login'.encode(), digestmod=hashlib.sha256).hexdigest()
        tradeStr = json.dumps({"op": "login","args": {"key":self.key, "sign":signature, "time":ts, "subaccount":self.subaccount}})
        self.ws.send(tradeStr)
        tradeStr = json.dumps({"op": "subscribe","channel": "fills"})
        self.ws.send(tradeStr)

    def price_init(self):
        reqs = self.ra.list_markets()
        for req in reqs:
            symbol = req['name']
            if symbol in self.tickers:
                location = self.tickers.index(symbol)
                self.live_price[location] = float(req['price'])
                self.spread_range[location] = float(req['priceIncrement']) * 20
        reqss = self.ra.get_account_info()
        self.value = reqss['collateral']
        self.set_qty = round(self.value / self.live_price[0], 4)

    def execution_managing(self, message):  # 成交是dict
        dataset = json.loads(message)
        obj = dataset['data']
        symbol = str(obj['market'])
        side = str(obj['side'])
        price = str(obj['price'])
        qty = str(obj['size'])
        ty = str(obj['liquidity'])
        print('{} | {} {} {}-order executed at {} with {} !'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        side, symbol, ty, price, qty))
        self.positions_managing()  # 利用api更新部位
        
    def positions_managing(self):
        dataset = self.ra.get_positions()
        for obj in dataset:
            symbol = str(obj['future'])
            if symbol in self.tickers:
                location = self.tickers.index(symbol)
                true_qty = float(obj["netSize"])
                if true_qty < 0 :
                    self.open_position[location] = -1
                    self.entry_price[location] = float(obj["recentAverageOpenPrice"])
                    self.qty[location] = abs(true_qty)
                elif true_qty > 0:
                    self.open_position[location] = 1
                    self.entry_price[location] = float(obj["recentAverageOpenPrice"])
                    self.qty[location] = abs(true_qty)
                elif true_qty == 0:
                    self.open_position[location] = 0
                    self.entry_price[location] = 0.0
                    self.qty[location] = 0.0
                
    def tick_managing(self, message):
        dataset = json.loads(message)
        symbol = dataset['market']
        location = self.tickers.index('{}'.format(symbol))
        if not dataset['data']['bid'] == 'null' or not dataset['data']['ask'] == 'null':
            bid = dataset['data']['bid']
            ask = dataset['data']['ask']
            price = np.mean([bid, ask], dtype='float64')
            self.live_price[location] = price  
            # 判斷array裏面有沒有np.nan, 沒有才更新價差資料
            if not np.isnan(np.sum(self.live_price)) and location == self.last_num:   
                temp_diff = self.live_price[0]
                for i in range(len(self.lambdaD)):
                    temp_diff -= self.lambdaD[i] * self.live_price[i+1]   
                self.live_diff = temp_diff
                if not np.isnan(self.diff_mid):
                    self.open_position, self.entry_price, self.qty = spread_strategy(self.ra, self.tickers, self.set_qty,
                        self.qty, self.live_price, self.live_diff, self.diff_mid, self.trade_range, self.open_position, self.entry_price, self.lambdaD, self.stop_add)     

    def min_managing(self):
        reqs = self.ra.get_account_info()
        margin = reqs['marginFraction']
        if margin == None:
            margin = 0.0
            self.stop_add = False
        elif margin <= self.danger_zone:
            self.stop_add = True
        elif margin > self.danger_zone:
            self.stop_add = False
        value = reqs['collateral']
        self.daily_managing()
        if np.isnan(self.live_diff):
            guaili = np.nan
        else:
            guaili = 100 * (self.live_diff - self.diff_mid) / self.diff_mid
        print('{} | Spread = {} | 基準: {} | 乖離: {} %'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 
            round(self.live_diff,2), round(self.diff_mid,2), round(guaili,2)))
        print(' '*19+ ' | {} 部位: {} 價格: {} 間距: {}'.format( 
            self.tickers[0], self.open_position[0]*self.qty[0],
            round(self.entry_price[0], 2), round(self.trade_range, 2)))
        for i in range(1,len(self.tickers)):
            print(' '*19+ ' | {} 部位: {} 價格: {} Lambda: {}'.format(
                self.tickers[i], round(self.open_position[i]*self.qty[i],4), 
                round(self.entry_price[i], 2), round(self.lambdaD[i-1], 2)))
        print(' '*19+ ' | 市值: {} USD | 維持: {} %'.format(round(value,2), round(margin*100,2)))
    
        def lambda_managing(self):
        self.ram_df = pd.DataFrame(columns=self.tickers)
        self.ram_df = prepare_ram_df(self.ram_df, self.backward, self.tickers)
        self.diff_mid, self.lambdaD, self.trade_range = spread_limits(self.backward, self.ram_df, self.tickers, self.diff_mid, self.profit_buff, self.std_seperate)

if __name__ == "__main__":
    # pass
