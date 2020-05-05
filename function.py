from api import Rest_api
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import numpy as np


# pass

def spread_limits(backward, df, tickers, diff_mid, profit_buff, std_seperate):
    lambdaD = regression(df, tickers)
    df['diff'] = df[tickers[0]]
    for i in range(len(lambdaD)):
        df['diff'] -= lambdaD[i] * df[tickers[i+1]]
    diff_mid = df['diff'].mean()
    stdd = df['diff'].std()
    basic = df[tickers[0]][-1] * profit_buff
    if stdd / std_seperate < basic:  # 預防區間太小
        trade_range = basic
    else:
        trade_range = stdd / std_seperate
    return diff_mid, lambdaD, trade_range

def open_(ra, method, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD):
    action = []
    if method == 'long':
        action.append('buy')
        open_position[0] = 1
    else:
        action.append('sell')
        open_position[0] = -1
    for i in range(1, len(tickers)):
        if action[0] == 'buy':
            action.append('sell')
        elif action[0] == 'sell':
            action.append('buy')
    set_qty_ = locals()
    set_qty_[0] = set_qty
    entry_price[0] = live_price[0]
    qty[0] = set_qty
    for i in range(len(lambdaD)):
        if lambdaD[i] >= 0:
            if action[i+1] == 'buy':
                open_position[i+1] = 1
            else:
                open_position[i+1] = -1
            set_qty_[i+1] = lambdaD[i] * set_qty      
            entry_price[i+1] = live_price[i+1]                       
            qty[i+1] = set_qty_[i+1]
        else:
            if action[i+1] == 'buy':
                action[i+1] == 'sell'
                open_position[i+1] = -1
            else:
                action[i+1] == 'buy'
                open_position[i+1] = 1
            set_qty_[i+1] = abs(lambdaD[i]) * set_qty               
            entry_price[i+1] = live_price[i+1]                       
            qty[i+1] = set_qty_[i+1]
    with ThreadPoolExecutor() as executor:
        for i in range(len(action)):
            executor.submit(ra.place_order, tickers[i], action[i], set_qty_[i], live_price[i])
    return open_position, entry_price, qty

def add_(ra, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD):
    action = []
    for i in range(len(tickers)):
        if open_position[i] > 0:
            action.append('buy')
        elif open_position[i] < 0:
            action.append('sell')
    set_qty_ = locals()
    set_qty_[0] = set_qty
    entry_price[0] = (entry_price[0] * qty[0] + live_price[0] * set_qty_[0]) / (qty[0] + set_qty_[0])
    qty[0] += set_qty
    for i in range(len(lambdaD)):
        set_qty_[i+1] = abs(lambdaD[i]) * set_qty               
        entry_price[i+1] = (entry_price[i+1] * qty[i+1] + live_price[i+1] * set_qty_[i+1]) / (qty[i+1] + set_qty_[i+1])                          
        qty[i+1] += set_qty_[i+1]
    with ThreadPoolExecutor() as executor:
        for i in range(len(action)):
            executor.submit(ra.place_order, tickers[i], action[i], set_qty_[i], live_price[i])
    return entry_price, qty

def cut_position(ra, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD):
    action = []
    for i in range(len(tickers)):
        if open_position[i] > 0:
            action.append('sell')
        elif open_position[i] < 0:
            action.append('buy')
    set_qty_ = locals()
    set_qty_[0] = set_qty
    qty[0] -= set_qty
    for i in range(len(lambdaD)):
        set_qty_[i+1] = abs(lambdaD[i]) * set_qty                                         
        qty[i+1] -= set_qty_[i+1]
    with ThreadPoolExecutor() as executor:
        for i in range(len(action)):
            executor.submit(ra.place_order, tickers[i], action[i], set_qty_[i], live_price[i], reduce_only=True)
    if qty[0] <= 0:
        for i in range(len(tickers)):
            open_position[i] = 0
            entry_price[i] = 0
            qty[i] = 0
    return open_position, entry_price, qty

def close_position(ra, tickers, live_price, set_qty, qty, open_position, entry_price):
    action = []
    for i in range(len(tickers)):
        if open_position[i] > 0:
            action.append('sell')
        elif open_position[i] < 0:
            action.append('buy')
    with ThreadPoolExecutor() as executor:
        for i in range(len(action)):
            executor.submit(ra.place_order, tickers[i], action[i], qty[i], live_price[i], reduce_only=True)
    for i in range(len(tickers)):
        open_position[i] = 0
        entry_price[i] = 0
        qty[i] = 0
    return open_position, entry_price, qty

def spread_strategy(ra, tickers, set_qty, qty, live_price, live_diff, diff_mid, trade_range, open_position, entry_price, lambdaD, stop_add):
    times = qty[0] / set_qty
    if open_position[0] > 0 :
        if live_diff > diff_mid:
            open_position, entry_price, qty = close_position(ra, tickers, live_price, set_qty, qty, open_position, entry_price)
        elif live_diff < diff_mid - trade_range * (1 + times) and not stop_add:
            entry_price, qty = add_(ra, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD)
        elif live_diff > diff_mid - trade_range * (times - 1) and times > 1:
            open_position, entry_price, qty = cut_position(ra, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD)
    elif open_position[0] < 0 :
        if live_diff < diff_mid:
            open_position, entry_price, qty = close_position(ra, tickers, live_price, set_qty, qty, open_position, entry_price)
        elif live_diff > diff_mid + trade_range * (1 + times) and not stop_add:
            entry_price, qty = add_(ra, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD)
        elif live_diff < diff_mid + trade_range * (times - 1) and times > 1:
            open_position, entry_price, qty = cut_position(ra, tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD)
    elif open_position[0] == 0:
        # 第一層
        if live_diff < diff_mid - trade_range:
            open_position, entry_price, qty = open_(ra, 'long', tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD)
        elif live_diff > diff_mid + trade_range:
            open_position, entry_price, qty = open_(ra, 'short',tickers, live_price, set_qty, qty, open_position, entry_price, lambdaD)
    return open_position, entry_price, qty





