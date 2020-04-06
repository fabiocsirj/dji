import threading
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from time import sleep
import tokens

LOG_FILE = 'DJI.log'
# data = datetime.strptime('2020-04-03 16:44:00', '%Y-%m-%d %H:%M:%S')


def telegram_sendText(message):
    bot_token = tokens.TELEGRAM_TOKEN
    bot_chatID = '986525812'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + message
    try:
        response = requests.get(send_text)
        return response.json()
    except:
        return 'ERROR TELEGRAM SEND!'
    

def get_TradeDJI():
    simbol = '^DJI'
    interval = 1 # minutes    
    token = tokens.ALPHAVANTAGE_TOKEN
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval={}min&apikey={}'.format(simbol, interval, token)
    try:
        dji = requests.get(url)
        trades = dji.json()
        # print(trades)
        return trades['Time Series ({}min)'.format(interval)]
    except:
        return None


def get_bb_rsi(df):
    # BOLLINGER BANDS
    sma20 = df['close'].rolling(window=20).mean()
    df['sma20'] = sma20
    rs20 = df['close'].rolling(window=20).std()
    bb_up = sma20 + rs20 * 2
    bb_dw = sma20 - rs20 * 2
    df['bb_up'] = bb_up
    df['bb_dw'] = bb_dw

    # Relative Strength Index
    u = df['close'].to_frame()
    d = df['close'].to_frame()
    u[df['close'] < df['open']] = 0
    d[df['close'] > df['open']] = 0
    rm14_u = u.rolling(window=9).mean()
    rm14_d = d.rolling(window=9).mean()
    rsi = 100 - (100 / (1 + rm14_u / rm14_d))
    df['rsi'] = rsi

    return df


def is_Sell(anterior, atual):
    if atual['close'] < atual['open'] and (anterior['bb_up'] < max(anterior['open'], anterior['close']) or anterior['rsi'] > 70):
        return True
    else: 
        return False


def is_Buy(anterior, atual):
    if atual['close'] > atual['open'] and (anterior['bb_dw'] > min(anterior['open'], anterior['close']) or anterior['rsi'] < 30):
        return True
    else: 
        return False


def worker():
    with open(LOG_FILE, 'a') as log: print(datetime.now(), "Job working...", file=log)
    log.close()

    trades = get_TradeDJI()
    # print(trades)
    
    if trades:
        trades = eval(str(trades).replace('1. open', 'open').replace('2. high', 'high').replace('3. low', 'low').replace('4. close', 'close').replace('5. volume', 'volume'))

        df = pd.DataFrame.from_dict(trades, orient='index')
        df = df[::-1]

        first = df.iloc[0].name
        first_date = datetime.strptime(first, '%Y-%m-%d %H:%M:%S')
        first_minute = int(datetime.strftime(first_date, '%M'))
        print(first_minute)
        if first_minute%2 == 0:
            df1 = df[1::2]
            df2 = df[2::2]
        else:
            df1 = df[::2]
            df2 = df[1::2]
        
        dfx = df2['close'].to_frame()
        dfx['open'] = df1['open'].values
        dfx['high'] = np.maximum(df1['high'].astype('float').values, df2['high'].astype('float').values)
        dfx['low'] = np.minimum(df1['low'].astype('float').values, df2['low'].astype('float').values)        
        dfx['volume'] = np.add(df1['volume'].astype('float').values, df2['volume'].astype('float').values)
        dfx = dfx[['open', 'high', 'low', 'close', 'volume']]

        df = get_bb_rsi(dfx)
        # with pd.option_context('display.max_rows', None, 'display.width', 300):
        #     print(df)
    
        data = datetime.now()
        data_p = datetime.strftime(data-timedelta(hours=1, minutes=2), '%Y-%m-%d %H:%M:00')
        data_u = datetime.strftime(data-timedelta(hours=1), '%Y-%m-%d %H:%M:00')        
        penultimo = df.loc[data_p].astype('float')
        ultimo    = df.loc[data_u].astype('float')
        with open(LOG_FILE, 'a') as log:
            print(data_p, penultimo.to_json(), file=log)
            print(data_u, ultimo.to_json(), file=log)
        log.close()

        if is_Sell(penultimo, ultimo): 
            ts = telegram_sendText('{} - Venda'.format(data))
            with open(LOG_FILE, 'a') as log: print(datetime.now(), 'TELEGRAM OUTPUT: {}'.format(ts), file=log)
            log.close()

        if is_Buy(penultimo, ultimo): 
            ts = telegram_sendText('{} - Compra'.format(data))
            with open(LOG_FILE, 'a') as log: print(datetime.now(), 'TELEGRAM OUTPUT: {}'.format(ts), file=log)
            log.close()
    else:
        with open(LOG_FILE, 'a') as log: print(datetime.now(), "ERROR GET TRADES!", file=log)
        log.close()


def job():
    t = threading.Thread(target=worker)
    t.start()


# Sync
segundos = (60 - int(datetime.strftime(datetime.now(), '%S')) + 30) % 60
for s in range(segundos, 0, -1):
    print('\rComeça em {}s'.format(s), end='')
    sleep(1)

with open(LOG_FILE, 'a') as log: print(datetime.now(), 'STARTING...', file=log)
log.close()

while True:
    # data = data+timedelta(minutes=2)
    
    job()
    sleep(120)
