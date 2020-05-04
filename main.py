import threading
import pandas as pd
import numpy as np
import requests
from datetime import datetime, timedelta
from time import sleep
import tokens

LOG_FILE = 'DJI.log'

def telegram_sendText(message):
    bot_token = tokens.TELEGRAM_TOKEN
    bot_chatID = '-1001435946491'
    send_text = 'https://api.telegram.org/bot' + bot_token + '/sendMessage?chat_id=' + bot_chatID + '&parse_mode=Markdown&text=' + message
    try:
        response = requests.get(send_text)
        return response.json()
    except:
        return 'ERROR TELEGRAM SEND!'
    

def get_TradeDJI(from_timestamp, to_timestamp):
    simbol = '^DJI'
    interval = 1 # minutes    
    token = tokens.FINNHUB
    url = 'https://finnhub.io/api/v1/stock/candle?symbol={}&resolution={}&from={}&to={}&token={}'.format(simbol, interval, from_timestamp, to_timestamp, token)
    try:
        dji = requests.get(url)
        return dji.json()
    except:
        return {'s': 'error'}


def get_bb_rsi(df):
    # BOLLINGER BANDS
    sma20 = df['close'].rolling(window=20).mean()
    df['sma20'] = sma20
    rs20 = df['close'].rolling(window=20).std()
    bb_up = sma20 + rs20 * 2
    bb_dw = sma20 - rs20 * 2
    df['bb_up'] = bb_up
    df['bb_dw'] = bb_dw

    # Relative Strength Index (Simple)
    periods=9
    u = (df['close'] - df['open']).to_frame()
    u.loc[df['close'] < df['open']] = 0
    d = (df['open'] - df['close']).to_frame()
    d.loc[df['close'] > df['open']] = 0
    rm_u = u.rolling(window=periods).mean()
    rm_d = d.rolling(window=periods).mean()
    # Relative Strength Index (Classic)
    for i in range(periods, df['close'].size): 
        rm_u.iloc[i] = (rm_u.iloc[i-1] * (periods-1) + u.iloc[i]) / periods
        rm_d.iloc[i] = (rm_d.iloc[i-1] * (periods-1) + d.iloc[i]) / periods
    ###
    rsi = 100 - (100 / (1 + rm_u / rm_d))
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

    segundos = int(datetime.strftime(datetime.now(), '%S'))
    tsnow00 = int(datetime.now().timestamp()) - segundos
    trades = get_TradeDJI(tsnow00-(42*60), tsnow00)
    
    if trades['s'] == 'ok':
        df = pd.DataFrame()
        i = 0
        t, o, c = [], [], []
        while i < len(trades['t']):
            t.append(trades['t'][i])
            o.append(trades['o'][i])
            c.append(trades['c'][i+1])
            i += 2
        df['time']  = t
        df['open']  = o
        df['close'] = c

        df = get_bb_rsi(df)
        # with pd.option_context('display.max_rows', None, 'display.width', 300):
        #     print(df)

        try:
            penultimo = df.iloc[-2].astype('float')
            ultimo    = df.iloc[-1].astype('float')
            with open(LOG_FILE, 'a') as log:
                print(datetime.fromtimestamp(penultimo['time']), penultimo.to_json(), file=log)
                print(datetime.fromtimestamp(ultimo['time']), ultimo.to_json(), file=log)
            log.close()

            if is_Sell(penultimo, ultimo):
                ts = telegram_sendText('Venda')
                with open(LOG_FILE, 'a') as log: print(datetime.now(), 'TELEGRAM OUTPUT: {}'.format(ts), file=log)
                log.close()

            if is_Buy(penultimo, ultimo):
                ts = telegram_sendText('Compra')
                with open(LOG_FILE, 'a') as log: print(datetime.now(), 'TELEGRAM OUTPUT: {}'.format(ts), file=log)
                log.close()
        
        except:
            with open(LOG_FILE, 'a') as log: print(datetime.now(), "Erro no Dataframe!!!", file=log)
            log.close()

    else:
        with open(LOG_FILE, 'a') as log: print(datetime.now(), "Erro get trades!!!", file=log)
        log.close()


def job():
    t = threading.Thread(target=worker)
    t.start()


# Sync
segundos = (60 - int(datetime.strftime(datetime.now(), '%S')) + 10) % 60
for s in range(segundos, 0, -1):
    print('\rComeça em {}s'.format(s), end='')
    sleep(1)

with open(LOG_FILE, 'a') as log: print(datetime.now(), 'STARTING...', file=log)
log.close()
telegram_sendText('{} - Starting...'.format(datetime.now()))

while True:
    job()
    sleep(120)
