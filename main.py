import threading
import pandas as pd
import requests
from datetime import datetime, timedelta
from time import sleep
import schedule
import tokens

LOG_FILE = 'DJI.log'


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
    interval = 2 # minutes
    days = 1
    token = tokens.WORLDTRADINGDATA_TOKEN
    url = 'https://intraday.worldtradingdata.com/api/v1/intraday?symbol={}&interval={}&range={}&api_token={}'.format(simbol, interval, days, token)
    # url = 'https://intraday.worldtradingdata.com/api/v1/intraday?symbol=SNAP&interval=1&range=1&api_token=demo'
    try:
        dji = requests.get(url)
        trades = dji.json()
        return trades['intraday']
    except:
        return None


def schedule_job():
    with open(LOG_FILE, 'a') as log: print(datetime.now(), "Scheduling job...", file=log)
    log.close()

    schedule.every(2).tag(2).minutes.do(job)    


def clear_job():
    with open(LOG_FILE, 'a') as log: print(datetime.now(), "Clear job...", file=log)
    log.close()

    schedule.clear(2)


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
        df = pd.DataFrame.from_dict(trades, orient='index')
        df = df[::-1]
        df = get_bb_rsi(df)
        # with pd.option_context('display.max_rows', None, 'display.width', 300):
        #     print(df)
    
        data = datetime.now()
        # data = datetime.strptime('2020-04-02 12:00:00', '%Y-%m-%d %H:%M:%S')
        data_2 = datetime.strftime(data-timedelta(hours=1, minutes=2), '%Y-%m-%d %H:%M:00')
        data_4 = datetime.strftime(data-timedelta(hours=1, minutes=4), '%Y-%m-%d %H:%M:00')
        penultimo = df.loc[data_4].astype('float')
        ultimo    = df.loc[data_2].astype('float')
        with open(LOG_FILE, 'a') as log: 
            print(data_4, penultimo.to_json(), file=log)
            print(data_2, ultimo.to_json(), file=log)
            # print(data_4, penultimo.to_json())
            # print(data_2, ultimo.to_json())
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
for s in range(segundos, 0, -1): sleep(1)

with open(LOG_FILE, 'a') as log: print(datetime.now(), 'STARTING...', file=log)
log.close()

schedule.every().monday.at("10:30").do(schedule_job)
schedule.every().monday.at("17:00").do(clear_job)
schedule.every().tuesday.at("10:30").do(schedule_job)
schedule.every().tuesday.at("17:00").do(clear_job)
schedule.every().wednesday.at("10:30").do(schedule_job)
schedule.every().wednesday.at("17:00").do(clear_job)
schedule.every().thursday.at("10:30").do(schedule_job)
schedule.every().thursday.at("17:00").do(clear_job)
schedule.every().friday.at("10:30").do(schedule_job)
schedule.every().friday.at("17:00").do(clear_job)

while True:
    with open(LOG_FILE, 'a') as log: 
        print('\n{}'.format(datetime.now()), 'SCHEDULE:', file=log)
        for s in schedule.jobs: print(datetime.now(), s, file=log)
        print('================================================================================', file=log)
    log.close()

    schedule.run_pending()
    sleep(60)
