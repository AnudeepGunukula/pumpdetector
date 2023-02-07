import warnings
import pandas as pd
import os
import ccxt
import time
from datetime import datetime, timedelta
import pytz
import requests as re
from fake_headers import Headers
pd.options.mode.chained_assignment = None
warnings.filterwarnings('ignore', category=DeprecationWarning)
warnings.filterwarnings('ignore', category=FutureWarning)

url = 'https://Pdetector.anudeep447.repl.co'


def add_RA(df, win_size, col, name):
    df[name] = pd.Series.rolling(df[col], window=win_size, center=False).mean()


def parse_msg(df, p_thresh, win_size):

    scantime = str((datetime.now(pytz.utc)+timedelta(hours=5,
                   minutes=30)).replace(second=0, microsecond=0))[11:16]
    msg = 'scantime is '+scantime+' \n----------------------------\n'
    for index, row in df.iterrows():
        occurtime = str(row['Timestamp'])[11:16]
        msg += f"{row['symbol']} inc by {p_thresh}% within {win_size} min,at {occurtime}\n-------------------------\n"
    return msg


def send_to_telbot(df, p_thresh, win_size):

    msg = parse_msg(df, p_thresh, win_size)
    try:
        header = Headers(
            # generate any browser & os headeers
            headers=False  # don`t generate misc headers
        )
        re.post(url, json={"msg": msg}, headers=header.generate())
    except:
        return


def find_price_spikes(df, p_thresh, win_size):
    # add rolling average column to df
    pRA = str(win_size)+'m Close Price RA'
    add_RA(df, win_size, 'Close', pRA)

    # find spikes
    p_threshold = p_thresh*df[pRA]   # p_thresh increase in price
    # where the high is at least p_thresh greater than x-hr RA
    p_spike_mask = df['High'] > p_threshold
    df_price_spike = df[p_spike_mask]
    df_price_spike.drop([pRA], axis=1, inplace=True)

    return (p_spike_mask, df_price_spike)


# def save_csv(df, p_thresh, win_size):
#     file_name = 'output_'+str(p_thresh)+'_'+str(win_size)+'.csv'

#     if not os.path.isfile(file_name):
#         df.to_csv(file_name, mode='w', index=False, header=True)
#     else:
#         exist_df = pd.read_csv(file_name)

#         final_df = pd.concat([exist_df, df], ignore_index=True)
#         final_df = final_df.drop_duplicates()
#         final_df['Timestamp'] = pd.to_datetime(final_df['Timestamp'])
#         final_df.sort_values(by='Timestamp', ascending=False, inplace=True)

#         if not exist_df.equals(final_df):
#             print(exist_df)
#             print(final_df)
#             final_df.to_csv(file_name, mode='w',
#                             index=False, header=True)
#             send_to_telbot(final_df.head(50), p_thresh, win_size)

def analyzesymbol(orig_df, exchange, symbol, v_thresh=3, p_thresh=1.02, win_size=15, c_size='1m'):

    pw_dic_q = {
        1.04: 15,
        1.05: 15,
        1.06: 15,
        1.07: 15,
        1.08: 15,
        1.09: 15,
        1.1: 15
    }

    pw_dic_h = {
        1.06: 30,
        1.07: 30,
        1.08: 30,
        1.09: 30,
        1.1: 30
    }

    temppdf = pd.DataFrame()

    for p_thresh, win_size in pw_dic_q.items():
        df = orig_df.copy()
        pmask, pdf = find_price_spikes(df, p_thresh, win_size)
        # print(pdf)
        if pdf.empty and temppdf.empty:
            break
        elif pdf.empty and (not temppdf.empty):
            pdf = temppdf.copy()
            break
        else:
            temppdf = pdf.copy()
            continue
    pdf['symbol'] = symbol
    # pdf['scantime'] = (datetime.now(
    # pytz.utc)+timedelta(hours=5, minutes=30)).replace(second=0, microsecond=0)

    if not pdf.empty:
        #save_csv(pdf, p_thresh, win_size)
        send_to_telbot(pdf.head(1), p_thresh, win_size)

    tempppdf = pd.DataFrame()
    for p_thresh, win_size in pw_dic_h.items():
        sdf = orig_df.copy()
        ppmask, ppdf = find_price_spikes(sdf, p_thresh, win_size)
        if ppdf.empty and tempppdf.empty:
            break
        elif ppdf.empty and (not tempppdf.empty):
            ppdf = tempppdf.copy()
            break
        else:
            tempppdf = ppdf.copy()
            continue
    ppdf['symbol'] = symbol
    # ppdf['scantime'] = (datetime.now(
    # pytz.utc)+timedelta(hours=5, minutes=30)).replace(second=0, microsecond=0)
    if not ppdf.empty:
        #save_csv(ppdf, p_thresh, win_size)
        send_to_telbot(ppdf.head(1), p_thresh, win_size)
    print('completed')


def create_ohlcv_df(data):
    header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', origin='unix')
    df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime(
        x.year, x.month, x.day, x.hour, x.minute, x.second)+timedelta(hours=5, minutes=30))

    df.drop(['Open', 'Low'], axis=1, inplace=True)

    return df


def pull_data(exchange, from_date, n_candles, c_size, f_path, skip=False):
    count = 1
    msec = 1000
    hold = 30

    # # list to print any symbols missed at final output
    # missing_symbols = []
    usdt_symbols = []

    # load the exchange
    exc_instance = getattr(ccxt, exchange)()
    exc_instance.load_markets()

    # converting to unix timestamp from normal date time

    from_timestamp = exc_instance.parse8601(from_date)

    # filtering usdt pairs
    for symbol in exc_instance.symbols:
        if 'USDT' in symbol:
            if not (('UP' in symbol) or ('DOWN' in symbol)):
                usdt_symbols.append(symbol)
    # pull ohlcv
    flag = 0
    start_time = time.time()
    for symbol in usdt_symbols:
        for attempt in range(5):  # 5 attempts max
            try:
                if flag == 0:
                    print('Pulling and Analysing: ', exchange, ':', symbol,
                          '[{}/{}]'.format(count, len(usdt_symbols)), end='......')
                data = exc_instance.fetch_ohlcv(
                    symbol, c_size, from_timestamp, n_candles)

                # if missing candles then skip this pair
                if len(data) < n_candles and (skip is True):
                    if flag == 0:
                        print('...nodata')
                    flag = 1
                    continue

                # create df
                df = create_ohlcv_df(data)

                analyzesymbol(df, exchange, symbol)

            except(ccxt.ExchangeError, ccxt.AuthenticationError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout, IndexError) as error:
                print('Got an Error', type(error).__name__,
                      error.args, ',retrying in', hold, 'seconds...')
                # time.sleep(hold)
            else:  # if no error, proceed to next symbol
                break
        else:  # we failed all attempts (enters this block if exit happen from loop without break statement means after 5 attempts)
            #print('All attempts failed, skipping: ', symbol)
            # missing_symbols.append(symbol)
            flag = 0
            continue

        count += 1  # counting how many symbols worked

        # wait for rate limit
        # rate limit + 5sec to just to be safe
        time.sleep((exc_instance.rateLimit/msec))
    end_time = time.time()-start_time
    print('taken ', end_time, 'seconds to complete the scan')
    count = 1


def startwork():
    exchanges = ['binance']
    while True:
        UTC = pytz.utc
        curr_time = datetime.now(UTC)-timedelta(minutes=60)
        from_date = str(curr_time)
        for e in exchanges:
            pull_data(e, from_date, 60, '1m', 'data', skip=True)
