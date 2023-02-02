import pandas as pd
import os
import ccxt
import time
from datetime import datetime, timedelta
import pytz


def add_RA(df, win_size, col, name):
    df[name] = pd.Series.rolling(df[col], window=win_size, center=False).mean()


def find_price_spikes(df, p_thresh, win_size):
    # add rolling average column to df
    pRA = str(win_size)+'m Close Price RA'
    add_RA(df, win_size, 'Close', pRA)

    # find spikes
    p_threshold = p_thresh*df[pRA]   # p_thresh increase in price
    # where the high is at least p_thresh greater than x-hr RA
    p_spike_mask = df['High'] > p_threshold
    df_price_spike = df[p_spike_mask]
    return (p_spike_mask, df_price_spike)


def find_volume_spikes(df, v_thresh, win_size):
    # add rolling/moving average column to dataframe
    vRA = str(win_size)+'m Volume RA'
    add_RA(df, win_size, 'Volume', vRA)

    # find spikes
    vol_threshold = v_thresh * df[vRA]  # v_thresh times increase in volume
    # where the vol is at least v_thresh greater than the x-hr RA
    vol_spike_mask = df['Volume'] > vol_threshold
    df_vol_spike = df[vol_spike_mask]

    return (vol_spike_mask, df_vol_spike)


def analyzesymbol(df, exchange, symbol, v_thresh=3, p_thresh=1.03, win_size=30, c_size='1m'):

    # find spikes
    vmask, vdf = find_volume_spikes(df, v_thresh, win_size)
    num_v_spikes = vdf.shape[0]

    pmask, pdf = find_price_spikes(df, p_thresh, win_size)
    num_p_spikes = pdf.shape[0]

    vp_combined_mask = (vmask) & (pmask)
    vp_combined_df = df[vp_combined_mask]
    vp_combined_df['symbol'] = symbol
    if(os.path.isfile('output.csv')):
        vp_combined_df.to_csv('output.csv', mode='a',
                              index=False, header=False)

    else:
        vp_combined_df.to_csv('output.csv', mode='a', index=False, header=True)

    print('completed')


def create_ohlcv_df(data):
    header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='ms', origin='unix')
    df['Timestamp'] = df['Timestamp'].apply(lambda x: datetime(
        x.year, x.month, x.day, x.hour, x.minute, x.second)+timedelta(hours=5, minutes=30))

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
    while True:
        for symbol in usdt_symbols:
            for attempt in range(5):  # 5 attempts max
                try:
                    if flag == 0:
                        print('Pulling and Analysing: ', exchange, ':', symbol,
                              '[{}/{}]'.format(count, len(usdt_symbols)), end='......')
                    data = exc_instance.fetch_ohlcv(
                        symbol, c_size, from_timestamp, n_candles)
                    # data=exc_instance.fetch_ticker('BTC'+'/'+'USDT')

                    # if missing candles then skip this pair
                    if len(data) < n_candles and (skip is True):
                        if flag == 0:
                            print('...nodata')
                        flag = 1
                        continue

                    # create df
                    df = create_ohlcv_df(data)

                    analyzesymbol(df, exchange, symbol)

                    # # save csv
                    # symbol=symbol.replace("/","-")
                    # filename=newpath+'{}_{}.csv'.format(exchange,symbol)
                    # df.to_csv(filename)

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
            time.sleep((exc_instance.rateLimit/msec)+3)

    # if len(missing_symbols) != 0:
    #     print('Unable to obtain: ', missing_symbols)

    # return missing_symbols


def startwork():
    UTC = pytz.utc
    curr_time = datetime.now(UTC)-timedelta(minutes=60)
    from_date = str(curr_time)
    exchanges = ['binance']
    for e in exchanges:
        pull_data(e, from_date, 60, '1m', 'data', skip=True)
