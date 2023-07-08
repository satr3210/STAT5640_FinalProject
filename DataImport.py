import pickle
import requests
import bs4 as bs
import pandas as pd
import pandas_datareader as web
import datetime as dt
import os
import yfinance as yf
from tqdm import tqdm


def fun(x):
    """A helper function to round a value up or down.

                    Parameters
                    ----------
                    x: float

                    Returns
                    -------
                    1 or 2 : int

                    Notes
                    -----

                    References
                    ------
                    """
    if x>0:
        return 1
    else:
        return 0


def save_sp500_tickers():
    """A function to get the ticker symbols of every company in the S&P500. Will save them in a file.

                Parameters
                ----------
                NONE

                Returns
                -------
                tickers : list
                    A list of numpy arrays which contain the eigenvalues for each of the provided adjacency matrices

                Notes
                -----

                References
                ------
                """
    # get the list
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.find_all('td')[0].text.replace('\n', '')
        if "." in ticker:
            ticker = ticker.replace('.', '-')
            print('ticker replaced to', ticker)
        tickers.append(ticker)
    with open("sp500tickers.pickle", "wb") as f:
        pickle.dump(tickers, f)
    return tickers


def get_data_from_yahoo(reload_sp500=False):
    """A function to get the price data for each ticker from Yahoo Finance.

                   Parameters
                   ----------
                   reload_sp500: bool
                        Default False. A bool to identify weather or not redownload the ticker names

                   Returns
                   -------
                   tickers : list
                       A list of numpy arrays which contain the eigenvalues for each of the provided adjacency matrices

                   Notes
                   -----

                   References
                   ------
                   """
    if reload_sp500:
        tickers = save_sp500_tickers()
    # making a folder called stocks_df
    else:
        with open('sp500tickers.pickle', 'rb') as f:
            tickers = pickle.load(f)
    if not os.path.exists('stock_dfs'):
        os.makedirs('stock_dfs')

    start = dt.datetime(2000, 1, 1)
    end = dt.date.today()

    for ticker in tickers:
        print(ticker)
        if not os.path.exists('stock_dfs/{}.csv'.format(ticker)):
            df = yf.download(ticker, start, end)
            df.to_csv('stock_dfs/{}.csv'.format(ticker))
        else:
            print('All Ready Have {}'.format(ticker))


def compile_data():
    """A function to compile all the stock data into one csv file with only the return and return.

                     Parameters
                     ----------
                     None

                     Returns
                     -------
                     sp500_joined_returns : csv file
                         A csv of our return data

                     Notes
                     -----

                     References
                     ------
                     """
    with open("sp500tickers.pickle", "rb") as f:
        tickers = pickle.load(f)

    main_df = pd.DataFrame()

    for count, ticker in tqdm(enumerate(tickers)):
        df = pd.read_csv('stock_dfs/{}.csv'.format(ticker))
        df.set_index('Date', inplace=True)

        df['Return'] = df['Close'] - df['Open']
        df.rename(columns={'Return': ticker}, inplace=True)
        df.drop(['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'], 1, inplace=True)

        if main_df.empty:
            main_df = df
        else:
            main_df = main_df.join(df, how='outer')

        #if count % 10 == 0:
            #print(count)

    print(main_df.head())
    main_df.to_csv('sp500_joined_returns.csv')


def get_spy():
    """A function to get the return data for SPY and convert it to 1 or 0 if the return was positive or not

                    Parameters
                    ----------
                    NONE

                    Returns
                    -------
                    SPY.csv : csv file
                        A csv file containing the date, and a 0 or 1 representing SPYs return that day.

                    Notes
                    -----

                    References
                    ------
                    """
    start = dt.datetime(2000, 1, 1)
    end = dt.date.today()
    df = yf.download('SPY', start, end)
    df['Return'] = df['Close'] - df['Open']
    df.drop(['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'], axis=1, inplace=True)
    df['SPYUpDown'] = df['Return'].map(lambda x: fun(x))
    df.drop(['Return'], axis=1, inplace=True)

    if not os.path.exists('stock_dfs/SPY.csv'):
        df.to_csv('stock_dfs/SPY.csv')
    else:
        print('All Ready Have SPY')
