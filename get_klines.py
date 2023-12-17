import requests, json
import pandas as pd


def get_all_symbols():
    url = "https://open-api.bingx.com/openApi/swap/v2/quote/contracts"
    res = requests.get(url)
    symbols = []
    res = res.json()['data']
    for r in res:
        symbols.append(r['symbol'])
    df = pd.DataFrame(symbols, columns=['symbol'])
    df.to_csv('symbols.csv', index=False, header=False)

# get_all_symbols()


def get_klines(symbol: str):
    url = f"https://open-api.bingx.com/openApi/swap/v3/quote/klines?symbol={symbol}&interval=1h&limit=1440"
    res = requests.get(url)
    res = res.json()['data']
    return res

symbols = pd.read_csv("symbols.csv", header=None)
symbols.columns = ['sym']

def get_all_klines():
    df = pd.DataFrame()

    for symbol in symbols.sym.values:
        print(symbol)
        try:
            kline = get_klines(f"{symbol}")
            df2 = pd.DataFrame(kline)
            df2.insert(loc=0, column='symbol', value=symbol)
            df = pd.concat([df, df2], axis=0, ignore_index=True)
        except Exception as e:
            print(e)
    df.to_csv("klines.csv", index=False)

get_all_klines()


df = pd.read_csv('klines.csv')
print(df)