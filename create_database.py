from pandas_datareader import data as web
import pandas as pd

def load_tckr(ticker, start, end): # load stock history dataset from API
    df = web.DataReader(name=ticker, data_source='yahoo', start=start, end=end).reset_index()
    
    return df

def result_dia(x): #classify wins, losses and draws
    if x == 0: return 0
    if x < 0: return 1
    if x > 0: return 2

def enrich(df): #enrich data with useful indexes
    lower = [str.lower(c) for c in df.columns.to_list()]
    df.columns = lower
    df['return'] = df.close - df.open #daily movement
    df['pct_change'] = 100*df['return']/df['open']
    df['result_dia'] = df['return'].apply(result_dia)
    
    return df

def get_ticker_list(ticker_path):#create stock ticker list from file 
    header = ['ticker', 'company', 'tp', 'theory_qnt', 'part'] 
    tick = pd.read_excel(ticker_path, names=header)
    t = tick['ticker'].to_numpy()
    l_tick = t+'.SA'
    l_tick = l_tick.tolist()
    
    return l_tick

def create_database(start,end, ticker_path): #wrap up
    l_tick = get_ticker_list(ticker_path)
    df = pd.DataFrame()

    for t in l_tick:
        try: 
            load = load_tckr(t,start,end)
            trat = enrich(load)
            tam = trat.shape[0]
            ind_tick = pd.DataFrame([t]*tam)
            ind_tick.columns=['ticker']
            temp = pd.concat([ind_tick,trat],axis=1)
        except:
                print(f'Ticker {t} not found')
        if df.empty:
            df = temp
        else:
            df = pd.concat([df,temp],axis=0)

    return df  
