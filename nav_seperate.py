import gc
import sys, time, datetime, os, importlib, math, pandas as pd, numpy as np
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
global fn,stockpricehis,result
result = []

fn = 'CSVKlineHist.csv'
stockpricehis = pd.read_csv(fn, parse_dates=[1,2])
stockpricehis.set_index(['code','tradingday','timekey'], inplace=True)
stockpricehis.sort_index(inplace=True)


def nav_seperate(month, N):
    global fn,stockpricehis
    #for month in month:
    portfolio_nav_files =[]
    for files in list(filter(lambda x: x.startswith(month), os.listdir('./Portfolio/'))):

        portfolio_nav = []
        portfolio = pd.read_csv('./Portfolio/' + files,header=None)[0].values
        date = datetime.datetime.strptime(files[:-4],'%Y%m%d')
        tradingdays = list(stockpricehis.index.levels[1])
        while date not in tradingdays and date <= tradingdays[-1]:
            date = date + datetime.timedelta(days=1)
        if date > tradingdays[-1]:
            continue
        date30 = tradingdays[min(tradingdays.index(date)+ N,len(tradingdays)-1)]
        temp = stockpricehis.query("code in @portfolio and tradingday >= @date and tradingday <= @date30").reset_index('timekey')[['c']]
        for i in portfolio:
            nav = temp.loc[i]
            if len(nav) > 0:
                nav['i'] = nav['c'] / nav['c'][0]
                nav = nav[['i']][1:].T
                nav.index = [i]
            else:
                nav = nav.T
                nav.index = [i]
            portfolio_nav.append(nav)
        NAV = pd.DataFrame(pd.concat(portfolio_nav).mean()).T
        index_nav = [datetime.datetime.strptime(files[:-4],'%Y%m%d').strftime('%Y-%m-%d')+' portfolio NAV']
        NAV.index = index_nav
        save_nav = pd.concat(portfolio_nav+[NAV])
        save_nav = save_nav.rename(columns = {i:'Day_'+ str(list(save_nav.columns).index(i)).rjust(3,'0') for i in save_nav.columns})
        K = len(save_nav.columns)
        save_nav['NAV_Max'] = save_nav.max(axis = 1)
        save_nav['NAV_Min'] = save_nav.min(axis = 1)
        save_nav.loc[index_nav, 'Max_Day'] = list(save_nav.loc[index_nav].values[0][:K]).index(save_nav.loc[index_nav,'NAV_Max'].values[0])

        save_nav.loc[index_nav, 'Min_Day'] = list(save_nav.loc[index_nav].values[0][:K]).index(save_nav.loc[index_nav,'NAV_Min'].values[0])
        save_nav['Win'] = (save_nav['NAV_Max'] + save_nav['NAV_Min']).apply(lambda x: 1 if x > 2 else 0 )
        save_nav.loc[index_nav, 'Win'] = save_nav.Win.mean()
        save_nav.loc[index_nav,'Max'] = save_nav.NAV_Max[:-1].max()
        save_nav.loc[index_nav,'Max_stock'] = list(save_nav.index)[list(save_nav.NAV_Max.values).index(save_nav.loc[index_nav,'Max'].values[0])]
        save_nav.loc[index_nav,'Min'] = save_nav.NAV_Min[:-1].min()
        save_nav.loc[index_nav,'Min_stock'] = list(save_nav.index)[list(save_nav.NAV_Min.values).index(save_nav.loc[index_nav,'Min'].values[0])]
        portfolio_nav_files.append(save_nav)
    pd.concat(portfolio_nav_files).to_csv('./Result/'+month + '.csv')

def when_done(r):
    global result
    result.append(r.result())

def read_csv():
    filelist = os.listdir('./Result/')
    nav_result = []
    for files in filelist:
        nav_data = pd.read_csv('./Result/'+files)
        nav_data['NAV'] = nav_data['Unnamed: 0'].str.contains('NAV')
        nav_result.append(nav_data.query('NAV == True').drop('NAV',axis = 1))
    nav = pd.concat(nav_result).set_index('Unnamed: 0')
    mn = pd.DataFrame(nav.mean()).T
    mn.index = ['mean']
    pd.concat([nav, mn]).to_csv('nav_his.csv')

if __name__ == '__main__':
    gc.collect()

    month = ['201701','201702','201703','201704','201705','201706','201707','201708','201709','201710','201711','201712']
    N = 30
    time_start = time.time()
    with ProcessPoolExecutor() as pool:
        for month in month:
            future_result = pool.submit(nav_seperate, month, N)
            future_result.add_done_callback(when_done)
    time_end=time.time()
    print (time_end-time_start, 's')   #testing the speed
    read_csv()
