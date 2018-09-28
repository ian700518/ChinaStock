#!/usr/bin/python3

import time, re, sys, os
import urllib
import requests
import json
from bs4 import BeautifulSoup
from pymongo import MongoClient

"""
Get Shanghai and Shenzhen stock list
http://quote.eastmoney.com/stocklist.html
"""
def GetChinaStockList(path, GCSdb) :
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15'}
    try :
        request_list = requests.get(path, headers = headers)
    except requests.exceptions.Timeout :
        print('Request stock list data timeout~~!!')
    except requests.exceptions.TooManyRedirects :
        print('Request stock list data url maybe bad~~!')
    except requests.exceptions.RequestException as e:
        print(e)
    request_list.encoding = 'gbk'
    stock_soup = BeautifulSoup(request_list.text, 'html5lib')
    stock_href_list = stock_soup.findAll(href = re.compile(r'(sh6|sz0|sz3|sh51)\d{4,5}\.html'))
    stock_list = []
    for i in stock_href_list :
        Stock_Name_Code = i.get_text()
        if Stock_Name_Code.find('(') != -1 :
            stock_list.append(i.get_text())
    for i, j in enumerate(stock_list) :
        stock_list_dict = {'idx': '', 'Stock_Name_Code': '', 'Col_name': '', 'Stock_History_Status': ''}
        stock_list_dict['idx'] = i
        stock_list_dict['Stock_Name_Code'] = j
        item = GCSdb.SLcol.find_one({'Stock_Name_Code': j})
        if item is not None :
            if item['Stock_Name_Code'] == j :
                #print('{} has in database~~!'.format(j))
                continue
        FirstSym = j.find('(')
        LastSym = j.rfind(')')
        colname = '{:06d}'.format(int(j[FirstSym + 1 : LastSym]))
        if colname[0] is '5' or colname[0] is '6' :
            Col_Name = 'sh' + colname
        else :
            Col_Name = 'sz' + colname
        GCSdb.SLcol.update_one({'Col_name': Col_Name}, {'$set': {'idx': i, 'Stock_Name_Code': j, 'Col_name': Col_Name, 'Stock_History_Status': 'None'}}, upsert=True)

"""
Get Stock history data with China Stock List
http://money.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/601006.phtml?year=2007&jidu=1
"""
def GetStockHistory(GCSdb) :
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0 Safari/605.1.15'}
    Stock_his_dict = {}
    for i in GCSdb.SLcol.find() :
        TempCode = i['Col_name']
        StockNC = i['Stock_Name_Code']
        StockHisStatus = i['Stock_History_Status']
        Stockidx = i['idx']
        #print('i is {}, Status is {}'.format(i, StockHisStatus))
        if StockHisStatus == 'OK' :
            print('{} history is download ok~~!!'.format(StockNC))
            continue
        SCode = int(TempCode[2:])
        cur_year = int(time.strftime("%Y", time.localtime()), 10)
        Stock_StartTime = time.time()
        col_scode = GCSdb[TempCode]
        list_count = 0
        print('vvvvvvvvvvvvvvvvvvvv')
        print('Start time is {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(Stock_StartTime))))
        for year in range(2000, cur_year + 1) :
            for season in range(1, 5) :
                try :
                    Shistory = 'http://money.finance.sina.com.cn/corp/go.php/vMS_MarketHistory/stockid/{:06d}.phtml?year={:4d}&jidu={:d}'.format(SCode, year, season)
                    request_stock_his = requests.get(Shistory, headers = headers)
                except requests.exceptions.Timeout :
                    print('Request history data timeout~~!!')
                    time.sleep(1)
                    continue
                except requests.exceptions.TooManyRedirects :
                    print('Request history data url maybe bad~~!')
                    time.sleep(1)
                    continue
                except requests.exceptions.RequestException as e:
                    print(e)
                    time.sleep(1)
                    continue
                request_stock_his.encoding = 'gbk'
                stock_his_soup = BeautifulSoup(request_stock_his.text, 'html5lib')      # used html5lib mode will be find correct source code for web page
                SHisTable = stock_his_soup.find('table', id = 'FundHoldSharesTable')
                if SHisTable is not None :
                    SHisTable_body = SHisTable.find('tbody')
                    SHisrows = SHisTable_body.findAll('tr')
                    shlcols = []
                    if SHisrows is not None :
                        for row in SHisrows:
                            cols = row.findAll('td')
                            if cols is not None :
                                for ele in cols :
                                    if ele.get_text().find('日期') != -1 :
                                        break
                                    shlcols.append(ele.get_text().strip('(\n|\t)'))
                                if len(shlcols) != 0 :
                                    list_count += 1
                                    col_scode.update_one({'日期': shlcols[0]}, {'$set': {'日期': shlcols[0], '开盘价': shlcols[1], '最高价': shlcols[2], '收盘价': shlcols[3],
                                                            '最低价': shlcols[4], '交易量(股)': shlcols[5], '交易金额(元)': shlcols[6]}}, upsert=True)
                                shlcols = []
                else :
                    print('{}, {}-{}, Can not find data table'.format(StockNC, year, season))
                    pass
                time.sleep(1)
        GCSdb.SLcol.update_one({'Col_name': TempCode}, {'$set': {'idx': Stockidx, 'Stock_Name_Code': StockNC, 'Col_name': TempCode, 'Stock_History_Status': 'OK'}}, upsert=True)
        print('End time is {}'.format(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())))
        print('Get {} success, total {} counts~~!!'.format(StockNC, list_count))
        print('^^^^^^^^^^^^^^^^^^^^')
        print(GCSdb.SLcol.find_one({'Col_name': TempCode}))


def main() :
    conn = MongoClient('localhost', 27017)
    Stock_list_db = conn.SlistDB
    GetChinaStockList('http://quote.eastmoney.com/stocklist.html', Stock_list_db)
    GetStockHistory(Stock_list_db)


if __name__ == "__main__" :
    main()
