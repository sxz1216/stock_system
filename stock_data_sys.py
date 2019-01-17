#!/Users/sunxuanzhi/anaconda3/bin/python3
# -*- coding:utf-8 -*-
#author:xuanzhi

import requests
import random
from bs4 import BeautifulSoup as bs
import time,os
import redis
import csv
import pymongo
import pandas as pd
import json

class Stock_data_sys():
    def __init__(self):
        self.rds = redis.Redis(host='localhost', port = 6379, db = 0)   # 连接redis db0

    def get_stock_names(self, file_csv_path='stock_names.csv', _type=None):
        """
        通过东方财富网上爬取股票的名称代码,并存入redis数据库和本地csv文档
        """
        root_url = "http://quote.eastmoney.com/stocklist.html"
        headers = {
                'Referer': 'http://quote.eastmoney.com',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'
            }
        response = requests.get(root_url, headers=headers).content.decode('gbk')   # 网站编码为gbk 需要解码
        self.write_csv_redis(response,file_csv_path,_type)
    def write_csv_redis(self, response, file_csv_path, _type):
        soup = bs(response, 'lxml')
        all_ul = soup.find('div', id='quotesearch').find_all('ul')   # 获取两个ul 标签数据
        with open(file_csv_path, 'w', encoding='utf-8',newline = '') as f:
            writer = csv.writer(f,)
            for ul in all_ul:
                all_a = ul.find_all('a')            # 获取url下的所有的a标签
                for a in all_a:
                    row = a.text
                    if _type != 'temp':
                        self.rds.rpush('stock_names',row)  # a.text 为a标签中的text数据  rpush将数据右侧插入数据库
                    writer.writerow([row])

    #检查股票名称的合法性
    def check_stcok_name(self, stock_name_cn):
        #先检查redis中是否已经爬取
        _stocks = self.rds.lrange('stock_names',0,-1)
        for i in _stocks:
            if stock_name_cn.encode('utf-8') in i:
                print(i.decode('utf-8'), ' 是合法的。')
                return True, i.decode('utf-8')
        #若redis中不存在,则重新在网页查找
        self.get_stock_names('temp.csv', _type='temp')
        with open('temp.csv', 'r', encoding='utf-8',newline = '') as f:
            reader = csv.reader(f)
            rows = [row[0] for row in reader]
        for j in rows:
            if stock_name_cn in j:
                print(j.decode('utf-8'), ' 是合法的。')
                self.rds.rpush('stock_names',j)
                return True, j.decode('utf-8')
        print(stock_name_cn, '不合法。')
        return False, stock_name_cn

    def download_history(self, stock_name, start_time, end_time):
        headers = {
                'Referer': 'http://quotes.money.163.com/',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.81 Safari/537.36'
            }
        stock_code = stock_name.split('(')[1].split(')')[0]
        # 由于东方财富网上获取的代码一部分为基金，无法获取数据，故将基金剔除掉。
        # 沪市股票以6开头，深市以0、3开头。
        # 另外获取data的网址股票代码 沪市前加0， 深市前加1
        if int(stock_code[0]) in [0, 2, 3, 6]:
            if int(stock_code[0]) == 6:
                stock_code_new = '0' + stock_code
            elif int(stock_code[0]) in [0, 3] and stock_code[:2] != '03':
                stock_code_new = '1' + stock_code
            else:
                return None
        else:
            return None
        #print(stock_code_new)
        times = 5
        while times <= 5:
            try:                
                stock_url = 'http://quotes.money.163.com/trade/lsjysj_{}.html'.format(stock_code)
                respones = requests.get(stock_url, headers=headers).text
                soup = bs(respones, 'lxml')
                first_market_time = soup.find('input', {'name': 'date_start_type'}).get('value').replace('-', '')    # 获取上市时间
                print('上市时间为: ',first_market_time)
                today_time = soup.find('input', {'name': 'date_end_type'}).get('value').replace('-', '')        # 获取今日时间
                time.sleep(random.choice([1, 2]))                                                             # 两次访问之间休息1-2秒
                download_url = "http://quotes.money.163.com/service/chddata.html?code={}&start={}&end={}&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(stock_code_new, start_time, end_time)
                data = requests.get(download_url, headers=headers)

                file_name = os.getcwd() +'/data_files/stock_history/{0}/{1}.csv'.format(start_time+'_'+end_time, stock_name)
                with open(file_name, 'wb') as f:                                 #保存数据
                    for chunk in data.iter_content(chunk_size=10000):
                        if chunk:
                            f.write(chunk)
                    print("{0}  {1}--{2}  数据已经下载完成".format(stock_name ,start_time, end_time))
                break
            except:
                times -= 1
                time.sleep(1)                         


    def get_stock_historydata_csv(self, all_data=False, stock_names=None, start_time='20180101', end_time='20180930'):
        if all_data:
            stock_names = [i.decode('utf-8') for i in  self.rds.lrange('stock_names',327,-1)]
        else:
            temp = []
            for stock_name_cn in stock_names:
                print(stock_name_cn)
                _, stock_name = self.check_stcok_name(stock_name_cn)
                if _:
                    temp.append(stock_name)
            stock_names = temp
        self.os_path('stock_history/', start_time, end_time)
        for stock_name in stock_names:
            self.download_history(stock_name, start_time, end_time)

    def os_path(self, p, start_time, end_time):
        path = os.getcwd() + '/data_files/' + p + start_time+'_'+end_time
        if os.path.exists(path):
            return True
        else:
            os.makedirs(path)






if __name__ == '__main__':
    client = Stock_data_sys()
    #client.check_stcok_name('孙轩志')
    client.get_stock_historydata_csv(True)


