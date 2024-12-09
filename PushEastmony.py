# -*- coding: utf-8 -*-
"""
Created on Tue Nov 26 23:31:30 2024

@author: devot
"""

import requests
import json
import re
import pandas as pd
import os

def sanitize_filename(filename):
    # 使用正则表达式替换文件名中的非法字符
    # 可以根据需要扩展非法字符列表
    return re.sub(r'[<>:"/\\|?*]', '#', filename)

def update_stock_code():
    # 设置 API 基本 URL
    base_url = "https://37.push2.eastmoney.com/api/qt/clist/get?" 
    # 设置查询参数
    params_list = [{ #上证交易所股票代码
            'pn': '1',   #--
            'pz': '50000',   #--
            'po': '0',   #--
            'np': '1',   #--
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',   #--
            'fltt': '2',   #--
            'invt': '2',   #--
            'dect': '1',   #--
            'wbp2u': '|0|0|0|web',   #--
            'fid': 'f12',   #--
            'fs': 'm:1+t:2,m:1+t:23',   #--
            'fields': 'f12,f14',   #--
            '_': '1732636342041',   #--
        },{  #深圳交易所股票代码
            'pn': '1',   #--
            'pz': '50000',   #--
            'po': '1',   #--
            'np': '1',   #--
            'ut': 'bd1d9ddb04089700cf9c27f6f7426281',   #--
            'fltt': '2',   #--
            'invt': '2',   #--
            'dect': '1',   #--
            'wbp2u': '|0|0|0|web',   #--
            'fid': 'f3',   #--
            'fs': 'm:0 t:6,m:0 t:80',   #--
            'fields': 'f12,f14',   #--
            '_': '1732636342041',  
        }]

    df_code = pd.DataFrame()
    
    for params in params_list:
        # 发送 GET 请求并传递参数
        response = requests.get(base_url, params=params)

        # 确保请求成功
        if response.status_code == 200:
            json_data = response.text  # 获取响应文本（JSONP 数据）

            # 尝试将 JSON 数据解析为 Python 字典
            try:
                data = json.loads(json_data)
                print("JSON 解析成功！")
                
                # 提取 klines 数据并转换为 DataFrame
                diff_data = data['data']['diff']

                # 创建 DataFrame
                df = pd.DataFrame(diff_data)

                df_code = pd.concat([df_code, df], ignore_index=True)
                
                #return diff_data  # 返回股票代码和名称的列表
            except json.JSONDecodeError as e:
                print(f"JSON 解析失败：{e}")
                return None
        else:
            print(f"请求失败，HTTP 状态码：{response.status_code}")
            return None
        
    # 设置保存的文件夹路径
    save_path = r'E:\stock\stock_codes.xlsx'
    # 确保文件夹存在，如果不存在则创建
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # 将 DataFrame 保存为 Excel 文件
    df_code.to_excel(save_path, index=False, engine='openpyxl')
    print("数据已成功保存为 Excel 文件：stock_code.xlsx")
    return df_code  # 返回股票代码和名称的列表

def update_stock_data(code, name):
    # 设置 API 基本 URL
    base_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?"  # 替换为实际的 URL

    tag = '0'
    if code[0] != '6' :
        tag = '0'
    else:
        tag = '1'
        
    # 设置查询参数
    params = {
        'secid': tag + '.' + code,   #--
        'ut': 'fa5fd1943c7b386f172d6893dbfba10b',   #--
        'fields1': 'f1,f2,f3,f4,f5,f6',   #--
        'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',   #--
        'klt': '101',   #--
        'fqt': '1',   #--
        'end': '20500101',   #--
        'lmt': '50000',   #--
        '_': '1732546090136',   #--
    }

    # 发送 GET 请求并传递参数
    response = requests.get(base_url, params=params)

    # 确保请求成功
    if response.status_code == 200:
        #jsonp_data = response.text  # 获取响应文本（JSONP 数据）
        # 使用正则表达式去除回调函数的包装
        #json_data = re.sub(r'^jQuery.*?\((.*)\);$', r"\1", jsonp_data, flags=re.DOTALL).strip()
        
        json_data = response.text  # 获取响应文本（JSONP 数据）
        
        # 打印替换后的数据，确认是否为有效的 JSON 字符串
        #print("替换后的数据：")
        #print(json_data)

        # 尝试将 JSONP 数据解析为 Python 字典
        try:
            data = json.loads(json_data)
            #print("JSON 解析成功！")
            
            # 提取 klines 数据并转换为 DataFrame
            klines = data['data']['klines']
            #          "日期", "开盘价","收盘价","最高价","最低价","成交量","成交额", "振幅",        "涨跌幅",      "涨跌额",   "换手率"
            columns = ["Date", "Open", "Close", "High", "Low", "Volume", "Amount", "oscillation", "Pct Change", "Increase", "Volume Ratio"]

            # 将 klines 数据拆分成列表
            klines_data = [line.split(',') for line in klines]

            # 创建 DataFrame
            df = pd.DataFrame(klines_data, columns=columns)

            # 将 DataFrame 保存为 Excel 文件
            filename = sanitize_filename(code + '_' + name + '.xlsx')
            
            # 设置保存的文件夹路径
            save_path = os.path.join(r'E:\stock', filename)
            # 确保文件夹存在，如果不存在则创建
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            # 保存为 Excel 文件
            df.to_excel(save_path, index=False, engine='openpyxl')
            print("数据已成功保存为 Excel 文件：" + code + '_' + name + ".xlsx")
        
        except json.JSONDecodeError as e:
            print(f"JSON 解析失败：{e}")
    else:
        print(f"请求失败，HTTP 状态码：{response.status_code}")
        
def download_all_A_stock():
    # 获取股票代码列表
    stock_list = update_stock_code()
    print(stock_list)
    # 如果获取成功，循环调用 update_stock_data() 获取所有股票数据
    if len(stock_list) > 0:
        for i in range(0, len(stock_list)):
            code = stock_list['f12'].iloc[i]  # 股票代码
            name = stock_list['f14'].iloc[i]  # 股票名称
            print(f"开始更新股票：{name} ({code})")
            update_stock_data(code, name)
            
def main():
    #download_all_A_stock()
    update_stock_data('000300', '沪深300')
    
# 调用 main() 函数开始执行
if __name__ == "__main__":
    main()