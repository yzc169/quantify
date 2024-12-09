# -*- coding: utf-8 -*-
"""
Created on Tue Dec  3 23:48:15 2024

@author: devot
"""

import os
import glob
import numpy as np
import pandas as pd
#import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time

start_date = "2022-01-04"
end_date = "2024-12-02"
base_symbol_file = r'E:\stock\000300_沪深300.xlsx'
base_path = r'E:\stock\0'
df_strategy = pd.read_excel(base_symbol_file)
df_stock = pd.read_excel(r'E:\stock\stock_codes.xlsx', dtype=str)
portfolio = pd.DataFrame(columns=['Stock', 'Buy_Date', 'Sell_Date', 'Buy_Price', 'Sell_Price', 'Volume_Ratio', 'Profit'])
capital = 100000  # 初始资金

def get_thread_id():
    current_thread = threading.current_thread()
    thread_id = current_thread.ident
    return thread_id

# 模拟交易函数
def execute_trade(stock, buy_date, buy_price, sell_date, sell_price, volume_ratio):
    """记录交易信息并返回收益率"""
    profit = (sell_price - buy_price) / buy_price
    return pd.DataFrame(
        [[stock, buy_date, sell_date, buy_price, sell_price, volume_ratio, profit * 100]], 
        columns=['Stock','Buy_Date', 'Sell_Date', 'Buy_Price', 'Sell_Price', 'Volume_Ratio', 'Profit'])

def excute_strategy(file, symbol):
    
    # 回测模拟
    cash = capital  # 当前现金
    stock = None  # 当前持股股票
    shares = 0  # 当前持股数量
    buy_price = None  # 买入价格
    buy_date = None  # 买入日期
    high_price = 0  # 区间最高价
    oscillation = True  # 跌停标志
    take_profit = False # 止盈
    stop_loss = False  # 止损
    expire = False # 到期卖出
    high_return = 0  # 最高收益率
    volume_ratio = None # 量比
    data = pd.read_excel(file)
    pid = get_thread_id()
    print(f"{pid}--Successfully loaded data for {symbol} from {file}")
    # 时段过滤
    data = data[(data['Date'] >= start_date) & (data['Date'] <= end_date)]
    # 策略条件
    data['Daily_Return'] = data['Close'].pct_change()
    data['Volume_Ratio'] = data['Volume'] / data['Volume'].rolling(window=20).mean()  # 量比：当前量/20日均量
    data['Price_Change_30'] = (data['Close'] - data['Close'].shift(30)) / data['Close'].shift(30)  # 30天涨跌幅
    data['Max_Close'] = data['Close'].rolling(window=30).max()  # 30天内的最大收盘价
    
    # 创建各个条件列，避免直接在 loc 中使用复杂表达式
    data['Condition_1'] = data['Close'] > 1  # 股价小于50元
    data['Condition_2'] = data['Price_Change_30'].abs() < 0.6  # 30天涨跌幅小于60%
    data['Condition_3'] = data['Pct Change'].shift(2) > 9  # 涨幅大于9%
    data['Condition_4'] = data['Close'].shift(3) == data['High'].shift(3)  # 涨停（假设当日最高价等于收盘价为涨停）
    data['Condition_5'] = data['Close'].shift(2) == data['High'].shift(2)  # 涨停（假设当日最高价等于收盘价为涨停）
    data['Condition_6'] = data['Volume'].shift(1) > data['Volume'].shift(2)  # 量能突破：今日成交量大于昨日成交量
    data['Condition_7'] = data['Volume_Ratio'].shift(1) > 0.8  # 量比
    data['Condition_8'] = data['Open'] != data['Close']  # 过滤开盘涨停
    
    # 使用逻辑运算符连接各个条件
    data['Buy_Signal'] = (data['Condition_1'] & 
                           data['Condition_2'] & 
                           data['Condition_3'] & 
                           data['Condition_4'] & 
                           data['Condition_5'] & 
                           data['Condition_6'] &
                           data['Condition_7'] &
                           data['Condition_8']
                           ).astype(int)  # 将布尔值转换为 1 或 0
    # 删除临时的条件列
    data.drop(['Condition_1', 'Condition_2', 'Condition_3', 'Condition_4', 'Condition_5', 'Condition_6', 'Condition_7', 'Condition_8'], axis=1, inplace=True)
    
    for i in range(0, len(data)):
        # 买入逻辑
        if data['Buy_Signal'].iloc[i] == 1 and stock is None:
            buy_price = data['Open'].iloc[i]
            shares = int(cash / buy_price)
            buy_date = data['Date'].iloc[i]
            cash -= shares * buy_price
            volume_ratio = data['Volume_Ratio'].iloc[i-1]
            stock = symbol  # 标记持仓 
    
        # 卖出逻辑
        if stock is not None:
            # 当日策略收益率 = 持仓数量 * 当日涨跌幅
            # daily_return = data['Close'].iloc[i] / data['Close'].iloc[i - 1] - 1
            # strategy = daily_return * (shares * data['Close'].iloc[i] / capital)
            # assets = cash + (shares * data['Close'].iloc[i])
            # df_strategy.loc[data_300.index[i], 'Strategy_Return'] = strategy
            # df_strategy.loc[data_300.index[i], 'Total_Assets'] = assets
            # df_strategy.loc[data_300.index[i], 'Holdings'] = stock
            # 持有时间
            holding_days = (pd.to_datetime(data['Date'].iloc[i]) - pd.to_datetime(buy_date)).days
            # 当前股价
            current_price = data['Close'].iloc[i]
            # 当前收益率
            current_return = (current_price - buy_price) / buy_price

            if data['oscillation'].iloc[i] < 0.1 and data['Open'].iloc[i] == data['Close'].iloc[i] and data['Pct Change'].iloc[i] < 0: # 判断是否开盘跌停
                oscillation = False
            else:
                oscillation = True
            # 卖出逻辑
            if oscillation and take_profit or stop_loss or expire:
                sell_price = data['Open'].iloc[i]
                cash += shares * sell_price
                global portfolio
                portfolio = pd.concat([portfolio, execute_trade(stock, buy_date, buy_price, data['Date'].iloc[i], sell_price, volume_ratio)], ignore_index=True)
                # 清空持仓
                stock = None
                shares = 0
                buy_price = None
                buy_date = None
                high_price = 0
                take_profit = False
                stop_loss = False
                expire = False
                volume_ratio = None
                oscillation = True
                continue
            if high_return >= 0.15: #止盈卖出
                if data['Open'].iloc[i] > data['Close'].iloc[i] and data['Low'].iloc[i] / data['High'].iloc[i] <= 0.95: 
                    take_profit = True
                if data['Open'].iloc[i] < data['Close'].iloc[i] and high_price != 0 and data['Low'].iloc[i] / high_price <= 0.95:
                    take_profit = True
            # 区间最高价
            if high_price < data['High'].iloc[i]:
                high_price = data['High'].iloc[i]
                high_return = (high_price - buy_price) / buy_price
            
            if current_return <= -0.0965: #止损卖出
                stop_loss = True
                
            if holding_days >= 15 and high_return < 0.15: #到期卖出
                expire = True    
        #else:
            # data_300.loc[data_300.index[i], 'Total_Assets'] = cash

def statistical_income():
    cash = capital  # 当前现金
    shares = 0  # 当前持股数量
    
    for i in range(0, len(df_strategy)):
        buy_date = None
        sell_date = None
        stock = None
        
        if stock is not None:
            if df_strategy['Date'].iloc[i] == sell_date:
                stock = None
                buy_date = None
                sell_date = None
            else:
                # 构造文件名
                file_name = f"{stock}_*.xlsx"
                file_path = os.path.join(base_path, file_name)
                matching_files = glob.glob(file_path)
                df_holdinds_tock = pd.read_excel(matching_files[0])
                df_holdinds_tock = df_holdinds_tock[(df_holdinds_tock['Date'] >= buy_date) & (df_holdinds_tock['Date'] <= sell_date)]
                close_price = df_holdinds_tock.loc[df_holdinds_tock['Date'] == df_strategy['Date'].iloc]
                assets = cash + (shares * close_price)
                # df_strategy.loc[data_300.index[i], 'Strategy_Return'] = strategy
                df_strategy.loc[df_strategy.index[i], 'Total_Assets'] = assets
                df_strategy.loc[df_strategy.index[i], 'Holdings'] = stock
        else:
            df_holdinds_portfolio = portfolio.loc[portfolio['Buy_Date'] == df_strategy['Date'].iloc[i]]
            if len(df_holdinds_portfolio) > 0:
                stock = df_holdinds_portfolio['Stock'].iloc[0]
                buy_date = df_holdinds_portfolio['Buy_Date'].iloc[0]
                sell_date = df_holdinds_portfolio['Sell_Date'].iloc[0]
                print(df_holdinds_portfolio)
        
    
def main():
    # 记录起始时间
    start_time = time.time()
    # 初始化策略每日收益率列
    df_strategy['Strategy_Return'] = 0.0  # 默认未持仓时收益为0
    df_strategy['Holdinds'] = ''          # 持仓记录
    df_strategy['Total_Assets'] = np.nan  # 初始资金10万
    df_strategy.loc[0, 'Total_Assets'] = 100000  # 为第一个值赋值
    df_strategy['Total_Assets'] = df_strategy['Total_Assets'].fillna(method='ffill')
            
    for i in range(0, len(df_stock)):
        symbol = df_stock['f12'].iloc[i]
        # 构造文件名
        file_name = f"{symbol}_*.xlsx"  # 这里星号代表股票名是动态的（可以是任意的名称）
        
        # 构造文件路径
        file_path = os.path.join(base_path, file_name)
        
        # 使用 glob 查找匹配的文件
        matching_files = glob.glob(file_path)
        
        if matching_files:
            # 如果找到了匹配的文件，读取第一个文件
            excute_strategy(matching_files[0], symbol)
        else:
            print(f"No file found for {symbol}")
    
    # 结果分析
    total_profit = portfolio['Profit'].sum()
    total_trades = len(portfolio)
    average_profit = total_profit / total_trades if total_trades > 0 else 0

    print(f"Total Profit: {total_profit:.2f}%")
    print(f"Total Trades: {total_trades}")
    print(f"Average Profit per Trade: {average_profit:.2f}%")
    
    # 统计收益曲线
    statistical_income()
    end_time = time.time()  # 记录结束时间
    elapsed_time = end_time - start_time  # 计算运行时间
    print(f"代码运行总时间: {elapsed_time:.2f} 秒")
    
# 调用 main() 函数开始执行
if __name__ == "__main__":
    main()