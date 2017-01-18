def get_market_cap(stock,start_date, end_date,freq=None):
    """
    流通股本数据（若公司有A股和B股则加总）获取。返回的数据时间间隔为天

    Parameters
    ----------
    stock 
        list of string, 股票的SID，ie: '600651.SH'。
    start_date
        string, 起始时间，ie: '20161231', 使用单引号
    end_date
        string，结束时间，ie: '20161231', 使用单引号
    freq
        {'A','M','W'}，分别为上一个年度末，上一个月度末，和上一个周度末,只改变取值间隔，并不改变返回数据的时间间隔（仍为天）
    Returns
    ----------
        pd.DataFrame，index为stock，column为date。
    """
    stock_str = str(stock)
    stock_str = str(stock).replace('[','(')
    stock_str = str(stock_str).replace(']',')')
    date_str = "'{0}' and '{1}'".format(start_date,end_date)
    sql = "SELECT SID, TRADE_DT, S_DQ_MV from ashare_eod_derivative_indicator where SID in {0} and TRADE_DT between {1}".format(stock_str,date_str)
    import pandas as pd
    from sqlalchemy import create_engine
    engine_mercury = create_engine(
    "mysql+pymysql://reader:reader123456@192.168.1.188:3306/mercury?charset=utf8", echo=False
    )
    raw_data = pd.read_sql(sql,engine_mercury)
    raw_data['TRADE_DT'] = pd.to_datetime(raw_data['TRADE_DT'])
    raw_data.columns=['stock','date','market_cap']
    pivot_data = raw_data.pivot(index='date', columns='stock',values='market_cap')
    day_index=pivot_data.index
    if freq == None:
        final_data = pivot_data
    elif freq !=None:
        final_data = pivot_data.resample(freq,fill_method='bfill').shift(1)[1:]
        final_data=final_data.resample('d',fill_method='ffill')
    return final_data


def get_fundamental(stock, start_date, end_date, table, field):
    """
    基本面数据查询 (资产负债表,现金表，收益表)

    Parameters
    ----------
    stock 
        list of string, 股票的SID，ie: '600651.SH'。
    start_date
        string, 起始时间，ie: '20161231', 使用单引号
    end_date
        string, 结束时间，ie: '20161231', 使用单引号
    table
        string, {'ashare_balancesheet','ashare_cash_flow','ashare_income'}
                分别为资产负债表，现金流表，和收益表
    field
        string, 所查询字段
    Returns
    ----------
        pd.DataFrame，index为stock，column为date。
    """
    stock_str = str(stock)
    stock_str = str(stock).replace('[','(')
    stock_str = str(stock_str).replace(']',')')
    import pandas as pd
    import datetime as dt
    pre_date = pd.to_datetime(start_date)-dt.timedelta(days=262)
    pre_date_str = pre_date.strftime('%Y%m%d')
    pre_date_index = pd.date_range(pre_date_str,end_date)
    pre_date_index = pd.DatetimeIndex(pre_date_index,name='date')
    date_index=pd.date_range(start_date,end_date)
    date_index=pd.DatetimeIndex(date_index,name='date')
    date_str = "'{0}' and '{1}'".format(pre_date_str,end_date)
    sql = "select SID,ANN_DT, {0} from {1} where STATEMENT_TYPE='408001000' and SID in {2} and ANN_DT between {3}".format(field,table,stock_str,date_str)
    from sqlalchemy import create_engine
    engine_mercury = create_engine(
    "mysql+pymysql://reader:reader123456@192.168.1.188:3306/mercury?charset=utf8", echo=False
    )
    raw_data = pd.read_sql(sql, engine_mercury)
    raw_data.columns=['stock','date',str(field)]
    replicate_data = raw_data.drop_duplicates(subset=['stock','date'],keep='last')
    replicate_data['date'] = pd.to_datetime(replicate_data['date'])
    pivot_data = replicate_data.pivot(index='date',columns='stock',values=str(field))
    pivot_data = pivot_data.reindex(pre_date_index)
    pivot_data = pivot_data.fillna(method='ffill')
    pivot_data = pivot_data.reindex(date_index)
    return pivot_data


def get_daily_price(stock, start_date, end_date, field):
    """
    日频行情数据查询

    Parameters
    ----------
    stock 
        list of string, 股票的SID，ie: '600651.SH'。
    start_date
        string, 起始时间，ie: '20161231', 使用单引号
    end_date
        string, 结束时间，ie: '20161231', 使用单引号
    field
        string, 常用字段 
                昨日收盘价：'S_DQ_PRECLOSE'
                开盘价: 'S_DQ_OPEN'
                最高价: 'S_DQ_HIGH'
                最低价: 'S_DQ_LOW'
                收盘价: 'S_DQ_CLOSE'
                涨幅:   'S_DQ_PCTCHANGE'
                成交量:  'S_DQ_VOLUME'
                成交金额: 'S_DQ_AMOUNT'
    Returns
    ----------
        pd.DataFrame，index为stock，column为date。
    """
    stock_str = str(stock)
    stock_str = str(stock).replace('[','(')
    stock_str = str(stock_str).replace(']',')')
    date_str = "'{0}' and '{1}'".format(start_date,end_date)
    sql = "SELECT SID, TRADE_DT, {0} from ashare_eod_prices where SID in {1} and TRADE_DT between {2}".format(field,stock_str,date_str)
    import pandas as pd
    from sqlalchemy import create_engine
    engine_mercury = create_engine(
    "mysql+pymysql://reader:reader123456@192.168.1.188:3306/mercury?charset=utf8", echo=False
    )
    raw_data = pd.read_sql(sql,engine_mercury)
    raw_data.columns = ['stock','date',str(field)]
    raw_data['date'] = pd.to_datetime(raw_data['date'])
    pivot_data = raw_data.pivot(index='date',columns='stock',values=str(field))
    return pivot_data


def rolling_average(indicator,span, freq='M', method = 'mean'):
    """
    移动均值计算. 若要求TTM,则freq='M',span=12

    Parameters
    ----------
    indicator 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    method
        string, {'mean','ewma'}
                若为mean，则计算算数移动平均值，若为ewma则计算指数移动平均值
    freq
        string, {'A','M','W'}，分别为上一个年度末，上一个月度末，和上一个周度末,只改变取值间隔，并不改变返回数据的时间间隔（仍为天）
    span
        若选择'mean'方法，为移动窗口(window)参数，若选择'ewma'方法，为周期(span)参数
    Returns
    ----------
        pd.DataFrame，index为stock，column为date。
    """
    import pandas as pd
    rolling_average = indicator.copy()
    rolling_average = rolling_average.resample(freq)
    if method == 'mean':
        rolling_average = pd.rolling_mean(rolling_average,axis=0,window=span)
    if method == 'ewma':
        rolling_average = pd.ewma(rolling_average,span=span)
    return rolling_average.dropna()

print(get_fundamental(['600651.SH','600652.SH'],'20150504','20151204','ashare_balancesheet','TOT_SHRHLDR_EQY_INCL_MIN_INT'))


    
