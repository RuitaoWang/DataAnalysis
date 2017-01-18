import pandas as pd
import numpy as np



"""
Volatility Factor ---
"""

def factor_CMRA(return_close, risk_free, freq='d',window=12):
    """
    因子：CMRA， 滚动周期的累计收益率
    CMRA = ln(max cumret) - ln(min cumret)
    Parameters
    ----------
    return_close 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    risk_free 
        pd.Series
    freq
        string, 只控制中位数求值的周期时间，并不改变返回数据的时间间隔（仍为天）
    window
        int, 只控制累计收益求值的周期时间，并不改变返回数据的时间间隔（仍为天）
    Returns
    -------
        pd.DataFrame，index为stock，column为date。

    """
    return_modified = return_close.resample(freq)
    riskless = risk_free.resample(freq)
    cumret = return_modified.rolling(window).cumprod().sub(riskless.rolling(window).cumprod(),axis=0)
    return cumret


def factor_DHILO(price_high,price_low,freq):
    """
    因子：DHILO， 计算每日最高价和最低价log差值的滚动中位数
    DHILO = median{ln(high price) - ln(low price)}
    Parameters
    ----------
    price_high 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    price_high 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    freq
        int, 只控制中位数求值的周期时间，并不改变返回数据的时间间隔（仍为天）
    Returns
    -------
        pd.DataFrame，index为stock，column为date。

    """
    spread = np.log(price_low) - np.log(price_high)
    DHILO_value = spread.rolling(frequency).median()
    return DHILO_value


def factor_DVART(return_close, risk_free, T, q=10):
    """
    因子：DVRAT (Daily returns variance ratio - serial dependence in daily return)
    DVRAT = (sig_q^2/sig^2 - 1)
    sig^2: 窗口为T的滚动超额收益率方差
    sig_q^2: q期累计超额收益的窗口为T的滚动方差，权重为m
    m = q(T-q+1)(1-q/T)
    Parameters
    ----------
    return_close 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    risk_free 
        pd.Series
    T
        int, 计算方差的滚动窗口。
    q   
        int, 计算累计收益率的滚动窗口
    Returns
    -------
        pd.DataFrame，index为stock，column为date。

    """
    excess_return = return_close.sub(risk_free, axis=0)
    sig_2 = excess_return.rolling(T).var()
    m = q*(T-q+1)*(1-q/T)
    cum_excess = excess_return.rolling(q).cumprod()
    cum_sig_2 = (cum_excess.rolling(T).var)*(T-1)/m
    DVRAT_value = cum_sig_2.div(sig_2) - 1
    return DVRAT_value


def factor_HBETA_HSIGMA(return_close, market_return, window=262, lag=3):
    """
    因子：HBETA 时间序列beta
    因子：HSIGMA 时间序列残差
    Parameters
    ----------
    return_close 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    market_return 
        pd.Series, 市场收益率
    window
        int, 回归的滚动窗口。
    lag   
        int, 回归的lag
    Returns
    -------
        2个pd.DataFrame，index为stock，column为date。分别为历史beta和历史sigma

    """
    stocks = return_close.columns
    index = return_close.index
    H_BETA = pd.DataFrame
    H_SIGMA = pd.DataFrame
    for sid in stocks:
        reg = pd.stats.ols.MovingOLS(y=return_close[sid],x=market_return,
                                    window_type='rolling',window=window,intercept=True,
                                    nw_lags=lag)
        H_BETA['sid'] = reg.beta['x']
        H_SIGMA['sid'] = reg.resid.rolling(window).std()
    return H_BETA, H_SIGMA









def cal_return(prices:pd.DataFrame, intervals=[1], log=False, forward = False):
    """
    根据行情数据计算收益率
    
    Parameters
    ----------
    prices : pd.DataFrame
        行情数据输入，股票id为column，日期为index
    intervals : list[int]
        用于计算收益率的间隔，如[1],[1,2,3]
    log : True 或者 False
        若True，计算连续收益率，若否，计算简单收益率
    forward： True 或者 False
        若True，后验收益率（用于因子效果监测）
    Returns
    -------
    returns : pd.DataFrame - MultiIndex
    """
    returns = pd.DataFrame(index=pd.MultiIndex.from_product([prices.index, prices.columns], names=['date', 'stock']))
    for interval in intervals:
        if forward is False & log is False:
            pct_chg = prices.pct_change(interval)
        elif forward is True & log is False:
            pct_chg = prices.pct_change(interval).shift(-interval)
        elif forward is False & log is True:
            pct_chg = np.log(prices/prices.shift(interval))
        else:
            pct_chg = np.log(prices/prices.shift(interval)).shift(-interval)
        returns[interval] = pct_chg.stack()
    return returns     

def industry_group(factor:pd.DataFrame, prices:pd.DataFrame,
                   industry,intervals=[1]):
    """
    因子收益行业整理，返回2个date,stock,industry的MultiIndex DataFrame(分别为因子暴露和收益率)
    
    Parameters
    ----------
    factor : pd.DataFrame
        因子暴露数据输入，股票id为column，日期为index
    prices : pd.DataFrame
        行情数据输入，股票id为column，日期为index
    industry : Dict 或者Pd.Series,或行业分类
    intervals : list[int]
        用于计算收益率的间隔，如[1],[1,2,3]
    
    Returns
    -------
    returns : pd.DataFrame - MultiIndex
    """
    factor2=factor.copy().stack()
    factor2.name='factor'
    factor2.index=factor2.index.set_names(['date','stock'])
    forward_returns = cal_return(prices,intervals,False,True)
    data = pd.merge(pd.DataFrame(factor2),forward_returns,how='left',
                   left_index=True,right_index=True)
    industry = pd.Series(index=factor2.index,
                    data=industry[factor2.index.get_level_values('stock')].values)
    industry.name='industry'
    data = pd.merge(pd.DataFrame(industry),data,how='left',
                   left_index=True,right_index=True)
    data = data.set_index('industry',append=True)
    factor = data.pop('factor')
    returns = data
    return factor, returns

    
def factor_winsorize(f_loading:pd.DataFrame, deriviate = 0.01):
    """
    替换行业极值
    
    Parameters
    ----------
    f_loading : pd.DataFrame
        因子暴露数据输入，股票id为column，日期为index
    deriviate : float
        极值范围，以小数记(0.01,0.02,etc)
    Returns
    -------
    returns : pd.DataFrame 
    """
    def hor_win(x,wp=deriviate):
        s = x.copy()
        u=s.quantile(1-wp)
        l=s.quantile(wp)
        s[s<l]=l
        s[s>u]=u
        return s 
    fac = f_loading.apply(hor_win,axis=1)
    return fac

def weighted_average(indicator, weight):
    """
    以给定权重计算均值
    ----------
    indicator 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
    weight 
        pd.DataFrame, 目标DataFrame,index应为时间戳，column应为股票sid(ie:'600651.SH')。
        或 pd.Series, index为股票sid(ie:'600651.SH')
    Returns
    -------
        pd.DataFrame，index为stock，column为date。

    """
    import pandas as pd
    if isinstance(weight, pd.DataFrame):
        weight = weight.apply(lambda x: x/x.sum(),axis=1)
    wta = indicator*weight
    return wta


  
    
def factor_group(factor:pd.Series, bins=None, by_industry=False):
    def get_quantile(x,bins):
        return pd.cut(x,bins,labels=False)+1
    grouper = ['date','industry'] if by_industry else['date']
    factor_pct=factor.groupby(level=grouper)
    factor_quantile = factor_pct.apply(get_quantile,bins=bins)
    factor_quantile.name='quantile'
    return factor_quantile
    
    
def factor_normalize(factor:pd.Series,by_industry=True):
    factor2=factor.copy()
    grouper = ['date','industry'] if by_industry else['date']
    factor2 = (factor2 - factor2.groupby(level=grouper).mean())/factor2.groupby(level=grouper).std()
    factor2.name = 'normalized factor'
    return factor2
    
    

