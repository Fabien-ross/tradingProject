"""

"""
import numpy as np
import pandas as pd
from typing import List, Optional
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

from src.core.logging.loggers import logger_data_ret


@dataclass
class LhdrIndicator:

    asset_id: Optional[str] = None
    time_frame: Optional[str] = None
    open_date: Optional[datetime] = None
    open: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    close: Optional[Decimal] = None
    volume: Optional[Decimal] = None

    rsi: Optional[Decimal] = None
    stoch_rsi: Optional[Decimal] = None
    macd: Optional[Decimal] = None
    ema_short: Optional[Decimal] = None
    ema_mid: Optional[Decimal] = None
    ema_long: Optional[Decimal] = None

    boll_low2: Optional[Decimal] = None
    boll_low1: Optional[Decimal] = None
    boll_mid: Optional[Decimal] = None
    boll_up1: Optional[Decimal] = None
    boll_up2: Optional[Decimal] = None
    msd: Optional[Decimal] = None
    simple_return: Optional[Decimal] = None
    log_return: Optional[Decimal] = None
    obv: Optional[Decimal] = None
    vwap: Optional[Decimal] = None
    volatility: Optional[Decimal] = None


class IndicatorCalculation:

    def __init__(self):
        self.sma_standard_window: int = 20
        self.msd_standard_window: int = 20
        self.volatility_window: int = 20
        self.ema_signal_standard_window: int = 9
        self.ema_short_standard_window: int = 12
        self.ema_mid_standard_window: int = 26
        self.ema_long_standard_window: int = 48

    def make_data_frame(
        self, 
        data: List[List[int|str]],
        market_id: str
        ) -> Optional[pd.DataFrame]:

        if 'binance' in market_id:
            columns = ['open_time', 'open', 'high', 'low', 'close', 'volume']

            trimmed_data = [row[:6] for row in data]
            df = pd.DataFrame(trimmed_data, columns=columns)
            df['open_time'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.floor('s') # datetime in seconds
            
            float_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in float_cols:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            return df

    def full_indicators_calculation(
        self, 
        df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        
        msd_series = self.msd(prices=df)
        sma_series = self.sma(prices=df)
        df["rsi"] = self.rsi(prices=df)
        df["stoch_rsi"] = self.stoch_rsi(prices=df)
        df["macd"] = self.macd(prices=df)
        df["ema_short"] = self.ema(prices=df, span="short")
        df["ema_mid"] = self.ema(prices=df, span="mid")
        df["ema_long"] = self.ema(prices=df, span="long")
        df["msd"] = msd_series
        df["boll_mid"] = sma_series
        df["boll_low1"] = sma_series - msd_series
        df["boll_low2"] = sma_series - 2*msd_series
        df["boll_up1"] = sma_series + msd_series
        df["boll_up2"] = sma_series + 2*msd_series
        df["simple_return"] = self.simple_return(prices=df)
        df["log_return"] = self.log_return(prices=df)
        df["obv"] = self.obv(prices=df)
        df["vwap"] = self.vwap(prices=df)
        df["volatility"] = self.volatility(prices=df)
        return df
        
    def sma(
        self, 
        prices: pd.DataFrame|pd.Series, 
        window: Optional[int] = None
    ) -> pd.Series:
        if not window:
            window = self.sma_standard_window
        return prices['close'].rolling(window=window).mean()
    
    def ema(
        self, 
        prices: pd.DataFrame|pd.Series, 
        span: int|str
    ) -> pd.Series:
        if isinstance(span, str):
            if span == "long":
                span = self.ema_long_standard_window
            elif span == "mid":
                span = self.ema_mid_standard_window
            elif span == "short":
                span = self.ema_short_standard_window
            elif span == "signal":
                span = self.ema_signal_standard_window
            else:
                logger_data_ret.warning(f"Unknow span keyword '{span}' for EMA calculation. Default as 'long' EMA.")
                span = self.ema_long_standard_window
        if isinstance(prices, pd.DataFrame):
            return prices['close'].ewm(span=span, adjust=False).mean()
        else:
            return prices.ewm(span=span, adjust=False).mean()

    def msd(
        self, 
        prices: pd.DataFrame|pd.Series, 
        window: Optional[int] = None
    ) -> pd.Series:
        if not window:
            window = self.msd_standard_window
        return prices['close'].rolling(window=window).std()

    def simple_return(self, prices: pd.DataFrame) -> pd.Series:
        return prices['close'].pct_change()

    def log_return(self, prices: pd.DataFrame) -> pd.Series:
        log_ret = np.log(prices['close'] / prices['close'].shift(1))
        return pd.Series(log_ret, index=prices.index)

    def macd(
        self, 
        prices: pd.DataFrame, 
        short_window: int|str = "short", 
        long_window: int|str = "mid",
        signal_window: int|str = "signal"
    ) -> pd.Series:
        ema_short = self.ema(prices=prices, span=short_window)
        ema_long = self.ema(prices=prices, span=long_window)
        macd_line = ema_short - ema_long
        signal_line = self.ema(prices=macd_line, span=signal_window)
        return macd_line - signal_line

    def obv(self, prices: pd.DataFrame) -> pd.Series:
        close = pd.to_numeric(prices['close'], errors='coerce').astype(float)
        volume = pd.to_numeric(prices['volume'], errors='coerce').astype(float)
        price_change = close.diff()
        obv = pd.Series(index=close.index, dtype='float64')
        obv.iloc[0] = 0
        for i in range(1, len(close)):
            if price_change.iloc[i] > 0:
                obv.iloc[i] = obv.iloc[i - 1] + volume.iloc[i]
            elif price_change.iloc[i] < 0:
                obv.iloc[i] = obv.iloc[i - 1] - volume.iloc[i]
            else:
                obv.iloc[i] = obv.iloc[i - 1]
        return obv

    def rsi(
        self, 
        prices: pd.DataFrame, 
        window: int = 14
    ) -> pd.Series:
        delta = pd.to_numeric(prices['close'], errors='coerce').astype(float).diff().dropna()
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        avg_gain = gains.rolling(window=window).mean()
        avg_loss = losses.rolling(window=window).mean()
        rs = avg_gain / avg_loss
        rs = rs.replace([float('inf'), -float('inf')], float('nan')).fillna(0)
        rsi = (100 - (100 / (1 + rs)))/100
        return rsi

    def stoch_rsi(
        self, 
        prices: pd.DataFrame, 
        rsi_window: int = 14, 
        stoch_window: int = 14
    ) -> pd.Series:
        delta = pd.to_numeric(prices['close'], errors='coerce').astype(float).diff().dropna()
        gains = delta.where(delta > 0, 0)
        losses = -delta.where(delta < 0, 0)
        avg_gain = gains.rolling(window=rsi_window).mean()
        avg_loss = losses.rolling(window=rsi_window).mean()
        rs = avg_gain / avg_loss
        rs = rs.replace([float('inf'), -float('inf')], float('nan')).fillna(0)
        rsi_series = 100 - (100 / (1 + rs))
        stoch_rsi = (rsi_series - rsi_series.rolling(window=stoch_window).min()) / (rsi_series.rolling(window=stoch_window).max() - rsi_series.rolling(window=stoch_window).min())
        return stoch_rsi

    def vwap(self, prices: pd.DataFrame) -> pd.Series:
        pv = (prices['close'] * prices['volume']).cumsum()
        vol = prices['volume'].cumsum()
        return pv / vol

    def volatility(self, prices: pd.DataFrame) -> pd.Series:
        returns = prices['close'].pct_change()
        daily_vol = returns.rolling(self.volatility_window).std()
        annualized_vol = daily_vol * np.sqrt(365)
        return annualized_vol
    
    def adx(self, df, n=14):

        high = df['high']
        low = df['low']
        close = df['close']

        plus_dm = high.diff()
        minus_dm = low.shift(1) - low

        plus_dm = np.where((plus_dm > minus_dm) & (plus_dm > 0), plus_dm, 0.0)
        minus_dm = np.where((minus_dm > plus_dm) & (minus_dm > 0), minus_dm, 0.0)

        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.DataFrame({'tr1': tr1, 'tr2': tr2, 'tr3': tr3}).max(axis=1)

        atr = tr.ewm(alpha=1/n, adjust=False).mean()
        plus_dm_smooth = pd.Series(plus_dm).ewm(alpha=1/n, adjust=False).mean()
        minus_dm_smooth = pd.Series(minus_dm).ewm(alpha=1/n, adjust=False).mean()

        plus_di = 100 * (plus_dm_smooth / atr)
        minus_di = 100 * (minus_dm_smooth / atr)

        dx = (100 * abs(plus_di - minus_di) / (plus_di + minus_di)).fillna(0)
        adx = dx.ewm(alpha=1/n, adjust=False).mean()

        df['+DI'] = plus_di
        df['-DI'] = minus_di
        df['ADX'] = adx
        return df