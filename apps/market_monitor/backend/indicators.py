from __future__ import annotations

from typing import Any, Dict, Sequence

import pandas as pd


def calculate_rvol(df: pd.DataFrame, volume_lookback: int = 30) -> float | None:
    if df.empty or "volume" not in df.columns:
        return None
    lookback = max(1, int(volume_lookback))
    volume = pd.to_numeric(df["volume"], errors="coerce")
    if volume.empty or len(volume) < lookback:
        return None
    avg_volume = volume.rolling(window=lookback, min_periods=lookback).mean().iloc[-1]
    current_volume = volume.iloc[-1]
    if pd.isna(avg_volume) or avg_volume <= 0 or pd.isna(current_volume):
        return None
    return float(current_volume / avg_volume)


def calculate_obv(df: pd.DataFrame) -> pd.Series:
    if df.empty or "close" not in df.columns or "volume" not in df.columns:
        return pd.Series(dtype=float)
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    delta = close.diff().fillna(0.0)
    direction = delta.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else 0.0))
    signed_volume = direction * volume
    return signed_volume.cumsum()


def is_obv_rising(obv_series: pd.Series, period: int = 5) -> bool:
    if obv_series is None or obv_series.empty:
        return False
    lookback = max(2, int(period))
    if len(obv_series) < lookback:
        return False
    recent = obv_series.iloc[-lookback:]
    if recent.isna().any():
        return False
    return float(recent.iloc[-1]) > float(recent.iloc[0])


def is_obv_falling(obv_series: pd.Series, period: int = 5) -> bool:
    if obv_series is None or obv_series.empty:
        return False
    lookback = max(2, int(period))
    if len(obv_series) < lookback:
        return False
    recent = obv_series.iloc[-lookback:]
    if recent.isna().any():
        return False
    return float(recent.iloc[-1]) < float(recent.iloc[0])


def has_obv_divergence(price: pd.Series, obv: pd.Series, window: int = 10) -> bool:
    if price is None or obv is None or price.empty or obv.empty:
        return False
    lookback = max(4, int(window))
    if len(price) < lookback or len(obv) < lookback:
        return False

    p = pd.to_numeric(price.iloc[-lookback:], errors="coerce")
    o = pd.to_numeric(obv.iloc[-lookback:], errors="coerce")
    if p.isna().any() or o.isna().any():
        return False

    p_latest = float(p.iloc[-1])
    o_latest = float(o.iloc[-1])
    p_prev_high = float(p.iloc[:-1].max())
    p_prev_low = float(p.iloc[:-1].min())
    o_prev_high = float(o.iloc[:-1].max())
    o_prev_low = float(o.iloc[:-1].min())

    bearish_div = p_latest >= p_prev_high and o_latest < o_prev_high
    bullish_div = p_latest <= p_prev_low and o_latest > o_prev_low
    return bool(bearish_div or bullish_div)


def calculate_vwap(df: pd.DataFrame) -> float | None:
    required = {"high", "low", "close", "volume"}
    if df.empty or not required.issubset(df.columns):
        return None

    high = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    close = pd.to_numeric(df["close"], errors="coerce")
    volume = pd.to_numeric(df["volume"], errors="coerce")
    if high.empty or low.empty or close.empty or volume.empty:
        return None

    typical_price = (high + low + close) / 3.0
    volume = volume.clip(lower=0.0).fillna(0.0)
    cum_volume = volume.cumsum()
    if cum_volume.empty or float(cum_volume.iloc[-1]) <= 0:
        return None

    vwap_series = (typical_price * volume).cumsum() / cum_volume.replace(0, pd.NA)
    if vwap_series.empty:
        return None
    return _safe_float(vwap_series.iloc[-1])


def calculate_volume_delta(df: pd.DataFrame) -> float | None:
    required = {"open", "high", "low", "close", "volume"}
    if df.empty or not required.issubset(df.columns):
        return None

    latest = df.iloc[-1]
    open_price = _safe_float(latest.get("open"))
    high_price = _safe_float(latest.get("high"))
    low_price = _safe_float(latest.get("low"))
    close_price = _safe_float(latest.get("close"))
    volume = _safe_float(latest.get("volume"))
    if (
        open_price is None
        or high_price is None
        or low_price is None
        or close_price is None
        or volume is None
        or volume <= 0
    ):
        return None

    price_range = high_price - low_price
    if price_range <= 0:
        return None

    # Approximate candle-level signed volume pressure.
    return ((close_price - open_price) / price_range) * volume


def ohlcv_to_dataframe(ohlcv: Sequence[Sequence[float]]) -> pd.DataFrame:
    """
    Convert ccxt ohlcv rows into a typed DataFrame.
    Columns: timestamp, open, high, low, close, volume
    """
    df = pd.DataFrame(
        ohlcv,
        columns=["timestamp", "open", "high", "low", "close", "volume"],
    )
    if df.empty:
        return df

    numeric_cols = ["open", "high", "low", "close", "volume"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    return df


def calculate_latest_indicators(
    ohlcv: Sequence[Sequence[float]],
    atr_period: int = 14,
    rsi_period: int = 14,
    bb_period: int = 20,
    bb_std: float = 2.0,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    ma_period: int = 20,
    std_period: int = 20,
    volume_lookback: int = 30,
    obv_rising_period: int = 5,
    obv_divergence_window: int = 10,
) -> Dict[str, Any]:
    """
    Calculate latest indicator values from ohlcv candles.
    All values are based on the last row in the provided ohlcv sequence.
    """
    df = ohlcv_to_dataframe(ohlcv)
    if df.empty:
        return {}

    close = df["close"]
    high = df["high"]
    low = df["low"]

    # ATR
    prev_close = close.shift(1)
    tr = pd.concat(
        [
            high - low,
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    atr = tr.rolling(window=atr_period, min_periods=atr_period).mean()

    # RSI (Wilder)
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1 / max(1, rsi_period), min_periods=rsi_period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / max(1, rsi_period), min_periods=rsi_period, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))

    # Bollinger Bands
    bb_mid = close.rolling(window=bb_period, min_periods=bb_period).mean()
    bb_dev = close.rolling(window=bb_period, min_periods=bb_period).std(ddof=0)
    bb_upper = bb_mid + bb_std * bb_dev
    bb_lower = bb_mid - bb_std * bb_dev

    # MACD
    ema_fast = close.ewm(span=macd_fast, adjust=False, min_periods=macd_fast).mean()
    ema_slow = close.ewm(span=macd_slow, adjust=False, min_periods=macd_slow).mean()
    macd_line = ema_fast - ema_slow
    macd_signal_line = macd_line.ewm(
        span=macd_signal,
        adjust=False,
        min_periods=macd_signal,
    ).mean()
    macd_hist = macd_line - macd_signal_line

    # MA and STD for long anomaly
    ma = close.rolling(window=ma_period, min_periods=ma_period).mean()
    std_dev = close.rolling(window=std_period, min_periods=std_period).std(ddof=0)

    # RVOL + OBV
    rvol = calculate_rvol(df, volume_lookback=volume_lookback)
    obv_series = calculate_obv(df)
    obv_rising = is_obv_rising(obv_series, period=obv_rising_period)
    obv_falling = is_obv_falling(obv_series, period=obv_rising_period)
    obv_divergence = has_obv_divergence(close, obv_series, window=obv_divergence_window)
    vwap = calculate_vwap(df)
    volume_delta = calculate_volume_delta(df)
    volume_delta_ratio = None
    latest_volume = _safe_float(df["volume"].iloc[-1])
    if volume_delta is not None and latest_volume is not None and latest_volume > 0:
        volume_delta_ratio = volume_delta / latest_volume

    latest_atr = _safe_float(atr.iloc[-1])
    candle_body_abs = abs(float(close.iloc[-1]) - float(df["open"].iloc[-1]))
    atr_relative = None
    if latest_atr is not None and latest_atr > 0:
        atr_relative = candle_body_abs / latest_atr

    latest_idx = -1
    return {
        "atr": latest_atr,
        "rsi": _safe_float(rsi.iloc[latest_idx]),
        "bb_upper": _safe_float(bb_upper.iloc[latest_idx]),
        "bb_middle": _safe_float(bb_mid.iloc[latest_idx]),
        "bb_lower": _safe_float(bb_lower.iloc[latest_idx]),
        "macd": _safe_float(macd_line.iloc[latest_idx]),
        "macd_signal": _safe_float(macd_signal_line.iloc[latest_idx]),
        "macd_hist": _safe_float(macd_hist.iloc[latest_idx]),
        "ma": _safe_float(ma.iloc[latest_idx]),
        "std_dev": _safe_float(std_dev.iloc[latest_idx]),
        "rvol": rvol,
        "obv": _safe_float(obv_series.iloc[latest_idx]) if not obv_series.empty else None,
        "obv_rising": bool(obv_rising),
        "obv_falling": bool(obv_falling),
        "obv_divergence": bool(obv_divergence),
        "vwap": vwap,
        "volume_delta": volume_delta,
        "volume_delta_ratio": volume_delta_ratio,
        "atr_relative": atr_relative,
    }


def _safe_float(value: Any) -> float | None:
    try:
        casted = float(value)
        if pd.isna(casted):
            return None
        return casted
    except (TypeError, ValueError):
        return None
