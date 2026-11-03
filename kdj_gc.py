#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TradingView-Compatible Pine L2 KDJ & OBAF Scanner v3.0 (리팩토링 완결판)
========================================================================

주요 개선 및 해결 사항:
  1. Stale Signal 차단: 다운로드된 데이터의 실제 마지막 거래일(last_available_date)이
     현재 시장일 대비 과도하게 오래된 경우(상폐/거래정지 종목) 신호 판정에서 제외.
  2. 완성 봉 기준 일원화: 모든 기술적 지표(KDJ, CMF, OBV, Volume Ratio, WhalePump 등)와
     가격 스캔을 동일한 완성 봉(completed bar) 기준으로 통일하여 시점 불일치 제거.
  3. WhalePump dead code 및 무효 조건 해결:
     - 'close * 1.2 > 0'과 같은 항진 조건 및 xsa 0 나눗셈 처리(eps 추가) 정비.
     - WhalePump 스코어를 종목 분석 결과, 필터링, 정렬 및 엑셀/텔레그램 리포트에 직접 반영.
  4. Pandas 버전 호환성 확보:
     - Pandas 구버전과 신버전('M' vs 'ME') 모두 안전하게 작동하는 월간 리샘플링 폴백 로직 적용.
  5. 시가총액($1B ~ $30B) 및 OBAF 재무 게이트 추가:
     - yfinance Ticker info API 및 펀더멘털 지표(시가총액, 부채비율, 이자보상배율 등) 조회를 통해
       지정된 시총 범위를 벗어나거나 펀더멘털 결함 종목 필터링 옵션 제공.
  6. 테스트 용이성 및 유닛 테스트(unittest) 탑재:
     - 스크립트 단독 실행 또는 CLI `--test` 옵션을 통해 자체 테스트 수트 수행 가능.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import math
import os
import re
import sys
import time
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple, Union
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests

# --------------------------------------------------------------------------- #
# 상수
# --------------------------------------------------------------------------- #

ET = ZoneInfo("America/New_York")
UTC = timezone.utc
UA = "Mozilla/5.0 (compatible; tradingview-kdj-scanner/3.0)"

TIMEFRAMES = ("Daily", "Weekly", "Monthly")

ST_BUY = "BUY_SIGNAL"
ST_GC = "GOLDEN_CROSS"
ST_IMM = "IMMINENT"
ST_NONE = "NONE"
ST_NA = "N/A"

WHALE_STRONG = "강함"
WHALE_NONE = "없음"

SHEET_BUY = "B시그널"
SHEET_IMMINENT = "골든크로스 임박"
SHEET_CROSS = "골든크로스 발생"
SHEET_ALL = "전체 KDJ"

COLUMNS = [
    "Ticker",
    "MarketCap_B",
    "Close_Price",
    "Volume_Ratio",
    "Whale_Accumulation",
    "WhalePump_Score",
    "DarkPool_Ratio",
    "Institutional_Inflow(CMF)",
    "Interest_Coverage_Ratio",
    "Debt_To_Equity",
    "Signal_Timeframes",
    "Daily_Status",
    "Daily_K",
    "Daily_D",
    "Daily_J",
    "Weekly_Status",
    "Weekly_K",
    "Weekly_D",
    "Weekly_J",
    "Monthly_Status",
    "Monthly_K",
    "Monthly_D",
    "Monthly_J",
    "DarkPool_Source",
]

TICKER_RE = re.compile(r"^[A-Z0-9]{1,10}([.-][A-Z0-9]{1,5})?$")

INDEX_SPECS = {
    "S&P500": {"min_size": 400},
    "NASDAQ100": {"min_size": 90},
}

NASDAQ_API_URL = "https://api.nasdaq.com/api/quote/list-type/nasdaq100"

SYMBOL_ALIASES = {
    "BRKB": "BRK-B",
    "BFB": "BF-B",
}

_sleep: Callable[[float], None] = time.sleep


# --------------------------------------------------------------------------- #
# 설정
# --------------------------------------------------------------------------- #

@dataclass(frozen=True)
class Config:
    output_xlsx: str = f"L2KDJ_US_B_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
    universe_cache_path: str = ".cache/universe.json"

    kdj_n: int = 18
    kdj_m1: int = 4
    kdj_m2: int = 4
    buy_alert_limit: float = 0.0

    imminent_j_max: float = 20.0
    gc_threshold: float = 25.0  # 골든크로스 판정 시 상한 수치 기준 (20 이하)

    cmf_period: int = 20
    cmf_threshold: float = 0.1
    obv_lookback: int = 10
    vol_avg_period: int = 20
    vol_ratio_threshold: float = 1.5

    history_period: str = "5y"

    chunk_size: int = 80
    retry_chunk_size: int = 20
    download_retries: int = 3
    backoff_base: float = 2.0

    min_daily_bars: int = 120
    min_success_ratio: float = 0.5

    finra_days: int = 5
    finra_lookback_calendar: int = 10
    http_timeout: int = 20

    max_workers: int = 6
    telegram_top_whales: int = 15

    closed_only: bool = False
    crypto_mode: bool = False

    # Stale Signal 판정용 거래일 상한 기준 (영업일 기준 약 3일 이상 초과 시 차단)
    max_stale_days: int = 5

    # OBAF 및 시가총액 조건 게이트 필터링 설정 ($1B ~ $30B)
    enable_market_cap_filter: bool = False
    min_market_cap_usd: float = 1e9       # $1B
    max_market_cap_usd: float = 30e9      # $30B

    # OBAF 재무 필터링 설정 (기본값 False; 필요 시 비활성화/활성화 가능)
    enable_financial_filter: bool = False
    min_interest_coverage: float = 2.0
    max_debt_to_equity: float = 200.0

    @classmethod
    def from_env(cls) -> "Config":
        base = cls()

        def _int(name: str, default: int) -> int:
            raw = os.getenv(name)
            try:
                return int(raw) if raw else default
            except ValueError:
                return default

        def _float(name: str, default: float) -> float:
            raw = os.getenv(name)
            try:
                return float(raw) if raw else default
            except ValueError:
                return default

        def _bool(name: str, default: bool) -> bool:
            raw = os.getenv(name)
            if not raw:
                return default
            return raw.strip().lower() in ("1", "true", "yes", "on")

        return replace(
            base,
            output_xlsx=os.getenv("OUTPUT_XLSX", base.output_xlsx),
            history_period=os.getenv("HISTORY_PERIOD", base.history_period),
            universe_cache_path=os.getenv("UNIVERSE_CACHE", base.universe_cache_path),
            chunk_size=_int("CHUNK_SIZE", base.chunk_size),
            max_workers=_int("MAX_WORKERS", base.max_workers),
            telegram_top_whales=_int("TOP_WHALES", base.telegram_top_whales),
            kdj_n=_int("KDJ_N", base.kdj_n),
            kdj_m1=_int("KDJ_M1", base.kdj_m1),
            kdj_m2=_int("KDJ_M2", base.kdj_m2),
            buy_alert_limit=_float("BUY_ALERT_LIMIT", base.buy_alert_limit),
            imminent_j_max=_float("IMMINENT_J_MAX", base.imminent_j_max),
            gc_threshold=_float("GC_THRESHOLD", base.gc_threshold),
            closed_only=_bool("CLOSED_ONLY", base.closed_only),
            max_stale_days=_int("MAX_STALE_DAYS", base.max_stale_days),
            enable_market_cap_filter=_bool("ENABLE_MCAP_FILTER", base.enable_market_cap_filter),
            min_market_cap_usd=_float("MIN_MCAP_USD", base.min_market_cap_usd),
            max_market_cap_usd=_float("MAX_MCAP_USD", base.max_market_cap_usd),
            enable_financial_filter=_bool("ENABLE_FINANCIAL_FILTER", base.enable_financial_filter),
            min_interest_coverage=_float("MIN_ICR", base.min_interest_coverage),
            max_debt_to_equity=_float("MAX_DEBT_TO_EQUITY", base.max_debt_to_equity),
        )


# --------------------------------------------------------------------------- #
# 로깅
# --------------------------------------------------------------------------- #

RUN_ID = uuid.uuid4().hex[:8]


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "level": record.levelname,
            "event": record.getMessage(),
            "run_id": RUN_ID,
        }
        ctx = getattr(record, "ctx", None)
        if ctx:
            payload.update(ctx)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


_logger = logging.getLogger("kdj_scanner")
if not _logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(JsonFormatter())
    _logger.addHandler(_handler)
    _logger.setLevel(logging.INFO)
    _logger.propagate = False


def log(level: int, event: str, **ctx) -> None:
    _logger.log(level, event, extra={"ctx": ctx})


# --------------------------------------------------------------------------- #
# Pine-compatible xrf / xsa
# --------------------------------------------------------------------------- #

def pine_xrf(values: pd.Series, length: int = 1) -> pd.Series:
    arr = values.to_numpy(dtype=float)
    out = np.full(len(arr), np.nan)

    for i in range(len(arr)):
        r_val = np.nan
        for j in range(length + 1):
            idx = i - j
            if idx < 0:
                break
            v = arr[idx]
            if np.isnan(r_val) or not np.isnan(v):
                r_val = v
        out[i] = r_val

    return pd.Series(out, index=values.index)


def pine_xsa(src: pd.Series, length: int, wei: int) -> pd.Series:
    if length < 1:
        raise ValueError("xsa length must be >= 1")
    if wei < 1:
        raise ValueError("xsa weight must be >= 1")

    values = src.to_numpy(dtype=float)
    result = np.full(len(values), np.nan)

    sumf_prev = 0.0
    out_prev = np.nan

    for i, current in enumerate(values):
        if i >= length:
            old = values[i - length]
            old = 0.0 if np.isnan(old) else old
        else:
            old = 0.0

        if np.isnan(current):
            sumf = np.nan
        else:
            prev_sum = 0.0 if np.isnan(sumf_prev) else sumf_prev
            sumf = prev_sum - old + current

        if i < length or np.isnan(values[i - length]):
            ma = np.nan
        else:
            ma = sumf / length

        if np.isnan(out_prev):
            out = ma
        elif np.isnan(current):
            out = np.nan
        else:
            out = (
                current * wei
                + out_prev * (length - wei)
            ) / length

        result[i] = out
        sumf_prev = sumf
        out_prev = out

    return pd.Series(result, index=src.index)


# --------------------------------------------------------------------------- #
# L2 KDJ 및 Whale Pump Signal 계산 함수 (수정 반영)
# --------------------------------------------------------------------------- #

def calculate_whale_pump_signal(low: pd.Series, close: pd.Series) -> pd.Series:
    """
    Whale Pump 지표 계산.
    xsa 분모 0/inf 방지(eps) 및 근거 명확한 스케일링 적용.
    """
    eps = 1e-9
    var1 = pine_xrf(low, 1)

    diff_abs = (low - var1).abs()
    diff_max = np.maximum(low - var1, 0.0)

    den = pine_xsa(diff_max, 3, 1)
    num = pine_xsa(diff_abs, 3, 1)

    var2 = np.where(den.abs() > eps, (num / (den + eps)) * 100.0, 0.0)
    cond_src = pd.Series(var2 * 10.0, index=close.index)

    var3 = cond_src.ewm(span=3, adjust=False).mean()

    var4 = low.rolling(38, min_periods=1).min()
    var5 = var3.rolling(38, min_periods=1).max()

    lowest_1 = low.rolling(90, min_periods=1).min()
    var6 = pd.Series(np.where(lowest_1.notna() & (lowest_1 != 0), 1, 0), index=low.index)

    cond_var7 = np.where(low <= var4, (var3 + var5 * 2.0) / 2.0, 0.0)
    var7 = (pd.Series(cond_var7, index=low.index).ewm(span=3, adjust=False).mean() / 618.0) * var6

    return var7


def l2_kdj_buy_signal(
    df: pd.DataFrame,
    n1: int = 18,
    m1: int = 4,
    m2: int = 4,
    buy_alert_limit: float = 0.0,
) -> pd.DataFrame:
    high = df["High"]
    low = df["Low"]
    close = df["Close"]

    lowest_n = low.rolling(window=n1, min_periods=n1).min()
    highest_n = high.rolling(window=n1, min_periods=n1).max()

    denominator = highest_n - lowest_n

    rsv = pd.Series(
        np.where(
            denominator.notna() & (denominator != 0),
            (close - lowest_n) / denominator * 100.0,
            np.nan,
        ),
        index=close.index,
    )

    k = pine_xsa(rsv, m1, 1)
    d = pine_xsa(k, m2, 1)
    j = 3.0 * k - 2.0 * d

    whalepump = calculate_whale_pump_signal(low, close)

    prev_j = j.shift(1)
    buy_signal = (prev_j <= buy_alert_limit) & (j > buy_alert_limit)

    return pd.DataFrame(
        {
            "K": k,
            "D": d,
            "J": j,
            "RSV": rsv,
            "Whalepump": whalepump,
            "Buy_Signal": buy_signal,
        },
        index=close.index,
    )


# --------------------------------------------------------------------------- #
# 시간프레임 OHLCV
# --------------------------------------------------------------------------- #

_OHLCV = ["Open", "High", "Low", "Close", "Volume"]


def clean_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=_OHLCV)

    if isinstance(df.columns, pd.MultiIndex):
        if len(df.columns.levels) >= 2:
            found = None
            for level in range(df.columns.nlevels):
                vals = set(map(str, df.columns.get_level_values(level)))
                if set(_OHLCV).issubset(vals):
                    found = level
                    break
            if found is not None:
                df = df.copy()
                df.columns = df.columns.get_level_values(found)

    if not set(_OHLCV).issubset(df.columns):
        return pd.DataFrame(columns=_OHLCV)

    out = df[_OHLCV].copy()

    if getattr(out.index, "tz", None) is not None:
        out.index = out.index.tz_convert(UTC).tz_localize(None)

    out.index = pd.DatetimeIndex(out.index).normalize()
    out = out[~out.index.duplicated(keep="last")].sort_index()

    for c in _OHLCV:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    out = out.dropna(subset=["High", "Low", "Close"])
    out = out[out["Close"] > 0]
    out["Volume"] = out["Volume"].fillna(0.0)

    return out


def resample_ohlcv(
    df: pd.DataFrame,
    rule: str,
    origin: Optional[str] = None,
) -> pd.DataFrame:
    agg = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }

    # Pandas 2.2+ 버전을 대비한 규칙 하위 호환 폴백
    resample_rules = [rule]
    if rule == "ME":
        resample_rules.append("M")
    elif rule == "M":
        resample_rules.append("ME")

    out = None
    for r in resample_rules:
        try:
            if origin:
                out = df.resample(r, origin=origin).agg(agg)
            else:
                out = df.resample(r).agg(agg)
            break
        except ValueError:
            continue

    if out is None:
        out = df.resample("M").agg(agg) if not origin else df.resample("M", origin=origin).agg(agg)

    return out.dropna(subset=["Open", "High", "Low", "Close"])


def build_timeframes(
    completed_daily: pd.DataFrame,
    crypto_mode: bool,
    closed_only: bool,
) -> Dict[str, pd.DataFrame]:
    """
    모든 타임프레임(Daily, Weekly, Monthly)을 시점 불일치가 없는 completed_daily 기반으로 구축.
    """
    df = completed_daily.copy()

    if closed_only and len(df) > 1:
        df = df.iloc[:-1].copy()

    if crypto_mode:
        weekly = resample_ohlcv(df, "W-SUN")
        monthly = resample_ohlcv(df, "ME")
    else:
        weekly = resample_ohlcv(df, "W-FRI")
        monthly = resample_ohlcv(df, "ME")

    if closed_only:
        if len(weekly) > 1:
            weekly = weekly.iloc[:-1].copy()
        if len(monthly) > 1:
            monthly = monthly.iloc[:-1].copy()

    return {
        "Daily": df,
        "Weekly": weekly,
        "Monthly": monthly,
    }


# --------------------------------------------------------------------------- #
# Signal classification (Stale Signal 차단 로직 보완)
# --------------------------------------------------------------------------- #

def classify_kdj(
    kdj: pd.DataFrame,
    buy_alert_limit: float = 0.0,
    j_max: float = 20.0,
    now_utc: Optional[datetime] = None,
    max_stale_days: int = 5,
    gc_threshold: float = 20.0,
) -> Tuple[str, Optional[float]]:

    valid = kdj.dropna(subset=["K", "D", "J"])

    if len(valid) < 2:
        return ST_NA, None

    # Stale Signal 검증: 데이터의 마지막 날짜와 현재 시장 거래일 검사
    last_bar_date = valid.index[-1].date()
    ref_now = now_utc or datetime.now(timezone.utc)
    current_et_date = ref_now.astimezone(ET).date()

    # 데이터가 최근 기준보다 너무 뒤처져 있다면 거래정지/상장폐지 종목으로 간주
    if (current_et_date - last_bar_date).days > max_stale_days:
        return ST_NA, None

    # 데이터의 마지막 봉(최신 완료 거래일) 기준 검사
    j0 = float(valid["J"].iloc[-2])
    j1 = float(valid["J"].iloc[-1])

    k0 = float(valid["K"].iloc[-2])
    d0 = float(valid["D"].iloc[-2])

    k1 = float(valid["K"].iloc[-1])
    d1 = float(valid["D"].iloc[-1])

    # 1. B Buy 시그널 (J가 BuyAlertLimit 상향 돌파)
    if j0 <= buy_alert_limit and j1 > buy_alert_limit:
        return ST_BUY, j1

    # 2. 골든크로스 판정 (K와 D가 교차하고, J값이 설정된 gc_threshold(20) 이하인 경우에만 인정)
    is_crossover = (k0 < d0) and (k1 > d1) and (j1 <= gc_threshold)
    if is_crossover:
        return ST_GC, j1

    # 3. 골든크로스 임박 조건
    if (
        j1 < j_max
        and k1 < d1
        and (d1 - k1) < (d0 - k0)
        and k1 > k0
    ):
        return ST_IMM, j1

    return ST_NONE, j1


# --------------------------------------------------------------------------- #
# 수급 및 기타 지표 계산
# --------------------------------------------------------------------------- #

def compute_cmf(df: pd.DataFrame, period: int = 20) -> pd.Series:
    hl = (df["High"] - df["Low"]).replace(0, np.nan)
    mfm = (
        ((df["Close"] - df["Low"]) - (df["High"] - df["Close"]))
        / hl
    ).fillna(0.0)

    mfv = mfm * df["Volume"]

    vol_sum = (
        df["Volume"]
        .rolling(period, min_periods=period)
        .sum()
        .replace(0, np.nan)
    )

    return (
        mfv.rolling(period, min_periods=period).sum()
        / vol_sum
    )


def compute_obv(df: pd.DataFrame) -> pd.Series:
    direction = np.sign(df["Close"].diff()).fillna(0.0)
    return (direction * df["Volume"]).cumsum()


@dataclass
class FlowMetrics:
    cmf: Optional[float]
    obv_up: bool
    volume_ratio: Optional[float]
    whale: str
    whalepump_score: Optional[float]


def compute_flow(
    df: pd.DataFrame,
    cfg: Config,
) -> FlowMetrics:
    need = max(
        cfg.cmf_period,
        cfg.vol_avg_period + 1,
        cfg.obv_lookback + 1,
    )

    if len(df) < need:
        return FlowMetrics(None, False, None, WHALE_NONE, None)

    cmf_val = compute_cmf(
        df,
        cfg.cmf_period,
    ).iloc[-1]

    cmf = None if pd.isna(cmf_val) else float(cmf_val)

    obv = compute_obv(df)

    obv_up = bool(
        obv.iloc[-1]
        > obv.iloc[-1 - cfg.obv_lookback]
    )

    avg = float(
        df["Volume"]
        .iloc[-1 - cfg.vol_avg_period:-1]
        .mean()
    )

    ratio = (
        float(df["Volume"].iloc[-1]) / avg
        if avg > 0
        else None
    )

    # WhalePump 시그널 점수 추출
    wp_series = calculate_whale_pump_signal(df["Low"], df["Close"])
    wp_val = float(wp_series.iloc[-1]) if not wp_series.empty and not pd.isna(wp_series.iloc[-1]) else 0.0

    strong = (
        (
            cmf is not None
            and cmf > cfg.cmf_threshold
            and obv_up
            and ratio is not None
            and ratio >= cfg.vol_ratio_threshold
        )
        or (wp_val > 5.0)  # WhalePump 지표가 임계치를 넘는 경우 수급 강함으로 연동
    )

    return FlowMetrics(
        cmf,
        obv_up,
        ratio,
        WHALE_STRONG if strong else WHALE_NONE,
        wp_val,
    )


# --------------------------------------------------------------------------- #
# FINRA 및 DarkPool Proxy
# --------------------------------------------------------------------------- #

def finra_url(d: date) -> str:
    return (
        "https://cdn.finra.org/equity/regsho/daily/"
        f"CNMSshvol{d:%Y%m%d}.txt"
    )


def normalize_symbol(sym: object) -> str:
    s = (
        str(sym)
        .strip()
        .upper()
        .lstrip("$")
        .replace(".", "-")
        .replace("/", "-")
    )
    return SYMBOL_ALIASES.get(s, s)


def parse_finra(text: str) -> pd.Series:
    df = pd.read_csv(
        io.StringIO(text),
        sep="|",
        dtype=str,
    )

    df.columns = [c.strip() for c in df.columns]

    if not {"Symbol", "TotalVolume"}.issubset(df.columns):
        raise ValueError(
            f"Unexpected FINRA columns: {list(df.columns)}"
        )

    df["TotalVolume"] = pd.to_numeric(
        df["TotalVolume"],
        errors="coerce",
    )

    df = df.dropna(
        subset=["Symbol", "TotalVolume"]
    )

    df["Symbol"] = df["Symbol"].map(normalize_symbol)

    return df.groupby("Symbol")["TotalVolume"].sum()


def fetch_finra_file(
    session: requests.Session,
    d: date,
    cfg: Config,
) -> Optional[pd.Series]:
    for attempt in range(1, cfg.download_retries + 1):
        try:
            resp = session.get(
                finra_url(d),
                timeout=cfg.http_timeout,
            )

            if resp.status_code in (403, 404):
                return None

            resp.raise_for_status()

            return parse_finra(resp.text)

        except (
            requests.RequestException,
            ValueError,
            pd.errors.ParserError,
        ) as exc:
            log(
                logging.WARNING,
                "finra_fetch_failed",
                date=str(d),
                attempt=attempt,
                error=f"{type(exc).__name__}: {exc}",
            )

            if attempt < cfg.download_retries:
                _sleep(cfg.backoff_base ** attempt)

    return None


def fetch_finra_days(
    now_utc: datetime,
    cfg: Config,
) -> Dict[date, pd.Series]:
    today = now_utc.astimezone(ET).date()

    cands = [
        today - timedelta(days=i)
        for i in range(cfg.finra_lookback_calendar + 1)
    ]

    cands = [
        d for d in cands
        if d.weekday() < 5
    ]

    session = requests.Session()
    session.headers.update(
        {"User-Agent": UA}
    )

    with ThreadPoolExecutor(
        max_workers=max(
            1,
            min(cfg.max_workers, len(cands)),
        )
    ) as ex:
        results = list(
            ex.map(
                lambda d: (
                    d,
                    fetch_finra_file(
                        session,
                        d,
                        cfg,
                    ),
                ),
                cands,
            )
        )

    found = sorted(
        [
            (d, s)
            for d, s in results
            if s is not None
        ],
        key=lambda x: x[0],
        reverse=True,
    )

    return dict(found[:cfg.finra_days])


def offexchange_ratio(
    vol_by_date: pd.Series,
    finra: Dict[date, pd.Series],
    symbol: str,
) -> Optional[float]:
    num = den = 0.0

    for d, series in finra.items():
        if symbol in series.index and d in vol_by_date.index:
            v = float(vol_by_date[d])

            if v > 0:
                num += min(
                    float(series[symbol]),
                    v,
                )
                den += v

    return num / den if den > 0 else None


def block_volume_share(
    df: pd.DataFrame,
    cfg: Config,
) -> Optional[float]:
    n = cfg.vol_avg_period

    if len(df) < 2 * n:
        return None

    vol = df["Volume"]

    base = (
        vol.rolling(n)
        .mean()
        .shift(1)
    )

    recent = vol.iloc[-n:]
    base_recent = base.iloc[-n:]

    total = float(recent.sum())

    if total <= 0:
        return None

    heavy = (
        recent
        >= cfg.vol_ratio_threshold * base_recent
    )

    return float(recent[heavy].sum()) / total


def darkpool_for(
    symbol: str,
    completed: pd.DataFrame,
    finra: Dict[date, pd.Series],
    cfg: Config,
) -> Tuple[Optional[float], str]:

    if finra:
        vol = pd.Series(
            completed["Volume"].to_numpy(),
            index=[
                ts.date()
                for ts in completed.index
            ],
        )

        ratio = offexchange_ratio(
            vol,
            finra,
            symbol,
        )

        if ratio is not None:
            return ratio, "FINRA_TRF"

    proxy = block_volume_share(
        completed,
        cfg,
    )

    if proxy is not None:
        return proxy, "PROXY_BLOCK"

    return None, "N/A"


# --------------------------------------------------------------------------- #
# Universe 수집
# --------------------------------------------------------------------------- #

def _fetch_fdr(listing_name: str) -> List[str]:
    import FinanceDataReader as fdr

    df = fdr.StockListing(listing_name)

    for col in (
        "Symbol",
        "Ticker",
        "Code",
        "symbol",
        "ticker",
    ):
        if col in df.columns:
            return [
                normalize_symbol(x)
                for x in df[col]
                .dropna()
                .astype(str)
            ]

    raise KeyError(
        f"심볼 컬럼 없음: {list(df.columns)}"
    )


def _fetch_nasdaq_api(index_name: str) -> List[str]:
    if index_name != "NASDAQ100":
        raise ValueError(
            f"지원하지 않는 지수: {index_name}"
        )

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://www.nasdaq.com",
        "Referer": "https://www.nasdaq.com/",
    }

    resp = requests.get(
        NASDAQ_API_URL,
        headers=headers,
        timeout=20,
    )

    resp.raise_for_status()

    data = (resp.json() or {}).get("data") or {}

    rows = (
        (data.get("data") or {}).get("rows")
        or data.get("rows")
        or []
    )

    syms = [
        normalize_symbol(r["symbol"])
        for r in rows
        if isinstance(r, dict)
        and r.get("symbol")
    ]

    if not syms:
        raise ValueError(
            "Nasdaq API 응답에 심볼이 없음"
        )

    return syms


def _load_universe_cache(
    cfg: Config,
    index_name: str,
) -> List[str]:
    try:
        payload = json.loads(
            Path(cfg.universe_cache_path).read_text(encoding="utf-8")
        )

        return [
            s
            for s in payload.get(index_name, {}).get("symbols", [])
            if TICKER_RE.match(s)
        ]

    except (OSError, ValueError, AttributeError):
        return []


def _save_universe_cache(
    cfg: Config,
    index_name: str,
    symbols: List[str],
) -> None:
    path = Path(cfg.universe_cache_path)

    try:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            payload = {}

        payload[index_name] = {
            "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "symbols": sorted(symbols),
        }

        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, path)

    except OSError as exc:
        log(logging.WARNING, "universe_cache_save_failed", error=str(exc))


def collect_universe(
    cfg: Config,
    fdr_fetch: Callable[[str], List[str]] = _fetch_fdr,
    nasdaq_fetch: Callable[[str], List[str]] = _fetch_nasdaq_api,
) -> Tuple[List[str], Dict[str, str]]:

    fetchers = {
        "S&P500": ("FinanceDataReader", lambda: fdr_fetch("S&P500")),
        "NASDAQ100": ("NasdaqAPI", lambda: nasdaq_fetch("NASDAQ100")),
    }

    tickers: set = set()
    sources: Dict[str, str] = {}

    for name, spec in INDEX_SPECS.items():
        src_name, fetcher = fetchers[name]
        syms: Optional[List[str]] = None

        for attempt in range(1, 4):
            try:
                cand = sorted({s for s in fetcher() if TICKER_RE.match(s)})
                if len(cand) < spec["min_size"]:
                    raise ValueError(f"심볼 수 부족: {len(cand)}")
                syms = cand
                break
            except Exception as exc:
                log(
                    logging.WARNING,
                    "universe_fetch_failed",
                    index=name,
                    source=src_name,
                    attempt=attempt,
                    error=f"{type(exc).__name__}: {exc}",
                )
                if attempt < 3:
                    _sleep(cfg.backoff_base * attempt)

        if syms:
            _save_universe_cache(cfg, name, syms)
            sources[name] = f"{src_name}({len(syms)})"
        else:
            cached = _load_universe_cache(cfg, name)
            if len(cached) >= spec["min_size"]:
                syms = cached
                sources[name] = f"Cache({len(cached)})"
            else:
                sources[name] = "FAILED"

        if syms:
            tickers.update(syms)

    if not tickers:
        raise RuntimeError("티커 유니버스를 수집하지 못했습니다.")

    return sorted(tickers), sources


# --------------------------------------------------------------------------- #
# OBAF 및 시가총액/재무 정보 조회 게이트
# --------------------------------------------------------------------------- #

@dataclass
class FinancialMetrics:
    market_cap: Optional[float]
    interest_coverage: Optional[float]
    debt_to_equity: Optional[float]


def fetch_financial_metrics(ticker: str) -> FinancialMetrics:
    """
    yfinance를 사용하여 종목의 시가총액($) 및 펀더멘털 재무 정보(ICR, 부채비율)를 수집.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        info = t.info or {}

        mcap = info.get("marketCap")

        # 이자보상배율(ICR) 및 부채비율(Debt to Equity) 수집
        debt_to_eq = info.get("debtToEquity")
        
        # ICR 계산 시도
        icr = None
        ebitda = info.get("ebitda")
        interest_exp = info.get("interestExpense")
        if ebitda and interest_exp and interest_exp != 0:
            icr = abs(float(ebitda) / float(interest_exp))

        return FinancialMetrics(
            market_cap=float(mcap) if mcap else None,
            interest_coverage=icr,
            debt_to_equity=float(debt_to_eq) if debt_to_eq else None,
        )
    except Exception:
        return FinancialMetrics(None, None, None)


# --------------------------------------------------------------------------- #
# Yahoo Finance 데이터 수집 및 전처리
# --------------------------------------------------------------------------- #

def split_frames(
    raw: pd.DataFrame,
    tickers: List[str],
) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}

    if raw is None or raw.empty:
        return out

    if isinstance(raw.columns, pd.MultiIndex):
        for t in tickers:
            try:
                if t in raw.columns.get_level_values(0):
                    df = clean_ohlcv(raw[t])
                elif t in raw.columns.get_level_values(1):
                    df = clean_ohlcv(raw.xs(t, axis=1, level=1))
                else:
                    continue

                if not df.empty:
                    out[t] = df
            except Exception:
                continue

    elif len(tickers) == 1:
        df = clean_ohlcv(raw)
        if not df.empty:
            out[tickers[0]] = df

    return out


def download_chunk(
    tickers: List[str],
    cfg: Config,
) -> Optional[pd.DataFrame]:
    import yfinance as yf

    for attempt in range(1, cfg.download_retries + 1):
        try:
            raw = yf.download(
                tickers,
                period=cfg.history_period,
                interval="1d",
                auto_adjust=True,
                group_by="ticker",
                threads=True,
                progress=False,
            )

            if raw is None or raw.empty:
                raise ValueError("빈 응답")

            return raw

        except Exception as exc:
            log(
                logging.WARNING,
                "download_chunk_failed",
                size=len(tickers),
                attempt=attempt,
                error=f"{type(exc).__name__}: {exc}",
            )
            if attempt < cfg.download_retries:
                _sleep(cfg.backoff_base ** attempt)

    return None


def download_all(
    universe: List[str],
    cfg: Config,
) -> Dict[str, pd.DataFrame]:
    frames: Dict[str, pd.DataFrame] = {}

    def _pass(tickers: List[str], size: int, label: str) -> None:
        for i in range(0, len(tickers), size):
            chunk = tickers[i:i + size]
            t0 = time.time()
            raw = download_chunk(chunk, cfg)
            got = split_frames(raw, chunk) if raw is not None else {}
            frames.update(got)

            log(
                logging.INFO,
                "download_progress",
                phase=label,
                chunk_start=i,
                requested=len(chunk),
                received=len(got),
                seconds=round(time.time() - t0, 1),
            )

    _pass(universe, cfg.chunk_size, "main")
    missing = [t for t in universe if t not in frames]
    if missing:
        _pass(missing, cfg.retry_chunk_size, "retry")

    return frames


# --------------------------------------------------------------------------- #
# 종목 분석
# --------------------------------------------------------------------------- #

def _r(x: Optional[float], nd: int) -> Optional[float]:
    if x is None:
        return None
    try:
        if math.isnan(float(x)):
            return None
    except (TypeError, ValueError):
        return None
    return round(float(x), nd)


def analyze_ticker(
    ticker: str,
    daily: pd.DataFrame,
    cfg: Config,
    now_utc: datetime,
    finra: Dict[date, pd.Series],
) -> Optional[dict]:
    if len(daily) < cfg.min_daily_bars:
        return None

    # 완성 봉(completed bar) 추출: 미완성 당일 봉 제거 조건
    completed = daily
    if len(daily) > 1:
        last_date = daily.index[-1].date()
        current_et = now_utc.astimezone(ET)

        if last_date == current_et.date() and current_et.weekday() < 5:
            completed = daily.iloc[:-1]

    if completed.empty:
        return None

    # OBAF / 시가총액 및 재무 게이트 체크
    fin_metrics = fetch_financial_metrics(ticker)
    
    if cfg.enable_market_cap_filter and fin_metrics.market_cap is not None:
        if not (cfg.min_market_cap_usd <= fin_metrics.market_cap <= cfg.max_market_cap_usd):
            return None  # 시가총액 $1B~$30B 이외 종목 걸러냄

    if cfg.enable_financial_filter:
        if fin_metrics.interest_coverage is not None and fin_metrics.interest_coverage < cfg.min_interest_coverage:
            return None
        if fin_metrics.debt_to_equity is not None and fin_metrics.debt_to_equity > cfg.max_debt_to_equity:
            return None

    # 모든 지표 계산을 completed 기반으로 통일
    flow = compute_flow(completed, cfg)
    dp_ratio, dp_src = darkpool_for(ticker, completed, finra, cfg)

    frames = build_timeframes(
        completed,
        crypto_mode=cfg.crypto_mode,
        closed_only=cfg.closed_only,
    )

    statuses: Dict[str, str] = {}
    values: Dict[str, Dict[str, Optional[float]]] = {}

    for tf, frame in frames.items():
        if len(frame) < cfg.kdj_n:
            statuses[tf] = ST_NA
            values[tf] = {"K": None, "D": None, "J": None}
            continue

        l2_res = l2_kdj_buy_signal(
            frame,
            cfg.kdj_n,
            cfg.kdj_m1,
            cfg.kdj_m2,
            cfg.buy_alert_limit,
        )

        status, _ = classify_kdj(
            l2_res,
            cfg.buy_alert_limit,
            cfg.imminent_j_max,
            now_utc=now_utc,
            max_stale_days=cfg.max_stale_days,
            gc_threshold=cfg.gc_threshold,
        )
        last = l2_res.dropna(subset=["K", "D", "J"])

        if last.empty:
            statuses[tf] = ST_NA
            values[tf] = {"K": None, "D": None, "J": None}
        else:
            row = last.iloc[-1]
            statuses[tf] = status
            values[tf] = {
                "K": _r(row["K"], 4),
                "D": _r(row["D"], 4),
                "J": _r(row["J"], 4),
            }

    mcap_b = _r(fin_metrics.market_cap / 1e9, 2) if fin_metrics.market_cap else None

    return {
        "Ticker": ticker,
        "MarketCap_B": mcap_b,
        "Close": _r(float(completed["Close"].iloc[-1]), 4),
        "Volume_Ratio": _r(flow.volume_ratio, 2),
        "Whale": flow.whale,
        "WhalePump_Score": _r(flow.whalepump_score, 2),
        "CMF": _r(flow.cmf, 4),
        "Interest_Coverage_Ratio": _r(fin_metrics.interest_coverage, 2),
        "Debt_To_Equity": _r(fin_metrics.debt_to_equity, 2),
        "DarkPool_Ratio": _r(dp_ratio, 4),
        "DarkPool_Source": dp_src,
        "statuses": statuses,
        "values": values,
    }


def analyze_all(
    universe: List[str],
    frames: Dict[str, pd.DataFrame],
    cfg: Config,
    now_utc: datetime,
    finra: Dict[date, pd.Series],
) -> Tuple[List[dict], List[str]]:
    rows: List[dict] = []
    skipped: List[str] = []

    for ticker in universe:
        df = frames.get(ticker)
        if df is None:
            skipped.append(ticker)
            continue

        try:
            row = analyze_ticker(ticker, df, cfg, now_utc, finra)
        except Exception as exc:
            log(
                logging.ERROR,
                "analyze_failed",
                ticker=ticker,
                error=f"{type(exc).__name__}: {exc}",
            )
            row = None

        if row is None:
            skipped.append(ticker)
        else:
            rows.append(row)

    return rows, skipped


# --------------------------------------------------------------------------- #
# Excel 생성
# --------------------------------------------------------------------------- #

def build_sheet(
    rows: List[dict],
    signal_status: Optional[str] = None,
) -> pd.DataFrame:
    out = []

    for r in rows:
        if signal_status is not None:
            tfs = [
                tf
                for tf in TIMEFRAMES
                if r["statuses"].get(tf) == signal_status
            ]
            if not tfs:
                continue
        else:
            tfs = [
                tf
                for tf in TIMEFRAMES
                if r["statuses"].get(tf) in (ST_BUY, ST_GC, ST_IMM)
            ]

        rec = {
            "Ticker": r["Ticker"],
            "MarketCap_B": r.get("MarketCap_B"),
            "Close_Price": r["Close"],
            "Volume_Ratio": r["Volume_Ratio"],
            "Whale_Accumulation": r["Whale"],
            "WhalePump_Score": r.get("WhalePump_Score"),
            "DarkPool_Ratio": r["DarkPool_Ratio"],
            "Institutional_Inflow(CMF)": r["CMF"],
            "Interest_Coverage_Ratio": r.get("Interest_Coverage_Ratio"),
            "Debt_To_Equity": r.get("Debt_To_Equity"),
            "Signal_Timeframes": ", ".join(tfs),
            "DarkPool_Source": r["DarkPool_Source"],
        }

        for tf in TIMEFRAMES:
            rec[f"{tf}_Status"] = r["statuses"].get(tf, ST_NA)
            rec[f"{tf}_K"] = r["values"].get(tf, {}).get("K")
            rec[f"{tf}_D"] = r["values"].get(tf, {}).get("D")
            rec[f"{tf}_J"] = r["values"].get(tf, {}).get("J")

        out.append(rec)

    if not out:
        return pd.DataFrame(columns=COLUMNS)

    df = pd.DataFrame(out)
    df["_w"] = (df["Whale_Accumulation"] == WHALE_STRONG).astype(int)
    df["_wp"] = pd.to_numeric(df["WhalePump_Score"], errors="coerce").fillna(0)
    df["_n"] = (
        df["Signal_Timeframes"].replace("", np.nan).fillna("").str.count(",") + 1
    )
    df["_v"] = pd.to_numeric(df["Volume_Ratio"], errors="coerce").fillna(0)

    df = df.sort_values(
        ["_w", "_wp", "_n", "_v", "Ticker"],
        ascending=[False, False, False, False, True],
    )

    return df[COLUMNS].reset_index(drop=True)


_NUMFMT = {
    "MarketCap_B": "$#,##0.00",
    "Close_Price": "#,##0.0000",
    "Volume_Ratio": "0.00",
    "WhalePump_Score": "0.00",
    "DarkPool_Ratio": "0.0%",
    "Institutional_Inflow(CMF)": "0.0000",
    "Interest_Coverage_Ratio": "0.00",
    "Debt_To_Equity": "0.00",
    "Daily_K": "0.0000",
    "Daily_D": "0.0000",
    "Daily_J": "0.0000",
    "Weekly_K": "0.0000",
    "Weekly_D": "0.0000",
    "Weekly_J": "0.0000",
    "Monthly_K": "0.0000",
    "Monthly_D": "0.0000",
    "Monthly_J": "0.0000",
}


def write_excel(
    path: str,
    buy_signals: pd.DataFrame,
    imminent: pd.DataFrame,
    crossed: pd.DataFrame,
    all_rows: pd.DataFrame,
) -> None:
    from openpyxl.styles import Alignment, Font, PatternFill
    from openpyxl.utils import get_column_letter

    tmp = f"{path}.tmp.xlsx"

    with pd.ExcelWriter(tmp, engine="openpyxl") as writer:
        for name, df in (
            (SHEET_BUY, buy_signals),
            (SHEET_IMMINENT, imminent),
            (SHEET_CROSS, crossed),
            (SHEET_ALL, all_rows),
        ):
            df.to_excel(writer, sheet_name=name, index=False)
            ws = writer.sheets[name]
            ws.freeze_panes = "B2"

            if len(df):
                ws.auto_filter.ref = ws.dimensions

            for idx, col in enumerate(COLUMNS, start=1):
                cell = ws.cell(row=1, column=idx)
                cell.font = Font(bold=True, color="FFFFFF")
                cell.fill = PatternFill("solid", fgColor="305496")
                cell.alignment = Alignment(horizontal="center")

                if len(df):
                    width = max(
                        [len(col)]
                        + [len(str(v)) for v in df[col].head(200)]
                    ) + 3
                else:
                    width = len(col) + 3

                ws.column_dimensions[get_column_letter(idx)].width = min(width, 32)

                if col in _NUMFMT:
                    for row in range(2, len(df) + 2):
                        ws.cell(row=row, column=idx).number_format = _NUMFMT[col]

            if len(df):
                whale_col = COLUMNS.index("Whale_Accumulation") + 1
                for row in range(2, len(df) + 2):
                    if ws.cell(row=row, column=whale_col).value == WHALE_STRONG:
                        ws.cell(row=row, column=whale_col).fill = PatternFill(
                            "solid", fgColor="FFE699"
                        )

    os.replace(tmp, path)


# --------------------------------------------------------------------------- #
# Telegram 발송 및 메세지 포맷팅
# --------------------------------------------------------------------------- #

class TelegramClient:
    def __init__(
        self,
        token: str,
        chat_id: str,
        timeout: int = 30,
        retries: int = 3,
        backoff: float = 2.0,
    ):
        self._token = token
        self._chat_id = chat_id
        self._timeout = timeout
        self._retries = retries
        self._backoff = backoff

    def _redact(self, text: str) -> str:
        return text.replace(self._token, "***") if self._token else text

    def _call(
        self,
        method: str,
        data: dict,
        file_path: Optional[str] = None,
    ) -> None:
        url = f"https://api.telegram.org/bot{self._token}/{method}"
        last = "unknown"

        for attempt in range(1, self._retries + 1):
            wait = self._backoff ** attempt
            try:
                if file_path:
                    with open(file_path, "rb") as fh:
                        resp = requests.post(
                            url,
                            data=data,
                            files={"document": fh},
                            timeout=self._timeout,
                        )
                else:
                    resp = requests.post(url, data=data, timeout=self._timeout)

                if resp.status_code == 429:
                    wait = float(
                        resp.json().get("parameters", {}).get("retry_after", wait)
                    )
                    last = "429 rate limited"
                elif resp.ok and resp.json().get("ok"):
                    return
                elif 400 <= resp.status_code < 500:
                    raise RuntimeError(
                        f"Telegram {method} 거부: {resp.status_code} "
                        f"{self._redact(resp.text[:200])}"
                    )
                else:
                    last = f"HTTP {resp.status_code}"

            except requests.RequestException as exc:
                last = self._redact(f"{type(exc).__name__}: {exc}")

            if attempt < self._retries:
                _sleep(min(wait, 60))

        raise RuntimeError(f"Telegram {method} 실패: {last}")

    def send_message(self, text: str) -> None:
        self._call(
            "sendMessage",
            {
                "chat_id": self._chat_id,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": "true",
            },
        )

    def send_document(self, path: str, caption: str) -> None:
        self._call(
            "sendDocument",
            {"chat_id": self._chat_id, "caption": caption[:1000]},
            file_path=path,
        )


def summarize(
    rows: List[dict],
    universe_size: int,
    skipped: int,
) -> dict:
    counts = {
        tf: {"buy": 0, "cross": 0, "imminent": 0, "na": 0}
        for tf in TIMEFRAMES
    }

    for r in rows:
        for tf in TIMEFRAMES:
            st = r["statuses"][tf]
            if st == ST_BUY:
                counts[tf]["buy"] += 1
            elif st == ST_GC:
                counts[tf]["cross"] += 1
            elif st == ST_IMM:
                counts[tf]["imminent"] += 1
            elif st == ST_NA:
                counts[tf]["na"] += 1

    return {
        "universe": universe_size,
        "analyzed": len(rows),
        "failed": universe_size - len(rows),
        "skipped_short_history": skipped,
        "counts": counts,
        "whale_total": sum(1 for r in rows if r["Whale"] == WHALE_STRONG),
    }


def whale_highlights(rows: List[dict], top_n: int) -> List[dict]:
    picked = []

    for r in rows:
        if r["Whale"] != WHALE_STRONG:
            continue

        sig = [
            f"B:{tf}"
            for tf in TIMEFRAMES
            if r["statuses"][tf] == ST_BUY
        ]
        sig += [
            f"GC:{tf}"
            for tf in TIMEFRAMES
            if r["statuses"][tf] == ST_GC
        ]
        sig += [
            f"IMM:{tf}"
            for tf in TIMEFRAMES
            if r["statuses"][tf] == ST_IMM
        ]

        if sig:
            picked.append({**r, "signals": " / ".join(sig)})

    picked.sort(
        key=lambda r: ((r.get("WhalePump_Score") or 0) * 10 + (r["CMF"] or 0) * (r["Volume_Ratio"] or 0)),
        reverse=True,
    )

    return picked[:top_n]


_SESSION_LABEL = {
    "PRE": "장전",
    "REGULAR": "장중",
    "POST": "장후",
    "CLOSED": "휴장일",
}

_TF_LABEL = {
    "Daily": "일봉",
    "Weekly": "주봉",
    "Monthly": "월봉",
}


def market_session(now_utc: datetime) -> str:
    et = now_utc.astimezone(ET)
    if et.weekday() >= 5:
        return "CLOSED"
    minutes = et.hour * 60 + et.minute
    if minutes < 9 * 60 + 30:
        return "PRE"
    if minutes < 16 * 60:
        return "REGULAR"
    return "POST"


def build_telegram_message(
    summary: dict,
    whales: List[dict],
    now_utc: datetime,
) -> str:

    head = [
      (
            f"📊 <b>L2 KDJ Scanner (US)</b> · {escape(_SESSION_LABEL[market_session(now_utc)])}"
        ),
        f"분석 {summary['analyzed']}/{summary['universe']}종목",
        "",
        "<b>타임프레임별 시그널</b>",
    ]

    for tf in TIMEFRAMES:
        c = summary["counts"][tf]
        head.append(
            f"• {_TF_LABEL[tf]}: "
            f"<b>B시그널 {c['buy']}</b> · "
            f"골든크로스 <b>{c['cross']}</b> · "
            f"임박 <b>{c['imminent']}</b>"
        )

    head += [
        "",
        (
            "🐋 <b>고래 매집 강함 + KDJ 시그널</b> "
            f"(상위 {len(whales)}개)"
        ),
    ]

    lines = []
    for w in whales:
        dp = (
            f"{w['DarkPool_Ratio'] * 100:.0f}%"
            if w["DarkPool_Ratio"] is not None
            else "-"
        )
        wp = f"{w['WhalePump_Score']:.1f}" if w.get("WhalePump_Score") is not None else "-"
        mcap = f"${w['MarketCap_B']:.1f}B" if w.get("MarketCap_B") is not None else "-"
        lines.append(
            f"• <b>{escape(w['Ticker'])}</b> ({mcap}) "
            f"{escape(w['signals'])} | "
            f"WP {wp} | CMF {w['CMF']:.2f} | "
            f"거래량 ×{w['Volume_Ratio']:.1f} | "
            f"장외 {dp}"
        )

    if not lines:
        lines = ["• 해당 없음"]

    tail = [
        "",
        "※ 장외 비중은 FINRA 공시 기반 추정치입니다.",
    ]

    while True:
        msg = "\n".join(head + lines + tail)
        if len(msg) <= 4000 or len(lines) <= 1:
            return msg[:4096]
        lines.pop()


# --------------------------------------------------------------------------- #
# 실행
# --------------------------------------------------------------------------- #

def run(
    cfg: Config,
    tickers_override: Optional[List[str]],
    send_telegram: bool,
    now_utc: Optional[datetime] = None,
) -> int:

    now_utc = now_utc or datetime.now(timezone.utc)
    t0 = time.time()
    tg: Optional[TelegramClient] = None

    if send_telegram:
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat = os.getenv("TELEGRAM_CHAT_ID")
        if token and chat:
            tg = TelegramClient(token, chat)
        else:
            log(logging.WARNING, "telegram_env_missing")
            send_telegram = False

    try:
        if tickers_override:
            universe = sorted({t.strip().upper() for t in tickers_override if t.strip()})
            sources = {"override": "cli"}
        else:
            universe, sources = collect_universe(cfg)

        frames = download_all(universe, cfg)

        if len(frames) < cfg.min_success_ratio * len(universe):
            raise RuntimeError(
                f"가격 데이터 수신률 부족: {len(frames)}/{len(universe)}"
            )

        finra = fetch_finra_days(now_utc, cfg) if not cfg.crypto_mode else {}

        rows, skipped = analyze_all(
            universe,
            frames,
            cfg,
            now_utc,
            finra,
        )

        if not rows:
            raise RuntimeError("분석 가능한 종목이 없거나 조건(시총/재무)을 충족하는 종목이 없습니다.")

    except Exception as exc:
        log(
            logging.ERROR,
            "scan_failed",
            error=f"{type(exc).__name__}: {exc}",
            exc_info=True,
        )
        if tg:
            try:
                tg.send_message(f"❌ <b>KDJ Scanner 실패</b>\n{escape(str(exc))[:500]}")
            except Exception:
                pass
        return 1

    buy_signals = build_sheet(rows, ST_BUY)
    imminent = build_sheet(rows, ST_IMM)
    crossed = build_sheet(rows, ST_GC)
    all_rows = build_sheet(rows, None)

    write_excel(cfg.output_xlsx, buy_signals, imminent, crossed, all_rows)

    summary = summarize(rows, len(universe), len(skipped))

    log(
        logging.INFO,
        "scan_summary",
        **summary,
        sources=sources,
        seconds=round(time.time() - t0, 1),
    )

    if not send_telegram or tg is None:
        return 0

    try:
        msg = build_telegram_message(
            summary,
            whale_highlights(rows, cfg.telegram_top_whales),
            now_utc,
        )
        tg.send_message(msg)
        tg.send_document(
            cfg.output_xlsx,
            f"B시그널 {len(buy_signals)} · 골든크로스 {len(crossed)} · 임박 {len(imminent)}",
        )
    except Exception as exc:
        log(logging.ERROR, "telegram_failed", error=str(exc))
        return 2

    return 0


# --------------------------------------------------------------------------- #
# 자체 테스트 수트 (Unit Tests)
# --------------------------------------------------------------------------- #

class TestKDJScanner(unittest.TestCase):
    def test_pine_xrf(self):
        s = pd.Series([1.0, 2.0, 3.0, 4.0])
        res = pine_xrf(s, 1)
        self.assertEqual(len(res), 4)

    def test_pine_xsa(self):
        s = pd.Series([10.0, 20.0, 30.0, 40.0, 50.0])
        res = pine_xsa(s, 3, 1)
        self.assertEqual(len(res), 5)

    def test_whale_pump_signal(self):
        low = pd.Series([10.0, 9.5, 9.8, 10.2, 10.5, 10.1, 10.8])
        close = pd.Series([10.5, 10.0, 10.2, 10.6, 10.9, 10.5, 11.2])
        res = calculate_whale_pump_signal(low, close)
        self.assertEqual(len(res), len(close))
        self.assertFalse(res.isna().all())

    def test_resample_ohlcv_compatibility(self):
        dates = pd.date_range("2023-01-01", periods=60, freq="D")
        df = pd.DataFrame({
            "Open": 100.0, "High": 105.0, "Low": 95.0, "Close": 102.0, "Volume": 1000.0
        }, index=dates)
        res = resample_ohlcv(df, "ME")
        self.assertFalse(res.empty)

    def test_stale_signal_filtering(self):
        # 10일 전 데이터인 경우 Stale로 판단되어 ST_NA가 반환되는지 확인
        dates = pd.date_range("2023-01-01", periods=30, freq="D")
        k = pd.Series([10.0]*30, index=dates)
        d = pd.Series([12.0]*30, index=dates)
        j = pd.Series([-5.0]*28 + [-1.0, 5.0], index=dates)
        kdj_df = pd.DataFrame({"K": k, "D": d, "J": j})

        now_utc = datetime(2023, 1, 25, tzinfo=UTC)  # 15일 후
        st, val = classify_kdj(kdj_df, buy_alert_limit=0.0, now_utc=now_utc, max_stale_days=3)
        self.assertEqual(st, ST_NA)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="TradingView-compatible Pine L2 KDJ & OBAF Scanner"
    )

    ap.add_argument("--test", action="store_true", help="내장 유닛 테스트 실행")
    ap.add_argument("--no-telegram", action="store_true", help="텔레그램 발송 생략")
    ap.add_argument("--tickers", help="쉼표 구분 티커 (예: AAPL,TSLA,NVDA)")
    ap.add_argument("--output", help="Excel 출력 경로")
    ap.add_argument("--crypto", action="store_true", help="코인 모드")
    ap.add_argument(
        "--closed-only",
        action="store_true",
        help="진행 중인 봉을 제외하고 확정봉만 계산 (추천)",
    )
    ap.add_argument("--n", type=int, help="KDJ n (기본 18)")
    ap.add_argument("--m1", type=int, help="KDJ m1 (기본 4)")
    ap.add_argument("--m2", type=int, help="KDJ m2 (기본 4)")
    ap.add_argument("--buy-alert-limit", type=float, help="B 시그널 기준값 (기본 0.0)")

    args = ap.parse_args(argv)

    if args.test:
        print("Running unit tests...")
        suite = unittest.TestLoader().loadTestsFromTestCase(TestKDJScanner)
        runner = unittest.TextTestRunner(verbosity=2)
        result = runner.run(suite)
        return 0 if result.wasSuccessful() else 1

    cfg = Config.from_env()

    if args.output:
        cfg = replace(cfg, output_xlsx=args.output)
    if args.crypto:
        cfg = replace(cfg, crypto_mode=True)
    if args.closed_only:
        cfg = replace(cfg, closed_only=True)

    if args.n:
        cfg = replace(cfg, kdj_n=args.n)
    if args.m1:
        cfg = replace(cfg, kdj_m1=args.m1)
    if args.m2:
        cfg = replace(cfg, kdj_m2=args.m2)
    if args.buy_alert_limit is not None:
        cfg = replace(cfg, buy_alert_limit=args.buy_alert_limit)

    override = (
        [t.strip() for t in args.tickers.split(",") if t.strip()]
        if args.tickers
        else None
    )

    return run(
        cfg,
        override,
        send_telegram=not args.no_telegram,
    )


if __name__ == "__main__":
    sys.exit(main())
