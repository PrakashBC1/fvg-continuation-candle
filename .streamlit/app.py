"""
₿  Binance USDT-M Futures
    FVG Launch Candle Scanner

PATTERN DEFINITION:
────────────────────────────────────────────────────────────────────────────
  A "FVG Launch Candle" is a candle that:
    1. OPENS inside a Fair Value Gap (FVG)
    2. CLOSES beyond the FVG boundary (above FVG high for bullish,
       below FVG low for bearish)
    3. The close also breaks above/below the prior consolidation
       structure high/low (BOS in the same candle)

  FVG is defined as the gap between:
    Bullish FVG : C1.low  and C3.high  — where C3.high < C1.low  (gap above)
    Bearish FVG : C1.high and C3.low   — where C3.low  > C1.high (gap below)

  Launch candle (C_launch) conditions:
    BULLISH:
      • C_launch.open  >= FVG.low  AND  C_launch.open  <= FVG.high
      • C_launch.close >  FVG.high  (closes above the imbalance)
      • C_launch.close >  C_launch.open  (bullish body)

    BEARISH:
      • C_launch.open  >= FVG.low  AND  C_launch.open  <= FVG.high
      • C_launch.close <  FVG.low   (closes below the imbalance)
      • C_launch.close <  C_launch.open  (bearish body)

TARGET LOGIC:
    TP1 — next significant swing high/low beyond the launch candle
    TP2 — 1.618 Fibonacci extension of the FVG range from close
    SL  — beyond the opposite side of the FVG (with 0.3% buffer)
────────────────────────────────────────────────────────────────────────────
No API key required — Binance public REST API.
"""

import io
import time
import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

# ══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="FVG Launch Candle Scanner",
    page_icon="🚀",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ══════════════════════════════════════════════════════════════════════════════
# CSS
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Space+Grotesk:wght@400;600;700;800&display=swap');

html,body,[class*="css"]{font-family:'Space Grotesk',sans-serif;}
.stApp{background:#04060e;color:#dde4f0;}
.stApp>header{background:transparent!important;}

[data-testid="stSidebar"]{background:#06080f!important;border-right:1px solid #0d1525;}
[data-testid="stSidebar"] label,
[data-testid="stSidebar"] p{color:#6a7f9c!important;font-size:.82rem;}
[data-testid="stSidebar"] h3{color:#a78bfa!important;font-family:'JetBrains Mono',monospace!important;
  font-size:.7rem!important;letter-spacing:.22em!important;text-transform:uppercase!important;}

[data-testid="metric-container"]{background:#07091a!important;border:1px solid #0d1525!important;
  border-radius:12px!important;padding:.75rem 1rem!important;}
[data-testid="metric-container"] label{font-family:'JetBrains Mono',monospace!important;
  font-size:.58rem!important;color:#283550!important;letter-spacing:.14em!important;text-transform:uppercase!important;}
[data-testid="metric-container"] [data-testid="stMetricValue"]{font-family:'Space Grotesk',sans-serif!important;
  font-weight:800!important;font-size:1.8rem!important;color:#dde4f0!important;line-height:1.1!important;}

.stProgress>div>div{background:linear-gradient(90deg,#7c3aed,#a78bfa)!important;border-radius:99px!important;}

.stButton>button{background:linear-gradient(135deg,#130a2e,#1e1040)!important;color:#a78bfa!important;
  border:1px solid #2d1f55!important;border-radius:9px!important;font-family:'JetBrains Mono',monospace!important;
  font-weight:700!important;font-size:.82rem!important;letter-spacing:.1em!important;
  padding:.55rem 1.4rem!important;width:100%!important;transition:all .18s ease!important;}
.stButton>button:hover{border-color:#a78bfa!important;box-shadow:0 0 22px #7c3aed30!important;}

[data-testid="stDownloadButton"]>button{background:linear-gradient(135deg,#003322,#004d33)!important;
  color:#34d399!important;border:1px solid #065f46!important;border-radius:9px!important;
  font-family:'JetBrains Mono',monospace!important;font-weight:700!important;font-size:.8rem!important;width:100%!important;}

/* ── Signal cards ── */
.bull-card{
  background:linear-gradient(135deg,#020d06 0%,#041208 100%);
  border:1px solid #16532820;border-left:4px solid #22c55e;
  border-radius:12px;padding:1.1rem 1.3rem;margin:.45rem 0;
  font-family:'JetBrains Mono',monospace;
  animation:cardIn .32s cubic-bezier(.34,1.56,.64,1) both;}
.bear-card{
  background:linear-gradient(135deg,#0d0204 0%,#160306 100%);
  border:1px solid #65161820;border-left:4px solid #ef4444;
  border-radius:12px;padding:1.1rem 1.3rem;margin:.45rem 0;
  font-family:'JetBrains Mono',monospace;
  animation:cardIn .32s cubic-bezier(.34,1.56,.64,1) both;}
@keyframes cardIn{
  from{opacity:0;transform:translateY(-8px) scale(.97)}
  to{opacity:1;transform:translateY(0) scale(1)}}

/* ── Target rows ── */
.tp-row{display:grid;grid-template-columns:repeat(4,1fr);gap:.55rem;margin-top:.7rem;}
.tp-cell .lbl{font-size:.57rem;color:#283550;text-transform:uppercase;letter-spacing:.08em;}
.tp-cell .val{font-weight:600;font-size:.82rem;margin-top:.05rem;}
.tp-cell .rr {font-size:.6rem;margin-top:.02rem;}

/* ── FVG zone indicator ── */
.fvg-bar{height:6px;border-radius:3px;margin:.45rem 0 .3rem 0;}
.fvg-bull{background:linear-gradient(90deg,#166534,#22c55e);}
.fvg-bear{background:linear-gradient(90deg,#991b1b,#ef4444);}

/* ── Badges ── */
.badge-bull{display:inline-block;background:#0a2916;color:#4ade80;font-size:.6rem;
  padding:.1rem .55rem;border-radius:4px;font-weight:700;letter-spacing:.08em;}
.badge-bear{display:inline-block;background:#2d0808;color:#f87171;font-size:.6rem;
  padding:.1rem .55rem;border-radius:4px;font-weight:700;letter-spacing:.08em;}
.badge-fvg{display:inline-block;background:#1e1040;color:#a78bfa;font-size:.6rem;
  padding:.1rem .55rem;border-radius:4px;font-weight:700;letter-spacing:.08em;}
.badge-bos{display:inline-block;background:#0a1a2e;color:#38bdf8;font-size:.6rem;
  padding:.1rem .55rem;border-radius:4px;font-weight:700;letter-spacing:.08em;}

/* ── Log ── */
.log-row{background:#06080f;border:1px solid #0b1220;border-radius:5px;
  padding:.28rem .75rem;margin:.14rem 0;font-family:'JetBrains Mono',monospace;font-size:.68rem;color:#1e3050;}
.log-hit{color:#6a8fac!important;border-color:#142035!important;}
.log-err{color:#3a1010!important;}

/* ── Section header ── */
.sec-hdr{font-family:'JetBrains Mono',monospace;font-size:.6rem;letter-spacing:.25em;
  text-transform:uppercase;color:#a78bfa;border-bottom:1px solid #0d1525;
  padding-bottom:.3rem;margin:1rem 0 .5rem 0;}

/* ── Scrollable pane ── */
.res-pane{max-height:66vh;overflow-y:auto;padding-right:4px;
  scrollbar-width:thin;scrollbar-color:#1e1040 #06080f;}
.res-pane::-webkit-scrollbar{width:4px;}
.res-pane::-webkit-scrollbar-track{background:#06080f;}
.res-pane::-webkit-scrollbar-thumb{background:#1e1040;border-radius:4px;}

/* ── Live dot ── */
.live-dot{display:inline-block;width:7px;height:7px;border-radius:50%;
  background:#a78bfa;animation:pulse 1.5s ease-in-out infinite;
  vertical-align:middle;margin-right:5px;}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.2}}

/* ── Info bar ── */
.info-bar{background:#06080f;border:1px solid #0d1525;border-left:3px solid #a78bfa;
  border-radius:0 8px 8px 0;padding:.7rem 1rem;font-family:'JetBrains Mono',monospace;
  font-size:.7rem;color:#6a7f9c;line-height:1.8;margin:.4rem 0 .8rem 0;}
</style>
""", unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════
BASE     = "https://fapi.binance.com"
KLINE_EP = "/fapi/v1/klines"
EXCH_EP  = "/fapi/v1/exchangeInfo"
TF_MAP   = {"1H": "1h", "4H": "4h", "1D": "1d", "1W": "1w"}

@dataclass
class FVGLaunch:
    direction:     str       # "BULLISH" | "BEARISH"
    symbol:        str
    tf:            str
    dt:            datetime  # launch candle datetime
    # FVG boundaries
    fvg_high:      float
    fvg_low:       float
    fvg_size_pct:  float     # FVG size as % of candle range — quality metric
    # Launch candle OHLC
    lc_open:       float
    lc_high:       float
    lc_low:        float
    lc_close:      float
    lc_body_pct:   float     # body size / range — candle strength
    # Levels
    sl:            float
    tp1:           float
    tp2:           float     # 1.618 extension
    tp3:           float     # 2.618 extension (runner)
    rr1:           float
    rr2:           float
    rr3:           float
    candle_idx:    int
    total_candles: int

# ══════════════════════════════════════════════════════════════════════════════
# HTTP SESSION
# ══════════════════════════════════════════════════════════════════════════════
@st.cache_resource
def get_session():
    s = requests.Session()
    s.headers.update({"User-Agent": "FVGLaunchScanner/1.0"})
    return s

SESSION = get_session()

# ══════════════════════════════════════════════════════════════════════════════
# BINANCE API
# ══════════════════════════════════════════════════════════════════════════════
def api_get(endpoint: str, params: dict | None = None, retries: int = 3):
    url = BASE + endpoint
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=8)
            used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
            if used > 1000:
                time.sleep(4)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                time.sleep(8)
            elif resp.status_code == 418:
                time.sleep(60)
            else:
                time.sleep(1)
        except requests.RequestException:
            time.sleep(1 + attempt)
    return None


@st.cache_data(ttl=300, show_spinner=False)
def get_all_symbols() -> list[str]:
    data = api_get(EXCH_EP)
    if not data:
        return []
    return sorted(
        s["symbol"] for s in data["symbols"]
        if s["status"] == "TRADING"
        and s["contractType"] == "PERPETUAL"
        and s["quoteAsset"] == "USDT"
    )


def fetch_klines(symbol: str, interval: str, limit: int) -> list[dict] | None:
    data = api_get(KLINE_EP, params={"symbol": symbol, "interval": interval, "limit": limit})
    if not data or not isinstance(data, list):
        return None
    candles = []
    for k in data:
        o, h, l, c = float(k[1]), float(k[2]), float(k[3]), float(k[4])
        candles.append({
            "dt":    datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
            "open":  o, "high": h, "low": l, "close": c,
            "range": h - l,
        })
    # Always drop the last candle — it is still forming and its OHLC is not
    # final. Scanning an unclosed candle is the single biggest source of
    # false signals (the pattern "exists" then disappears on the next tick).
    candles = candles[:-1]
    return candles if len(candles) >= 5 else None

# ══════════════════════════════════════════════════════════════════════════════
# FVG DETECTION  — corrected orientation
# ══════════════════════════════════════════════════════════════════════════════

def find_fvgs(candles: list[dict]) -> list[dict]:
    """
    Fair Value Gap — standard 3-candle definition:

    BULLISH FVG  (gap left on the way UP):
      Candle sequence: C1 → C2 (impulse up) → C3
      Condition : C3.low > C1.high
                  ↳ C2 was a bullish impulse so large that C3's LOW is still
                    above C1's HIGH — the zone between C1.high and C3.low was
                    never traded on both sides.
      FVG zone  : low  = C1.high
                  high = C3.low
      C2 must be bullish (close > open) — confirms it was an impulse, not noise.

    BEARISH FVG  (gap left on the way DOWN):
      Condition : C1.low > C3.high
                  ↳ C2 was a bearish impulse so large that C3's HIGH is still
                    below C1's LOW.
      FVG zone  : low  = C3.high
                  high = C1.low
      C2 must be bearish (close < open).

    The launch candle is a candle that LATER opens inside this zone and
    closes back OUT of it in the direction of the original impulse, proving
    the imbalance absorbed the pullback and price is continuing.
    """
    fvgs = []
    n = len(candles)
    for i in range(2, n):
        c1, c2, c3 = candles[i-2], candles[i-1], candles[i]

        # ── BULLISH FVG ───────────────────────────────────────────────────────
        # C2 is the bullish impulse candle. Gap: C1.high (bottom) to C3.low (top).
        if (c3["low"] > c1["high"]          # gap exists
                and c2["close"] > c2["open"]  # C2 is bullish impulse
                and c2["close"] > c1["high"]  # C2 actually closed above C1.high
                and c3["low"]   > c2["open"]  # C3 did not fill back into C2
           ):
            fvg_low  = c1["high"]   # bottom of the untouched zone
            fvg_high = c3["low"]    # top of the untouched zone
            gap_size  = fvg_high - fvg_low
            c2_range  = c2["range"] if c2["range"] > 0 else 1
            size_pct  = round(gap_size / c2_range * 100, 2)
            fvgs.append({
                "direction":      "BULLISH",
                "fvg_high":       fvg_high,
                "fvg_low":        fvg_low,
                "size_pct":       size_pct,
                "impulse_idx":    i - 1,    # C2 index (the impulse candle)
                "c3_idx":         i,        # C3 index (first candle after gap)
                "c1_low":         c1["low"],  # structure low (SL reference)
                "c3_high_ref":    c3["high"], # C3's high (structure context)
            })

        # ── BEARISH FVG ───────────────────────────────────────────────────────
        # C2 is the bearish impulse candle. Gap: C3.high (bottom) to C1.low (top).
        elif (c1["low"]  > c3["high"]         # gap exists
                and c2["close"] < c2["open"]  # C2 is bearish impulse
                and c2["close"] < c1["low"]   # C2 actually closed below C1.low
                and c3["high"]  < c2["open"]  # C3 did not fill back into C2
             ):
            fvg_low  = c3["high"]   # bottom of the untouched zone
            fvg_high = c1["low"]    # top of the untouched zone
            gap_size  = fvg_high - fvg_low
            c2_range  = c2["range"] if c2["range"] > 0 else 1
            size_pct  = round(gap_size / c2_range * 100, 2)
            fvgs.append({
                "direction":      "BEARISH",
                "fvg_high":       fvg_high,
                "fvg_low":        fvg_low,
                "size_pct":       size_pct,
                "impulse_idx":    i - 1,
                "c3_idx":         i,
                "c1_high":        c1["high"],  # structure high (SL reference)
                "c3_low_ref":     c3["low"],
            })
    return fvgs


# ══════════════════════════════════════════════════════════════════════════════
# LAUNCH CANDLE DETECTION  — corrected logic
# ══════════════════════════════════════════════════════════════════════════════

def find_nearest_swing_high(candles: list[dict], from_idx: int, above: float) -> float:
    """Return the nearest swing high above `above` looking backward from from_idx."""
    for i in range(from_idx - 1, -1, -1):
        if candles[i]["high"] > above:
            return candles[i]["high"]
    return above  # fallback

def find_nearest_swing_low(candles: list[dict], from_idx: int, below: float) -> float:
    """Return the nearest swing low below `below` looking backward from from_idx."""
    for i in range(from_idx - 1, -1, -1):
        if candles[i]["low"] < below:
            return candles[i]["low"]
    return below  # fallback

def calc_rr(entry: float, sl: float, tp: float) -> float:
    risk   = abs(entry - sl)
    reward = abs(tp    - entry)
    return round(reward / risk, 2) if risk > 0 else 0.0

def fp(v: float) -> str:
    if v <= 0:      return "—"
    if v >= 10_000: return f"{v:,.2f}"
    if v >= 1:      return f"{v:.4f}"
    return f"{v:.6f}"


def detect_fvg_launch(candles: list[dict], symbol: str, tf: str,
                      lookback: int, min_fvg_pct: float) -> list[FVGLaunch]:
    """
    Two-pass scan:

    Pass 1 — find all valid FVGs in the full candle history.

    Pass 2 — for each FVG, scan candles that come AFTER the FVG formed
    and look for the launch candle:
      • The candle must have touched inside the FVG (low <= FVG.high for
        bullish; high >= FVG.low for bearish) — this is the mitigation touch.
      • The candle must CLOSE beyond the FVG boundary in the trade direction.
      • The candle body must be directional (bullish close>open / bearish close<open).
      • The open does NOT have to be inside the FVG — price can wick down into
        the FVG and then close above FVG.high; that is still a valid launch.

    Only the MOST RECENT launch candle is kept per FVG (we break after finding
    the first one scanning backwards from the most recent candle).
    """
    results: list[FVGLaunch] = []

    # Use the last (lookback + 20) closed candles — extra context for FVG formation
    tail = candles[-(lookback + 20):]
    tn   = len(tail)

    fvgs = find_fvgs(tail)
    if not fvgs:
        return results

    for fvg in fvgs:
        if fvg["size_pct"] < min_fvg_pct:
            continue

        fvg_high  = fvg["fvg_high"]
        fvg_low   = fvg["fvg_low"]
        fvg_dir   = fvg["direction"]
        c3_idx    = fvg["c3_idx"]
        fvg_range = fvg_high - fvg_low

        # The structural level the close must clear:
        #   Bullish → close must be above C3's HIGH (the candle after the gap)
        #   Bearish → close must be below C3's LOW
        c3_high = fvg.get("c3_high_ref", fvg_high)   # stored in find_fvgs
        c3_low  = fvg.get("c3_low_ref",  fvg_low)    # stored in find_fvgs

        # Only scan candles within the lookback window that appear AFTER the FVG
        scan_start = max(c3_idx + 1, tn - lookback)

        for j in range(scan_start, tn):
            c  = tail[j]
            o  = c["open"]
            h  = c["high"]
            l  = c["low"]
            cl = c["close"]
            cr = c["range"]

            body_size = abs(cl - o)
            body_pct  = round(body_size / cr * 100, 1) if cr > 0 else 0

            if fvg_dir == "BULLISH":
                # ── One end in the FVG, other end above C3.high ──────────────
                #
                # Condition 1 — LOW is inside the FVG zone
                #   (the candle wicked down into the imbalance — mitigation)
                #   l >= fvg_low  (didn't blow through the bottom of the FVG)
                #   l <= fvg_high (actually touched inside the gap)
                #
                # Condition 2 — CLOSE is ABOVE C3's high
                #   (not just above the FVG top, but above the entire C3 candle)
                #   This is the structural break — price has cleared C3 entirely.
                #
                # Condition 3 — Bullish body (close > open)
                #
                low_in_fvg      = (fvg_low * 0.997 <= l <= fvg_high)
                close_above_c3  = (cl > c3_high)
                bullish_body    = (cl > o)

                if low_in_fvg and close_above_c3 and bullish_body:
                    sl  = fvg_low * (1 - 0.003)           # below FVG low
                    tp1 = find_nearest_swing_high(tail, j, cl)
                    if tp1 <= cl:
                        tp1 = cl + fvg_range * 1.0
                    tp2 = cl + fvg_range * 1.618
                    tp3 = cl + fvg_range * 2.618

                    results.append(FVGLaunch(
                        direction="BULLISH", symbol=symbol, tf=tf, dt=c["dt"],
                        fvg_high=fvg_high, fvg_low=fvg_low,
                        fvg_size_pct=fvg["size_pct"],
                        lc_open=o, lc_high=h, lc_low=l, lc_close=cl,
                        lc_body_pct=body_pct,
                        sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                        rr1=calc_rr(cl, sl, tp1),
                        rr2=calc_rr(cl, sl, tp2),
                        rr3=calc_rr(cl, sl, tp3),
                        candle_idx=j, total_candles=tn,
                    ))
                    break  # one launch candle per FVG

            else:  # BEARISH
                # ── One end in the FVG, other end below C3.low ───────────────
                #
                # Condition 1 — HIGH is inside the FVG zone
                #   (the candle wicked up into the imbalance — mitigation)
                #   h >= fvg_low  (touched inside the gap)
                #   h <= fvg_high (didn't pierce through the top of the FVG)
                #
                # Condition 2 — CLOSE is BELOW C3's low
                #   (cleared the entire C3 candle to the downside — BOS)
                #
                # Condition 3 — Bearish body (close < open)
                #
                high_in_fvg    = (fvg_low <= h <= fvg_high * 1.003)
                close_below_c3 = (cl < c3_low)
                bearish_body   = (cl < o)

                if high_in_fvg and close_below_c3 and bearish_body:
                    sl  = fvg_high * (1 + 0.003)          # above FVG high
                    tp1 = find_nearest_swing_low(tail, j, cl)
                    if tp1 >= cl:
                        tp1 = cl - fvg_range * 1.0
                    tp2 = cl - fvg_range * 1.618
                    tp3 = cl - fvg_range * 2.618

                    results.append(FVGLaunch(
                        direction="BEARISH", symbol=symbol, tf=tf, dt=c["dt"],
                        fvg_high=fvg_high, fvg_low=fvg_low,
                        fvg_size_pct=fvg["size_pct"],
                        lc_open=o, lc_high=h, lc_low=l, lc_close=cl,
                        lc_body_pct=body_pct,
                        sl=sl, tp1=tp1, tp2=tp2, tp3=tp3,
                        rr1=calc_rr(cl, sl, tp1),
                        rr2=calc_rr(cl, sl, tp2),
                        rr3=calc_rr(cl, sl, tp3),
                        candle_idx=j, total_candles=tn,
                    ))
                    break

    return results


# ══════════════════════════════════════════════════════════════════════════════
# WORKER
# ══════════════════════════════════════════════════════════════════════════════
def scan_symbol(args: tuple):
    symbol, interval, lookback, tf, min_fvg_pct = args
    candles = fetch_klines(symbol, interval, lookback + 15)  # extra for FVG context
    if not candles:
        return symbol, [], True
    signals = detect_fvg_launch(candles, symbol, tf, lookback, min_fvg_pct)
    return symbol, signals, False


# ══════════════════════════════════════════════════════════════════════════════
# SIGNAL CARD HTML
# ══════════════════════════════════════════════════════════════════════════════
def signal_card(sig: FVGLaunch) -> str:
    is_bull  = sig.direction == "BULLISH"
    cls      = "bull-card" if is_bull else "bear-card"
    d_badge  = ('<span class="badge-bull">▲ BULLISH</span>'
                if is_bull else '<span class="badge-bear">▼ BEARISH</span>')
    col      = "#4ade80" if is_bull else "#f87171"
    sl_col   = "#f87171"
    tp_col   = "#38bdf8"
    fvg_bar_cls = "fvg-bull" if is_bull else "fvg-bear"

    dt_str   = sig.dt.strftime("%d %b %Y  %H:%M UTC")

    # FVG quality colour
    fq_col = "#4ade80" if sig.fvg_size_pct >= 20 else "#f0b429" if sig.fvg_size_pct >= 8 else "#94a3b8"
    # Body strength colour
    bq_col = "#4ade80" if sig.lc_body_pct >= 60 else "#f0b429" if sig.lc_body_pct >= 35 else "#94a3b8"

    def cell(lbl, val, color, rr=""):
        rr_html = f'<div class="rr" style="color:#94a3b8;">{rr}</div>' if rr else ""
        return (f'<div class="tp-cell">'
                f'<div class="lbl">{lbl}</div>'
                f'<div class="val" style="color:{color};">{val}</div>'
                f'{rr_html}</div>')

    return f"""
<div class="{cls}">
  <div style="display:flex;justify-content:space-between;align-items:center;gap:.5rem;flex-wrap:wrap;">
    <span style="color:#f1f5f9;font-weight:800;font-size:1.08rem;
                 font-family:'Space Grotesk',sans-serif;letter-spacing:-.01em;">{sig.symbol}</span>
    <div style="display:flex;gap:.35rem;align-items:center;flex-wrap:wrap;">
      {d_badge}
      <span class="badge-fvg">FVG LAUNCH</span>
      <span class="badge-bos">BOS</span>
      <span style="font-size:.6rem;color:#283550;font-family:'JetBrains Mono',monospace;">{sig.tf}</span>
    </div>
  </div>

  <div style="color:#283550;font-size:.67rem;margin-top:.3rem;font-family:'JetBrains Mono',monospace;">
    📅 {dt_str}
    &nbsp;·&nbsp; FVG size <span style="color:{fq_col};">{sig.fvg_size_pct}%</span>
    &nbsp;·&nbsp; Body <span style="color:{bq_col};">{sig.lc_body_pct}%</span>
    &nbsp;·&nbsp; Candle {sig.candle_idx}/{sig.total_candles}
  </div>

  <!-- FVG zone visual bar -->
  <div class="fvg-bar {fvg_bar_cls}" style="opacity:.6;"></div>

  <!-- FVG zone boundaries -->
  <div style="display:flex;gap:1.5rem;font-size:.68rem;font-family:'JetBrains Mono',monospace;
              color:#283550;margin-bottom:.55rem;flex-wrap:wrap;">
    <span>FVG High <b style="color:{col};">{fp(sig.fvg_high)}</b></span>
    <span>FVG Low &nbsp;<b style="color:{col};">{fp(sig.fvg_low)}</b></span>
    <span>LC Low &nbsp;&nbsp;<b style="color:#facc15;">{fp(sig.lc_low)}</b>
      <span style="color:#283550;font-size:.58rem;">(inside FVG)</span></span>
    <span>LC Close &nbsp;<b style="color:{col};">{fp(sig.lc_close)}</b>
      <span style="color:#283550;font-size:.58rem;">(above C3 high)</span></span>
  </div>

  <!-- Targets grid -->
  <div class="tp-row">
    {cell("Entry (close)", fp(sig.lc_close), col)}
    {cell("Stop loss", fp(sig.sl), sl_col, "beyond FVG")}
    {cell(f"TP1  ({sig.rr1}R)", fp(sig.tp1), tp_col, "swing high/low")}
    {cell(f"TP2  ({sig.rr2}R)", fp(sig.tp2), tp_col, "1.618 ext.")}
  </div>
  <div style="margin-top:.35rem;">
    {cell(f"TP3  ({sig.rr3}R) — runner", fp(sig.tp3), "#818cf8", "2.618 extension")}
  </div>
</div>"""


def log_row_html(sym: str, kind: str, n: int = 0) -> str:
    if kind == "hit":
        return f'<div class="log-row log-hit">✦ {sym} &nbsp;·&nbsp; {n} signal(s)</div>'
    if kind == "err":
        return f'<div class="log-row log-err">✗ {sym}</div>'
    return f'<div class="log-row">· {sym}</div>'


def build_csv(signals: list[FVGLaunch]) -> bytes:
    rows = [{
        "Direction":    s.direction,
        "Symbol":       s.symbol,
        "Timeframe":    s.tf,
        "Datetime":     s.dt.strftime("%Y-%m-%d %H:%M"),
        "FVG High":     s.fvg_high,
        "FVG Low":      s.fvg_low,
        "FVG Size %":   s.fvg_size_pct,
        "LC Open":      s.lc_open,
        "LC High":      s.lc_high,
        "LC Low":       s.lc_low,
        "LC Close":     s.lc_close,
        "Body %":       s.lc_body_pct,
        "Entry":        s.lc_close,
        "Stop Loss":    s.sl,
        "TP1":          s.tp1,
        "TP2 (1.618)":  s.tp2,
        "TP3 (2.618)":  s.tp3,
        "R:R TP1":      s.rr1,
        "R:R TP2":      s.rr2,
        "R:R TP3":      s.rr3,
    } for s in signals]
    buf = io.StringIO()
    pd.DataFrame(rows).to_csv(buf, index=False)
    return buf.getvalue().encode()


# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("### 🚀 Scanner Config")
    st.markdown("---")

    tf_choice = st.selectbox("⏱ Timeframe", ["1H", "4H", "1D", "1W"], index=1)
    lookback  = st.selectbox("🔭 Candles to scan", [5, 10, 15], index=1)

    st.markdown("---")
    st.markdown("### 📐 FVG Quality")
    min_fvg_pct = st.slider(
        "Min FVG size (%)",
        min_value=1, max_value=40, value=5,
        help="FVG gap size as % of avg candle range. Higher = cleaner imbalances only."
    )
    min_body_pct = st.slider(
        "Min launch candle body (%)",
        min_value=10, max_value=80, value=30,
        help="Body size / candle range. Higher = stronger conviction launch candles."
    )
    min_rr = st.slider(
        "Min R:R (TP1)",
        min_value=0.5, max_value=5.0, value=1.0, step=0.5,
        help="Only show setups where TP1 R:R meets this threshold."
    )

    st.markdown("---")
    st.markdown("### 🌐 Universe")
    with st.spinner("Loading Binance symbols…"):
        ALL_SYMBOLS = get_all_symbols()
    if not ALL_SYMBOLS:
        st.error("Cannot load symbols.")

    universe = st.radio("Scan", ["All USDT Perpetuals", "Custom symbols"], index=0)
    if universe == "Custom symbols":
        raw     = st.text_area("Symbols (comma / newline)",
                               value="BTCUSDT, ETHUSDT, SOLUSDT, BNBUSDT, XRPUSDT",
                               height=110)
        valid   = set(ALL_SYMBOLS)
        tickers = [t.strip().upper() for t in raw.replace("\n", ",").split(",") if t.strip()]
        tickers = [t for t in tickers if t in valid] or tickers
    else:
        tickers = ALL_SYMBOLS

    st.caption(f"**{len(tickers)}** symbols selected")

    st.markdown("---")
    st.markdown("### 🔽 Direction")
    show_bull = st.checkbox("▲ Bullish FVG launches", value=True)
    show_bear = st.checkbox("▼ Bearish FVG launches", value=True)

    st.markdown("---")
    st.markdown("### ⚡ Speed")
    workers  = st.slider("Parallel workers", 1, 25, 12)
    delay_ms = st.slider("Batch delay (ms)", 0, 400, 80, step=20)

    st.markdown("---")
    run_btn  = st.button("▶  SCAN NOW")
    stop_btn = st.button("⏹  STOP")
    if stop_btn:
        st.session_state["_stop"] = True

# ══════════════════════════════════════════════════════════════════════════════
# HEADER
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<div style="margin-bottom:.3rem;">
  <span style="font-family:'Space Grotesk',sans-serif;font-weight:800;font-size:2rem;
    letter-spacing:-.04em;
    background:linear-gradient(115deg,#a78bfa 0%,#818cf8 40%,#38bdf8 80%);
    -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">
    🚀 FVG Launch Candle Scanner
  </span>
</div>
<div style="font-family:'JetBrains Mono',monospace;font-size:.67rem;color:#283550;
  letter-spacing:.18em;text-transform:uppercase;margin-bottom:.6rem;">
  Binance USDT-M Perpetuals · FVG Mitigation + BOS Confluence · No API Key
</div>
""", unsafe_allow_html=True)

# Strategy info bar
st.markdown("""
<div class="info-bar">
  <b style="color:#a78bfa;">Pattern:</b>
  Price pulls back into a Fair Value Gap →
  <b style="color:#facc15;">Launch candle opens inside FVG</b> →
  <b style="color:#4ade80;">Closes beyond FVG boundary</b> (BOS in one candle)
  &nbsp;&nbsp;|&nbsp;&nbsp;
  <b style="color:#a78bfa;">Targets:</b>
  TP1 = Swing high/low &nbsp;·&nbsp; TP2 = 1.618 FVG ext. &nbsp;·&nbsp; TP3 = 2.618 ext. (runner)
  &nbsp;&nbsp;|&nbsp;&nbsp;
  <b style="color:#a78bfa;">SL:</b> 0.3% beyond opposite FVG boundary
</div>
""", unsafe_allow_html=True)

h1, h2 = st.columns([1, 4])
with h1:
    st.markdown(
        '<span style="background:#07091a;border:1px solid #2d1f55;border-radius:999px;'
        'padding:.18rem .75rem;font-family:\'JetBrains Mono\',monospace;font-size:.65rem;'
        'color:#a78bfa;letter-spacing:.12em;">'
        '<span class="live-dot"></span>BINANCE LIVE</span>',
        unsafe_allow_html=True,
    )
with h2:
    st.markdown(
        f'<span style="font-family:\'JetBrains Mono\',monospace;font-size:.72rem;color:#283550;">'
        f'{len(tickers)} symbols &nbsp;·&nbsp; {tf_choice} &nbsp;·&nbsp; '
        f'last {lookback} candles &nbsp;·&nbsp; min FVG {min_fvg_pct}% &nbsp;·&nbsp; '
        f'min R:R {min_rr} &nbsp;·&nbsp; {workers} workers</span>',
        unsafe_allow_html=True,
    )

st.markdown("")

# ── Metrics ───────────────────────────────────────────────────────────────────
mc1, mc2, mc3, mc4, mc5 = st.columns(5)
m_total   = mc1.empty()
m_scanned = mc2.empty()
m_signals = mc3.empty()
m_bull    = mc4.empty()
m_bear    = mc5.empty()

def upd(total, scanned, signals, bull, bear):
    m_total.metric("Symbols",   total)
    m_scanned.metric("Scanned", f"{scanned}/{total}")
    m_signals.metric("🚀 Setups",  signals)
    m_bull.metric("▲ Bullish",  bull)
    m_bear.metric("▼ Bearish",  bear)

upd(len(tickers), 0, 0, 0, 0)

prog_bar   = st.progress(0.0)
status_txt = st.empty()

# ── Layout ────────────────────────────────────────────────────────────────────
st.markdown('<div class="sec-hdr">Scan output</div>', unsafe_allow_html=True)
log_col, res_col = st.columns([1, 2], gap="medium")
with log_col:
    st.markdown('<div class="sec-hdr" style="font-size:.55rem;">Progress log</div>',
                unsafe_allow_html=True)
    log_ph = st.empty()
with res_col:
    st.markdown('<div class="sec-hdr" style="font-size:.55rem;">Confirmed setups</div>',
                unsafe_allow_html=True)
    res_ph = st.empty()

# ══════════════════════════════════════════════════════════════════════════════
# RUN SCAN
# ══════════════════════════════════════════════════════════════════════════════
if run_btn:
    st.session_state["_stop"] = False

    interval = TF_MAP[tf_choice]
    delay_s  = delay_ms / 1000.0
    total    = len(tickers)

    log_lines:   list[str]        = []
    result_html: str              = ""
    all_sigs:    list[FVGLaunch]  = []
    scanned = total_signals = bull_count = bear_count = 0

    status_txt.info(
        f"🔍 Scanning **{total}** Binance USDT-M perpetuals for FVG launch candles "
        f"on **{tf_choice}**, last **{lookback}** candles…"
    )

    for batch_start in range(0, total, workers):
        if st.session_state.get("_stop"):
            status_txt.warning("⏹ Scan stopped.")
            break

        batch = tickers[batch_start : batch_start + workers]
        args  = [(sym, interval, lookback, tf_choice, min_fvg_pct) for sym in batch]

        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            futs = {pool.submit(scan_symbol, a): a[0] for a in args}

            for fut in as_completed(futs):
                if st.session_state.get("_stop"):
                    break

                sym, sigs, had_error = fut.result()
                scanned += 1

                if had_error:
                    log_lines.append(log_row_html(sym, "err"))
                elif sigs:
                    # Apply quality filters
                    filtered = [
                        s for s in sigs
                        if s.lc_body_pct >= min_body_pct
                        and s.rr1 >= min_rr
                        and (show_bull if s.direction == "BULLISH" else show_bear)
                    ]
                    if filtered:
                        total_signals += len(filtered)
                        log_lines.append(log_row_html(sym, "hit", len(filtered)))
                        for sig in filtered:
                            all_sigs.append(sig)
                            if sig.direction == "BULLISH":
                                bull_count += 1
                            else:
                                bear_count += 1
                            result_html = signal_card(sig) + result_html
                    else:
                        log_lines.append(log_row_html(sym, "ok"))
                else:
                    log_lines.append(log_row_html(sym, "ok"))

                # Live updates
                log_ph.markdown("".join(log_lines[-30:]), unsafe_allow_html=True)
                res_ph.markdown(
                    f'<div class="res-pane">{result_html}</div>'
                    if result_html else
                    '<div class="log-row" style="color:#1a2040;">Waiting for setups…</div>',
                    unsafe_allow_html=True,
                )
                prog_bar.progress(min(scanned / total, 1.0))
                upd(total, scanned, total_signals, bull_count, bear_count)

        if delay_s > 0 and not st.session_state.get("_stop"):
            time.sleep(delay_s)

    # ── Done ──────────────────────────────────────────────────────────────────
    prog_bar.progress(1.0)
    if not st.session_state.get("_stop"):
        status_txt.success(
            f"✅ Scan complete — **{scanned}** symbols · "
            f"**{total_signals}** FVG launch setups · "
            f"**{bull_count}** bullish · **{bear_count}** bearish"
        )

    if not result_html:
        res_ph.markdown(
            '<div class="log-row" style="color:#a78bfa;font-size:.82rem;padding:.8rem;">'
            '0 setups found. Try: reduce min FVG % · reduce min body % · '
            'increase lookback · switch timeframe.</div>',
            unsafe_allow_html=True,
        )

    # ── Export ─────────────────────────────────────────────────────────────────
    if all_sigs:
        st.markdown('<div class="sec-hdr">Export</div>', unsafe_allow_html=True)
        tbl_col, dl_col = st.columns([3, 1], gap="medium")

        with tbl_col:
            df_show = pd.DataFrame([{
                "Dir":       s.direction,
                "Symbol":    s.symbol,
                "Time":      s.dt.strftime("%d %b %H:%M"),
                "FVG %":     s.fvg_size_pct,
                "Body %":    s.lc_body_pct,
                "Entry":     s.lc_close,
                "SL":        s.sl,
                "TP1":       s.tp1,
                "TP2":       s.tp2,
                "TP3":       s.tp3,
                "R:R TP1":   s.rr1,
                "R:R TP2":   s.rr2,
            } for s in all_sigs])

            def col_dir(v):
                return "color: #4ade80" if v == "BULLISH" else "color: #f87171"

            st.dataframe(
                df_show.style.applymap(col_dir, subset=["Dir"]),
                use_container_width=True, height=280,
            )

        with dl_col:
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
            st.download_button(
                label="⬇ Download CSV",
                data=build_csv(all_sigs),
                file_name=f"fvg_launch_{tf_choice}_{ts}.csv",
                mime="text/csv",
            )
            st.markdown(
                f'<div style="font-family:\'JetBrains Mono\',monospace;font-size:.7rem;'
                f'color:#283550;margin-top:.6rem;line-height:1.9;">'
                f'Total &nbsp;: {len(all_sigs)}<br>'
                f'Bullish : {bull_count}<br>'
                f'Bearish : {bear_count}</div>',
                unsafe_allow_html=True,
            )
