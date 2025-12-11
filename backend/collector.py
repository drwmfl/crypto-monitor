import time
import ccxt
import pandas as pd
import requests
import re
import warnings
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy import desc
from database import SessionLocal, engine, Base
from models import MarketData

# ================= 配置部分 =================
warnings.filterwarnings("ignore")
MIN_VOLUME_USDT = 10_000_000  
TARGET_QUOTE = 'USDT'
MAX_WORKERS = 10
CMC_API_KEY = os.getenv("CMC_API_KEY", "909286096de241b8868ad1d075c86ebb")

EXCLUDE_SYMBOLS = [
    'BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOGE', 'TRX', 'USDC', 'FDUSD', 
    'AVAX', 'LINK', 'DOT', 'MATIC', 'WBTC', 'LTC', 'BCH', 'XPR', 'ETC', 'UNI'
]

# 🔥 [新增] 特殊符号映射表 (Binance -> CMC)
# 格式: '币安的名字': 'CMC的名字'
SPECIAL_CMC_MAPPING = {
    'LUNA2': 'LUNA',      # 币安叫 LUNA2，CMC 叫 LUNA
    '1000LUNC': 'LUNC',   # 预防万一，处理 LUNC
    # 如果以后还有名字不一样的，加在这里即可
}

# ================= 辅助函数 =================

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0

def fetch_cmc_data(symbol_list):
    """批量获取 CMC 数据"""
    if not CMC_API_KEY or '你的' in CMC_API_KEY: return {}
    
    clean_symbols = []
    for s in symbol_list:
        base = s.split('/')[0]
        # 处理 1000 前缀 (如 1000PEPE -> PEPE)
        if base.startswith('1000') and len(base) > 4: 
            base = base[4:]
            
        # 🔥 [修改] 应用特殊映射：如果是 LUNA2，转成 LUNA
        base = SPECIAL_CMC_MAPPING.get(base, base)
        
        if re.match(r'^[a-zA-Z0-9]+$', base): 
            clean_symbols.append(base)
            
    unique_symbols = list(set(clean_symbols))
    if not unique_symbols: return {}
    
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    cmc_map = {}
    batch_size = 100
    
    for i in range(0, len(unique_symbols), batch_size):
        batch = unique_symbols[i : i + batch_size]
        try:
            # 这里的 batch 里已经包含了 'LUNA' 而不是 'LUNA2'
            response = requests.get(url, headers=headers, params={'symbol': ','.join(batch), 'convert': 'USD'}, timeout=5)
            if response.status_code == 200:
                data = response.json().get('data', {})
                for sym, info in data.items():
                    if isinstance(info, list): info = info[0]
                    quote = info['quote']['USD']
                    cmc_map[sym] = {
                        'mc': quote.get('market_cap', 0), 
                        'fdv': quote.get('fully_diluted_market_cap', 0)
                    }
            time.sleep(0.1)
        except: continue
    return cmc_map

def process_single_symbol(symbol, exchange_instance, ticker, interval_map, funding_map, cmc_map):
    try:
        market_info = exchange_instance.market(symbol)
        raw_id = market_info['id']
        base_symbol = market_info['base']
        price = safe_float(ticker['last'])
        if price == 0: return None

        # 1. 获取持仓量
        val_usdt = 0.0
        try:
            oi_data = exchange_instance.fetch_open_interest(symbol)
            val_usdt = safe_float(oi_data.get('openInterestValue'))
            amount_coin = safe_float(oi_data.get('openInterestAmount')) or safe_float(oi_data.get('openInterest'))
            if val_usdt == 0 and amount_coin > 0: val_usdt = amount_coin * price
        except: pass

        # 2. 获取 1h 涨跌
        change_1h = 0.0
        try:
            klines = exchange_instance.fetch_ohlcv(symbol, timeframe='1h', limit=1)
            if klines:
                open_price_1h = float(klines[0][1])
                if open_price_1h > 0:
                    change_1h = (price - open_price_1h) / open_price_1h * 100
        except: pass

        interval = interval_map.get(raw_id, 8)
        volume_24h = safe_float(ticker.get('quoteVolume'))
        change_24h = safe_float(ticker.get('percentage'))
        
        ratio_vol = val_usdt / volume_24h if volume_24h > 0 else 0.0

        # 🔥 [修改] 获取 CMC 数据时的映射逻辑
        # 先处理 1000 前缀
        lookup_symbol = base_symbol[4:] if base_symbol.startswith('1000') else base_symbol
        # 再应用特殊映射 (LUNA2 -> LUNA)
        lookup_symbol = SPECIAL_CMC_MAPPING.get(lookup_symbol, lookup_symbol)
        
        # 现在用 'LUNA' 去字典里查，就能查到了
        mc = cmc_map.get(lookup_symbol, {}).get('mc', 0)
        fdv = cmc_map.get(lookup_symbol, {}).get('fdv', 0)
        
        ratio_mc = val_usdt / mc if mc > 0 else 0.0

        return {
            'symbol': symbol.replace(f':{TARGET_QUOTE}', ''),
            'price': price,
            'change_1h': change_1h,
            'change_24h': change_24h,
            'mc': mc,
            'fdv': fdv,
            'volume_24h': volume_24h,
            'funding_rate': funding_map.get(symbol, 0.0),
            'listing_hours': interval,
            'oi': val_usdt,
            'oi_mc_ratio': ratio_mc,
            'oi_vol_ratio': ratio_vol
        }
    except Exception: 
        return None

def get_market_data():
    exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    try:
        markets = exchange.load_markets()
        tickers = exchange.fetch_tickers()
        funding_rates = exchange.fetch_funding_rates()
        
        interval_map = {}
        try:
            special_info = exchange.fapiPublicGetFundingInfo()
            for item in special_info: interval_map[item['symbol']] = int(item['fundingIntervalHours'])
        except: pass 

        funding_map = {k: safe_float(v['info'].get('lastFundingRate')) for k, v in funding_rates.items()}
        target_symbols = []
        cmc_query_list = []

        for s, t in tickers.items():
            market_info = markets.get(s)
            if not market_info or not market_info.get('active'): continue
            if 'info' in market_info and market_info['info'].get('status') != 'TRADING': continue
            if not s.endswith(f':{TARGET_QUOTE}'): continue
            
            vol = safe_float(t.get('quoteVolume'))
            if vol < MIN_VOLUME_USDT: continue
            if market_info['base'] in EXCLUDE_SYMBOLS: continue
            
            target_symbols.append(s)
            cmc_query_list.append(s)

        print(f"🎯 目标监控币种数: {len(target_symbols)}")
        cmc_data_map = fetch_cmc_data(cmc_query_list)
        data_list = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_symbol, s, exchange, tickers[s], interval_map, funding_map, cmc_data_map): s for s in target_symbols}
            for future in as_completed(futures):
                res = future.result()
                if res: data_list.append(res)
        
        return pd.DataFrame(data_list)
    except Exception as e:
        print(f"Error getting market data: {e}")
        return pd.DataFrame()

def save_to_postgres(df: pd.DataFrame):
    if df.empty: return
    db = SessionLocal()
    try:
        current_time = pd.Timestamp.now()
        
        previous_oi_map = {}
        last_record = db.query(MarketData).order_by(desc(MarketData.timestamp)).first()
        if last_record:
            last_timestamp = last_record.timestamp
            old_data = db.query(MarketData.symbol, MarketData.oi).filter(MarketData.timestamp == last_timestamp).all()
            previous_oi_map = {row.symbol: row.oi for row in old_data}

        data_objects = []
        for _, row in df.iterrows():
            current_oi = row['oi']
            prev_oi = previous_oi_map.get(row['symbol'], current_oi)
            
            oi_change_val = current_oi - prev_oi
            oi_change_pct = 0.0
            if prev_oi > 0:
                oi_change_pct = oi_change_val / prev_oi

            item = MarketData(
                timestamp=current_time,
                symbol=row['symbol'],
                price=row['price'],
                change_1h=row['change_1h'],
                change_24h=row['change_24h'],
                mc=row['mc'],
                fdv=row['fdv'],
                volume_24h=row['volume_24h'],
                funding_rate=row['funding_rate'],
                listing_hours=row['listing_hours'],
                oi=row['oi'],
                oi_change_val=oi_change_val,
                oi_change_pct=oi_change_pct,
                oi_mc_ratio=row['oi_mc_ratio'],
                oi_vol_ratio=row['oi_vol_ratio']
            )
            data_objects.append(item)
            
        db.add_all(data_objects)
        db.commit()
        print(f"✅ [{current_time.strftime('%H:%M:%S')}] 存入 {len(data_objects)} 条数据 (LUNA2 已修正)")
    except Exception as e:
        print(f"❌ 数据库写入错误: {e}")
        db.rollback()
    finally:
        db.close()

def run_collector():
    print("🚀 采集器后台进程已启动...")
    while True:
        try:
            df = get_market_data()
            if not df.empty:
                save_to_postgres(df)
            else:
                print("⚠️ 本轮未获取到数据")
        except Exception as e:
            print(f"❌ 采集循环异常: {e}")
        print("💤 休眠 300 秒...")
        time.sleep(300)

if __name__ == "__main__":
    run_collector()