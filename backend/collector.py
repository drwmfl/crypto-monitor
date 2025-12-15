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

# 🔥 [配置 1] 特殊符号映射表 (Binance Symbol -> CMC Symbol)
SPECIAL_CMC_MAPPING = {
    'LUNA2': 'LUNA',      
    '1000LUNC': 'LUNC',   
    '1000SATS': 'SATS',
    '1000PEPE': 'PEPE',
    '1000BONK': 'BONK',
    '1000RATS': 'RATS',
    'MYRO': 'MYRO',
}

# 🔥 [配置 2] ID 专用映射表 (Binance Symbol -> CMC ID)
SPECIAL_CMC_IDS = {
    '币安人生': '38590', # 兼容中文名配置
    'MOVE': '32452', 
}

# ================= 辅助函数 =================

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0

def fetch_cmc_data(symbol_list):
    """
    [升级版] 批量获取 CMC 数据
    支持双轨制：大部分币查 Symbol，特殊币查 ID
    """
    if not CMC_API_KEY or '你的' in CMC_API_KEY: return {}
    
    # 1. 准备工作：分拣篮子
    normal_symbols = set() # 存放普通币 (查 Symbol)
    id_map = {}            # 存放 ID 映射关系 (CMC_ID -> Binance_Symbol)
    ids_to_fetch = set()   # 存放需要查询的 ID 列表

    for s in symbol_list:
        # 清理 Binance 的名字 (去除 /USDT)
        base = s.split('/')[0]
        
        # 处理 1000 前缀 (如 1000PEPE -> PEPE)
        if base.startswith('1000') and len(base) > 4: 
            base = base[4:]
            
        # [判断] 是否在 VIP ID 名单里 (优先处理 ID)
        if base in SPECIAL_CMC_IDS:
            cmc_id = SPECIAL_CMC_IDS[base]
            ids_to_fetch.add(cmc_id)
            id_map[str(cmc_id)] = base 
            continue 

        # [判断] 普通映射 (处理 LUNA2 -> LUNA)
        base = SPECIAL_CMC_MAPPING.get(base, base)
        
        # 只要是正常的英文名，就加入普通查询队列
        if re.match(r'^[a-zA-Z0-9]+$', base): 
            normal_symbols.add(base)
            
    cmc_map = {}
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    
    # ================= 第一轨：普通 Symbol 查询 =================
    batch_size = 100
    normal_list = list(normal_symbols)
    
    if normal_list:
        for i in range(0, len(normal_list), batch_size):
            batch = normal_list[i : i + batch_size]
            try:
                # 这里的参数是 symbol
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
            except Exception as e:
                print(f"[CMC] Symbol query error: {e}")
                continue

    # ================= 第二轨：特殊 ID 查询 =================
    if ids_to_fetch:
        try:
            # 这里的参数是 id
            response = requests.get(url, headers=headers, params={'id': ','.join(ids_to_fetch), 'convert': 'USD'}, timeout=5)
            if response.status_code == 200:
                data = response.json().get('data', {})
                # data 的 Key 是数字 ID (如 "38590")
                for cmc_id, info in data.items():
                    quote = info['quote']['USD']
                    
                    # 关键步骤：把 ID 映射回原来的币安名字
                    original_symbol = id_map.get(str(cmc_id))
                    
                    if original_symbol:
                        cmc_map[original_symbol] = {
                            'mc': quote.get('market_cap', 0), 
                            'fdv': quote.get('fully_diluted_market_cap', 0)
                        }
        except Exception as e:
            print(f"[CMC] ID query error: {e}")

    return cmc_map

def process_single_symbol(symbol, exchange_instance, ticker, interval_map, funding_map, cmc_map, spot_symbols_set):
    """
    处理单个交易对数据，增加 spot_symbols_set 用于检查现货
    """
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

        # 🔥 [核心逻辑] 获取 CMC 数据
        lookup_symbol = base_symbol[4:] if base_symbol.startswith('1000') else base_symbol
        lookup_symbol = SPECIAL_CMC_MAPPING.get(lookup_symbol, lookup_symbol)
        
        mc = cmc_map.get(lookup_symbol, {}).get('mc', 0)
        fdv = cmc_map.get(lookup_symbol, {}).get('fdv', 0)
        
        ratio_mc = val_usdt / mc if mc > 0 else 0.0

        # 🔥 [新增逻辑] 检查是否有现货
        # 逻辑：如果是 1000PEPE，要去现货里找 PEPE/USDT
        check_spot_base = base_symbol[4:] if base_symbol.startswith('1000') else base_symbol
        spot_pair_name = f"{check_spot_base}/USDT"
        
        # 判断是否存在于现货集合中
        has_spot = spot_pair_name in spot_symbols_set

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
            'oi_vol_ratio': ratio_vol,
            'has_spot': has_spot  # ✅ 返回现货状态
        }
    except Exception: 
        return None

def get_market_data():
    # 1. 实例化合约客户端
    exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    
    # 🔥 2. 实例化现货客户端 (专门查名单)
    spot_exchange = ccxt.binance({'enableRateLimit': True})
    
    try:
        # 加载合约市场
        markets = exchange.load_markets()
        tickers = exchange.fetch_tickers()
        funding_rates = exchange.fetch_funding_rates()
        
        # 🔥 加载现货市场
        print("🔍 正在获取现货市场列表...")
        try:
            spot_markets = spot_exchange.load_markets()
            # 做成一个集合，查找速度快。格式如: 'BTC/USDT', 'ETH/USDT'
            spot_symbols_set = set(spot_markets.keys())
            print(f"✅ 获取到 {len(spot_symbols_set)} 个现货交易对")
        except Exception as e:
            print(f"⚠️ 获取现货列表失败: {e}")
            spot_symbols_set = set() # 如果失败，默认都没现货，防止程序崩溃
        
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
        # 调用 CMC 数据获取
        cmc_data_map = fetch_cmc_data(cmc_query_list)
        data_list = []
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # ✅ 将 spot_symbols_set 传递给处理函数
            futures = {executor.submit(process_single_symbol, s, exchange, tickers[s], interval_map, funding_map, cmc_data_map, spot_symbols_set): s for s in target_symbols}
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
                oi_vol_ratio=row['oi_vol_ratio'],
                has_spot=row['has_spot'] # ✅ 存入数据库
            )
            data_objects.append(item)
            
        db.add_all(data_objects)
        db.commit()
        print(f"✅ [{current_time.strftime('%H:%M:%S')}] 存入 {len(data_objects)} 条数据")
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