import time
import ccxt
import pandas as pd
import requests
import re
import warnings
import os
import redis
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
CMC_API_KEY = os.getenv("CMC_API_KEY", "d5a96ddc6d7340d59be22e18e64eab66")

# Redis 连接 (带调试信息)
print("🔌 [Collector] 正在初始化 Redis 连接...")
try:
    # 如果是在本地运行且没有用 Docker 网络，可能需要改成 localhost
    redis_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    redis_client.ping() # 测试连接
    print("✅ [Collector] Redis 连接成功！")
except Exception as e:
    print(f"⚠️ [Collector] Redis 连接警告: {e}")
    redis_client = None

EXCLUDE_SYMBOLS = [
    'BTC', 'ETH', 'BNB', 'SOL', 'XRP', 'ADA', 'DOGE', 'TRX', 'USDC', 'FDUSD', 
    'AVAX', 'LINK', 'DOT', 'MATIC', 'WBTC', 'LTC', 'BCH', 'XPR', 'ETC', 'UNI'
]

# 特殊符号映射表 (Binance -> CMC)
SPECIAL_CMC_MAPPING = {
    'LUNA2': 'LUNA',      
    '1000LUNC': 'LUNC',   
    '1000SATS': 'SATS',
    '1000PEPE': 'PEPE',
    '1000RATS': 'RATS',
    '1000BONK': 'BONK',
    '1000FLOKI': 'FLOKI',
    'MYRO': 'MYRO',
}

SPECIAL_CMC_IDS = {
    '币安人生': '38590',
    'MOVE': '32452',
    'SAPIEN': '38117',
    'TRADOOR': '36737',
    'TAC': '37338',
    'B2': '36352',
 
}

# ================= 辅助函数 =================

def safe_float(val):
    try:
        return float(val) if val is not None else 0.0
    except:
        return 0.0

def fetch_cmc_data(symbol_list):
    """
    [调试版] 获取供应量，并打印详细的 API 错误信息
    """
    if not CMC_API_KEY or '你的' in CMC_API_KEY: 
        print("❌ [CMC] API Key 未配置！")
        return {}
    
    print(f"🌍 [CMC] 正在请求 API... 目标: {len(symbol_list)} 个币种")
    
    normal_symbols = set()
    id_map = {}
    ids_to_fetch = set()

    for s in symbol_list:
        base = s.split('/')[0]
        if base.startswith('1000') and len(base) > 4: base = base[4:]
        if base in SPECIAL_CMC_IDS:
            cmc_id = SPECIAL_CMC_IDS[base]
            ids_to_fetch.add(cmc_id)
            id_map[str(cmc_id)] = base 
            continue 
        base = SPECIAL_CMC_MAPPING.get(base, base)
        if re.match(r'^[a-zA-Z0-9]+$', base): normal_symbols.add(base)
            
    cmc_map = {}
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest'
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    
    def parse_response(response_data):
        result_map = {}
        for key, info in response_data.items():
            if isinstance(info, list): info = info[0]
            circulating = info.get('circulating_supply')
            max_sup = info.get('max_supply')
            if max_sup is None:
                max_sup = info.get('total_supply')
            result_map[key] = {
                'supply': safe_float(circulating),
                'max_supply': safe_float(max_sup)
            }
        return result_map

    # 1. 普通 Symbol 查询
    batch_size = 100
    normal_list = list(normal_symbols)
    if normal_list:
        print(f"🔍 [CMC] 正在查询 {len(normal_list)} 个普通 Symbol...")
        for i in range(0, len(normal_list), batch_size):
            batch = normal_list[i : i + batch_size]
            try:
                response = requests.get(url, headers=headers, params={'symbol': ','.join(batch), 'convert': 'USD'}, timeout=10)
                
                # 🔥 [关键修改] 打印错误详情
                if response.status_code == 200:
                    data = response.json().get('data', {})
                    cmc_map.update(parse_response(data))
                    print(f"✅ [CMC] 批次 {i//batch_size + 1} 成功")
                else:
                    print(f"❌ [CMC] 请求失败! 状态码: {response.status_code}")
                    print(f"❌ [CMC] 错误信息: {response.text}")
                    
                time.sleep(0.1)
            except Exception as e:
                print(f"❌ [CMC] 网络异常: {e}")

    # 2. ID 查询
    if ids_to_fetch:
        try:
            response = requests.get(url, headers=headers, params={'id': ','.join(ids_to_fetch), 'convert': 'USD'}, timeout=5)
            if response.status_code == 200:
                data = response.json().get('data', {})
                parsed = parse_response(data)
                for cmc_id, val in parsed.items():
                    original_symbol = id_map.get(str(cmc_id))
                    if original_symbol:
                        cmc_map[original_symbol] = val
            else:
                print(f"❌ [CMC ID] 请求失败: {response.status_code}")
        except Exception as e:
            print(f"❌ [CMC ID] 异常: {e}")

    print(f"📊 [CMC] 最终获取了 {len(cmc_map)} 个币种的数据")
    return cmc_map

def process_single_symbol(symbol, exchange_instance, ticker, interval_map, funding_map, cmc_map, spot_symbols_set):
    try:
        market_info = exchange_instance.market(symbol)
        raw_id = market_info['id']
        base_symbol = market_info['base']
        price = safe_float(ticker['last'])
        if price == 0: return None

        # 1. 获取持仓量 (OI)
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
            if klines and float(klines[0][1]) > 0:
                change_1h = (price - float(klines[0][1])) / float(klines[0][1]) * 100
        except: pass

        interval = interval_map.get(raw_id, 8)
        volume_24h = safe_float(ticker.get('quoteVolume'))
        change_24h = safe_float(ticker.get('percentage'))
        ratio_vol = val_usdt / volume_24h if volume_24h > 0 else 0.0

        # 🔥 [核心修改] 处理供应量 (解决 1000 前缀导致市值放大 1000 倍的问题)
        
        # 判断是否是 1000 开头的代币
        is_1000_token = base_symbol.startswith('1000') and len(base_symbol) > 4
        
        # 获取 CMC 原始数据
        lookup_symbol = base_symbol[4:] if is_1000_token else base_symbol
        lookup_symbol = SPECIAL_CMC_MAPPING.get(lookup_symbol, lookup_symbol)
        
        cmc_data = cmc_map.get(lookup_symbol, {})
        raw_supply = cmc_data.get('supply', 0)
        raw_max_supply = cmc_data.get('max_supply', 0)
        
        # 计算有效供应量 (Effective Supply)
        # 如果币安价格是 1000 倍，我们就把供应量除以 1000，抵消价格的放大
        if is_1000_token:
            supply = raw_supply / 1000.0 if raw_supply else 0
            max_supply = raw_max_supply / 1000.0 if raw_max_supply else 0
        else:
            supply = raw_supply
            max_supply = raw_max_supply
        
        # 实时计算 (使用修正后的 supply)
        mc = price * supply
        fdv = price * max_supply
        ratio_mc = val_usdt / mc if mc > 0 else 0.0

        # 🔥 [写入 Redis] 存入修正后的供应量，供 Streamer 使用
        if redis_client:
            try:
                clean_symbol = symbol.split(':')[0].replace('/', '')
                if supply > 0:
                    redis_client.set(f"supply:{clean_symbol}", supply, ex=90000)
                if max_supply > 0:
                    redis_client.set(f"max_supply:{clean_symbol}", max_supply, ex=90000)
            except Exception:
                pass

        check_spot_base = base_symbol[4:] if base_symbol.startswith('1000') else base_symbol
        has_spot = f"{check_spot_base}/USDT" in spot_symbols_set

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
            'has_spot': has_spot 
        }
    except Exception: 
        return None

def get_market_data(cached_cmc_data):
    exchange = ccxt.binance({'enableRateLimit': True, 'options': {'defaultType': 'future'}})
    spot_exchange = ccxt.binance({'enableRateLimit': True})
    
    try:
        markets = exchange.load_markets()
        tickers = exchange.fetch_tickers()
        funding_rates = exchange.fetch_funding_rates()
        try:
            spot_markets = spot_exchange.load_markets()
            spot_symbols_set = set(spot_markets.keys())
        except: spot_symbols_set = set()
        
        interval_map = {}
        try:
            special_info = exchange.fapiPublicGetFundingInfo()
            for item in special_info: interval_map[item['symbol']] = int(item['fundingIntervalHours'])
        except: pass 
        funding_map = {k: safe_float(v['info'].get('lastFundingRate')) for k, v in funding_rates.items()}
        
        target_symbols = []
        for s, t in tickers.items():
            market_info = markets.get(s)
            if not market_info or not market_info.get('active'): continue
            if 'info' in market_info and market_info['info'].get('status') != 'TRADING': continue
            if not s.endswith(f':{TARGET_QUOTE}'): continue
            if safe_float(t.get('quoteVolume')) < MIN_VOLUME_USDT: continue
            if market_info['base'] in EXCLUDE_SYMBOLS: continue
            target_symbols.append(s)

        print(f"🎯 目标监控币种数: {len(target_symbols)}")
        data_list = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(process_single_symbol, s, exchange, tickers[s], interval_map, funding_map, cached_cmc_data, spot_symbols_set): s for s in target_symbols}
            for future in as_completed(futures):
                res = future.result()
                if res: data_list.append(res)
        return pd.DataFrame(data_list)
    except Exception as e:
        print(f"Error: {e}")
        return pd.DataFrame()

def save_to_postgres(df):
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
            oi_change_pct = 0.0 if prev_oi <= 0 else oi_change_val / prev_oi

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
                has_spot=row['has_spot']
            )
            data_objects.append(item)
        db.add_all(data_objects)
        db.commit()
        print(f"✅ [{current_time.strftime('%H:%M:%S')}] 存入 {len(data_objects)} 条数据")
    except Exception as e:
        db.rollback()
    finally:
        db.close()

def run_collector():
    print("🚀 采集器后台进程已启动...")
    cmc_cache = {}
    last_cmc_update = 0
    # ⚠️ 测试完记得改回 14400！你的额度只剩 70 多了！
    CMC_UPDATE_INTERVAL = 14400

    while True:
        try:
            current_time = time.time()
            if current_time - last_cmc_update > CMC_UPDATE_INTERVAL:
                print("⏰ 需要更新 CMC 供应量数据...")
                
                # 🔥 [核心修改] 这里加上 options，指定连接合约市场
                tmp_exchange = ccxt.binance({'options': {'defaultType': 'future'}}) 
                
                tickers = tmp_exchange.fetch_tickers()
                target_for_cmc = []
                for s, t in tickers.items():
                    if not s.endswith(f':{TARGET_QUOTE}'): continue
                    if safe_float(t.get('quoteVolume')) < MIN_VOLUME_USDT: continue
                    target_for_cmc.append(s)
                
                print(f"🔍 筛选出 {len(target_for_cmc)} 个目标币种，准备请求 CMC...")
                
                # 获取数据
                if target_for_cmc:
                    cmc_cache = fetch_cmc_data(target_for_cmc)
                    if cmc_cache:
                        last_cmc_update = current_time
                        print(f"✅ CMC 供应量缓存已更新 (包含 {len(cmc_cache)} 个币种)")
                    else:
                        print("⚠️ CMC 返回空数据")
                else:
                    print("⚠️ 未找到符合条件的币种 (target_for_cmc 为空)")
            else:
                print(f"♻️ 复用 CMC 供应量缓存")

            df = get_market_data(cmc_cache)
            if not df.empty: save_to_postgres(df)
            else: print("⚠️ 本轮未获取到数据")
        except Exception as e: print(f"❌ 异常: {e}")
        
        print("💤 休眠 300 秒...")
        time.sleep(300)

if __name__ == "__main__":
    run_collector()