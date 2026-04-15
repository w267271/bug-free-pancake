# -*- coding: utf-8 -*-
"""
数据获取模块 - TickFlow版本（高速批量下载）
"""
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import time
import sys
from pathlib import Path
import threading
import os

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DATA_SOURCE_CONFIG, VALIDATION_CONFIG
from utils.logger import get_logger
from utils.cache import get_stock_cache, get_cache_manager
from utils.helpers import safe_float, safe_int, filter_valid_stocks, generate_id
from models.sample import StockInfo, PriceData, MarketSnapshot, PriceDataStatus

logger = get_logger("DataFetcher")

# TickFlow全局实例
_tf_instance = None
_tf_lock = threading.Lock()


def get_tickflow_instance():
    """获取TickFlow单例"""
    global _tf_instance
    if _tf_instance is None:
        with _tf_lock:
            if _tf_instance is None:
                from tickflow import TickFlow
                _tf_instance = TickFlow.free()
                logger.info("TickFlow初始化成功（免费模式）")
    return _tf_instance


class DataFetcher:
    """数据获取器 - TickFlow版（批量高速）"""
    
    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache and DATA_SOURCE_CONFIG['cache']['enabled']
        self.retry_times = 3
        self.retry_delay = 2
        self._stock_list_cache = None
        
        # 获取TickFlow实例
        self.tf = get_tickflow_instance()
        
        if self.use_cache:
            self.cache = get_stock_cache()
        else:
            self.cache = None
        
        # 预定义主要股票列表（沪深主板+创业板+科创板）
        self._predefined_stocks = self._init_predefined_stocks()
    
    def _init_predefined_stocks(self) -> List[Dict]:
        """初始化预定义股票列表"""
        stocks = []
        # 上海主板 (600xxx, 601xxx, 603xxx)
        for code in range(600000, 600100):
            stocks.append({'code': str(code), 'market': 'shanghai', 'type': 'main'})
        for code in range(601000, 602000):
            stocks.append({'code': str(code), 'market': 'shanghai', 'type': 'main'})
        for code in range(603000, 604000):
            stocks.append({'code': str(code), 'market': 'shanghai', 'type': 'main'})
        # 深圳主板 (000xxx)
        for code in range(1, 1000):
            stocks.append({'code': f'{code:06d}', 'market': 'shenzhen', 'type': 'main'})
        # 创业板 (300xxx)
        for code in range(300001, 301000):
            stocks.append({'code': str(code), 'market': 'chinese_national', 'type': 'chinext'})
        # 科创板 (688xxx)
        for code in range(688000, 689000):
            stocks.append({'code': str(code), 'market': 'star', 'type': 'star'})
        return stocks
    
    def _retry_request(self, func, *args, **kwargs):
        """带重试的请求"""
        last_error = None
        for i in range(self.retry_times):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                last_error = e
                logger.warning(f"请求失败 (尝试 {i+1}/{self.retry_times}): {e}")
                if i < self.retry_times - 1:
                    time.sleep(self.retry_delay)
        
        logger.error(f"请求最终失败: {last_error}")
        raise last_error
    
    def _to_tf_code(self, stock_code: str) -> str:
        """将股票代码转换为TickFlow格式"""
        if stock_code.endswith('.SH') or stock_code.endswith('.SZ'):
            return stock_code
        if stock_code.startswith('6'):
            return f"{stock_code}.SH"
        else:
            return f"{stock_code}.SZ"
    
    def _to_simple_code(self, tf_code: str) -> str:
        """将TickFlow代码转换为简单格式"""
        return tf_code.replace('.SH', '').replace('.SZ', '')
    
    def get_stock_list(self, market: str = "all") -> List[Dict]:
        """
        获取股票列表
        
        Args:
            market: 市场类型 (all/shanghai/shenzhen)
        
        Returns:
            股票列表
        """
        # 检查缓存
        if self.cache:
            cached = self.cache.get_stock_list(market)
            if cached:
                logger.info(f"从缓存获取股票列表: {market}")
                return cached
        
        # 使用预定义列表 + TickFlow验证
        try:
            stock_list = []
            total = len(self._predefined_stocks)
            
            # 批量验证股票（每批100只）
            batch_size = 100
            validated_count = 0
            
            for i in range(0, total, batch_size):
                batch = self._predefined_stocks[i:i+batch_size]
                symbols = [self._to_tf_code(s['code']) for s in batch]
                
                # 尝试批量获取K线来验证股票是否存在
                try:
                    klines = self.tf.klines.batch(symbols, period="1d", count=1, show_progress=False)
                    for sym, df in klines.items():
                        if df is not None and len(df) > 0:
                            code = self._to_simple_code(sym)
                            stock_info = batch.find(lambda x: self._to_tf_code(x['code']) == sym)
                            if stock_info:
                                market_type = stock_info.get('market', 'shanghai')
                                if market in ['all', market_type]:
                                    stock_list.append({
                                        'code': code,
                                        'name': df.iloc[0].get('name', ''),
                                        'market': market_type,
                                    })
                                    validated_count += 1
                except Exception as e:
                    logger.debug(f"批量验证失败: {e}")
            
            # 过滤有效股票
            stock_list = filter_valid_stocks(stock_list)
            
            # 更新缓存
            if self.cache:
                self.cache.set_stock_list(market, stock_list)
            
            logger.info(f"获取股票列表成功: 共 {len(stock_list)} 只（验证了 {validated_count} 只）")
            self._stock_list_cache = stock_list
            return stock_list
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            # 返回预定义列表作为后备
            fallback = [s for s in self._predefined_stocks if 
                       market == 'all' or s.get('market') == market]
            return [{'code': s['code'], 'name': '', 'market': s.get('market', 'shanghai')} 
                   for s in fallback[:1000]]
    
    def get_stock_info(self, stock_code: str) -> Optional[StockInfo]:
        """
        获取股票基本信息
        
        Args:
            stock_code: 股票代码
        
        Returns:
            股票信息
        """
        # 检查缓存
        if self.cache:
            cached = self.cache.get_stock_info(stock_code)
            if cached:
                return self._dict_to_stock_info(cached)
        
        try:
            tf_code = self._to_tf_code(stock_code)
            
            # 获取基本信息
            info = self.tf.instruments.get(tf_code)
            
            if not info:
                logger.error(f"获取股票信息失败 ({stock_code}): 无数据")
                return None
            
            ext = info.get('ext', {})
            stock_info = StockInfo(
                code=stock_code,
                name=info.get('name', ''),
                market='A',
                sector='',
                market_cap=ext.get('total_shares', 0),
                float_cap=ext.get('float_shares', 0),
                list_date=self._parse_date(ext.get('listing_date')),
                is_main_board=stock_code.startswith(('6', '0')),
            )
            
            # 更新缓存
            if stock_info and self.cache:
                self.cache.set_stock_info(stock_code, {
                    'code': stock_info.code,
                    'name': stock_info.name,
                    'market': stock_info.market,
                    'sector': stock_info.sector,
                    'market_cap': stock_info.market_cap,
                    'float_cap': stock_info.float_cap,
                    'list_date': str(stock_info.list_date) if stock_info.list_date else None,
                    'is_main_board': stock_info.is_main_board,
                })
            
            return stock_info
            
        except Exception as e:
            logger.error(f"获取股票信息失败 ({stock_code}): {e}")
            return None
    
    def get_price_history(
        self,
        stock_code: str,
        days: int = 250,
        adjust: str = "qfq"
    ) -> List[PriceData]:
        """
        获取价格历史
        
        Args:
            stock_code: 股票代码
            days: 天数
            adjust: 复权类型 (qfq/hfq/none) - TickFlow暂不支持，固定返回不复权数据
        
        Returns:
            价格数据列表
        """
        # 检查缓存
        if self.cache:
            cached = self.cache.get_price_history(stock_code, days)
            if cached:
                logger.debug(f"从缓存获取价格历史: {stock_code}")
                return [self._dict_to_price_data(p) for p in cached]
        
        try:
            tf_code = self._to_tf_code(stock_code)
            
            # TickFlow获取K线
            df = self.tf.klines.get(tf_code, period="1d", count=days, as_dataframe=True)
            
            if df is None or len(df) == 0:
                logger.warning(f"获取价格历史失败 ({stock_code}): 无数据")
                return []
            
            prices = []
            for _, row in df.iterrows():
                # 处理日期字段
                trade_date = row.get('trade_date')
                if pd.isna(trade_date):
                    # 使用timestamp转换
                    ts = row.get('timestamp', 0)
                    if ts:
                        trade_date = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')
                    else:
                        continue
                
                price = PriceData(
                    date=trade_date,
                    open=safe_float(row.get('open', 0)),
                    high=safe_float(row.get('high', 0)),
                    low=safe_float(row.get('low', 0)),
                    close=safe_float(row.get('close', 0)),
                    volume=safe_float(row.get('volume', 0)),
                    amount=safe_float(row.get('amount', 0)),
                    turnover_rate=0,  # TickFlow K线数据暂无换手率
                )
                prices.append(price)
            
            # 按日期排序
            prices.sort(key=lambda x: str(x.date))
            
            # 更新缓存
            if self.cache and prices:
                self.cache.set_price_history(stock_code, days, [
                    {
                        'date': str(p.date),
                        'open': p.open,
                        'high': p.high,
                        'low': p.low,
                        'close': p.close,
                        'volume': p.volume,
                        'amount': p.amount,
                        'turnover_rate': p.turnover_rate,
                        'status': p.status.value,
                    }
                    for p in prices
                ])
            
            logger.debug(f"获取价格历史成功: {stock_code}, {len(prices)} 条")
            return prices
            
        except Exception as e:
            logger.error(f"获取价格历史失败 ({stock_code}): {e}")
            return []
    
    def get_batch_price_history(
        self,
        stock_codes: List[str],
        days: int = 250,
        adjust: str = "qfq"
    ) -> Dict[str, List[PriceData]]:
        """
        批量获取价格历史（TickFlow核心优势）
        
        Args:
            stock_codes: 股票代码列表
            days: 天数
            adjust: 复权类型
        
        Returns:
            {stock_code: [PriceData]}
        """
        results = {}
        symbols = [self._to_tf_code(code) for code in stock_codes]
        
        try:
            # TickFlow批量获取
            dfs = self.tf.klines.batch(symbols, period="1d", count=days, 
                                       as_dataframe=True, show_progress=False)
            
            for stock_code, df in dfs.items():
                if df is None or len(df) == 0:
                    continue
                    
                prices = []
                for _, row in df.iterrows():
                    trade_date = row.get('trade_date')
                    if pd.isna(trade_date):
                        ts = row.get('timestamp', 0)
                        if ts:
                            trade_date = datetime.fromtimestamp(ts / 1000).strftime('%Y-%m-%d')
                        else:
                            continue
                    
                    price = PriceData(
                        date=trade_date,
                        open=safe_float(row.get('open', 0)),
                        high=safe_float(row.get('high', 0)),
                        low=safe_float(row.get('low', 0)),
                        close=safe_float(row.get('close', 0)),
                        volume=safe_float(row.get('volume', 0)),
                        amount=safe_float(row.get('amount', 0)),
                        turnover_rate=0,
                    )
                    prices.append(price)
                
                if prices:
                    prices.sort(key=lambda x: str(x.date))
                    simple_code = self._to_simple_code(stock_code)
                    results[simple_code] = prices
                    
                    # 更新缓存
                    if self.cache:
                        self.cache.set_price_history(simple_code, days, [
                            {
                                'date': str(p.date),
                                'open': p.open,
                                'high': p.high,
                                'low': p.low,
                                'close': p.close,
                                'volume': p.volume,
                                'amount': p.amount,
                                'turnover_rate': p.turnover_rate,
                                'status': p.status.value,
                            }
                            for p in prices
                        ])
            
            logger.info(f"批量获取价格历史完成: {len(results)}/{len(stock_codes)} 只成功")
            return results
            
        except Exception as e:
            logger.error(f"批量获取价格历史失败: {e}")
            # 降级为逐个获取
            for code in stock_codes:
                try:
                    results[code] = self.get_price_history(code, days, adjust)
                except:
                    pass
            return results
    
    def get_money_flow(
        self,
        stock_code: str,
        date_str: Optional[str] = None
    ) -> Optional[Dict]:
        """
        获取资金流向 - TickFlow暂不支持，返回None
        
        Args:
            stock_code: 股票代码
            date_str: 日期
        
        Returns:
            资金流向数据
        """
        logger.warning(f"TickFlow暂不支持资金流向数据: {stock_code}")
        return None
    
    def get_market_snapshot(self, date_str: Optional[str] = None) -> Optional[MarketSnapshot]:
        """
        获取市场快照
        
        Args:
            date_str: 日期
        
        Returns:
            市场快照
        """
        try:
            # 获取上证指数和深证成指
            indices = {
                'sh': '000001.SH',  # 上证指数
                'sz': '399001.SZ',  # 深证成指
            }
            
            snapshot_data = {}
            for key, symbol in indices.items():
                try:
                    df = self.tf.klines.get(symbol, period="1d", count=1, as_dataframe=True)
                    if df is not None and len(df) > 0:
                        row = df.iloc[0]
                        snapshot_data[key] = {
                            'close': safe_float(row.get('close', 0)),
                            'volume': safe_float(row.get('volume', 0)),
                        }
                except:
                    pass
            
            snapshot = MarketSnapshot(
                date=date_str or datetime.now().strftime("%Y%m%d"),
                index_sh=snapshot_data.get('sh', {}).get('close', 0),
                index_sz=snapshot_data.get('sz', {}).get('close', 0),
                volume_sh=snapshot_data.get('sh', {}).get('volume', 0),
                volume_sz=snapshot_data.get('sz', {}).get('volume', 0),
            )
            
            return snapshot
            
        except Exception as e:
            logger.error(f"获取市场快照失败: {e}")
            return None
    
    def get_index_data(
        self,
        index_code: str,
        start_date: str,
        end_date: str
    ) -> pd.DataFrame:
        """
        获取指数数据
        
        Args:
            index_code: 指数代码
            start_date: 开始日期
            end_date: 结束日期
        
        Returns:
            指数数据DataFrame
        """
        try:
            # 转换指数代码格式
            if index_code.startswith('000'):
                tf_code = f"{index_code}.SH"
            else:
                tf_code = f"{index_code}.SZ"
            
            # 计算需要的天数
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            days = (end_dt - start_dt).days + 30  # 多取一些数据
            
            df = self.tf.klines.get(tf_code, period="1d", count=days, as_dataframe=True)
            
            if df is None or len(df) == 0:
                return pd.DataFrame()
            
            # 过滤日期范围
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df[(df['trade_date'] >= start_date) & (df['trade_date'] <= end_date)]
            
            return df
            
        except Exception as e:
            logger.error(f"获取指数数据失败 ({index_code}): {e}")
            return pd.DataFrame()
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期字符串"""
        if not date_str:
            return None
        try:
            for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(str(date_str), fmt).date()
                except:
                    continue
            return None
        except:
            return None
    
    def _dict_to_stock_info(self, data: Dict) -> StockInfo:
        """字典转StockInfo"""
        return StockInfo(
            code=data['code'],
            name=data['name'],
            market=data['market'],
            sector=data.get('sector', ''),
            market_cap=data.get('market_cap', 0),
            float_cap=data.get('float_cap', 0),
            list_date=self._parse_date(data.get('list_date')),
            is_main_board=data.get('is_main_board', True),
        )
    
    def _dict_to_price_data(self, data: Dict) -> PriceData:
        """字典转PriceData"""
        price = PriceData(
            date=data['date'],
            open=data['open'],
            high=data['high'],
            low=data['low'],
            close=data['close'],
            volume=data['volume'],
            amount=data.get('amount', 0),
            turnover_rate=data.get('turnover_rate', 0),
        )
        price.status = PriceDataStatus(data.get('status', 'normal'))
        return price
    
    def calculate_indicators(self, prices: List[PriceData]) -> List[PriceData]:
        """
        计算技术指标
        
        Args:
            prices: 价格数据列表
        
        Returns:
            带技术指标的价格数据列表
        """
        if not prices:
            return prices
        
        # 转换为DataFrame便于计算
        df = pd.DataFrame([{
            'date': p.date,
            'open': p.open,
            'high': p.high,
            'low': p.low,
            'close': p.close,
            'volume': p.volume,
            'amount': p.amount,
        } for p in prices])
        
        if len(df) == 0:
            return prices
        
        # 计算MA
        df['ma5'] = df['close'].rolling(window=5).mean()
        df['ma10'] = df['close'].rolling(window=10).mean()
        df['ma20'] = df['close'].rolling(window=20).mean()
        
        # 计算MACD
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        
        # 计算RSI
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / (loss + 0.0001)
        df['rsi'] = 100 - (100 / (1 + rs))
        
        # 计算KDJ
        low_min = df['low'].rolling(window=9).min()
        high_max = df['high'].rolling(window=9).max()
        df['k'] = 100 * (df['close'] - low_min) / (high_max - low_min + 0.0001)
        df['d'] = df['k'].rolling(window=3).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']
        
        # 计算BOLL
        df['boll_mid'] = df['close'].rolling(window=20).mean()
        df['boll_std'] = df['close'].rolling(window=20).std()
        df['boll_upper'] = df['boll_mid'] + 2 * df['boll_std']
        df['boll_lower'] = df['boll_mid'] - 2 * df['boll_std']
        
        # 更新prices
        for i, p in enumerate(prices):
            row = df.iloc[i]
            p.indicators = {
                'ma5': safe_float(row.get('ma5', 0)),
                'ma10': safe_float(row.get('ma10', 0)),
                'ma20': safe_float(row.get('ma20', 0)),
                'macd': safe_float(row.get('macd', 0)),
                'macd_signal': safe_float(row.get('macd_signal', 0)),
                'macd_hist': safe_float(row.get('macd_hist', 0)),
                'rsi': safe_float(row.get('rsi', 50)),
                'k': safe_float(row.get('k', 50)),
                'd': safe_float(row.get('d', 50)),
                'j': safe_float(row.get('j', 50)),
                'boll_upper': safe_float(row.get('boll_upper', 0)),
                'boll_mid': safe_float(row.get('boll_mid', 0)),
                'boll_lower': safe_float(row.get('boll_lower', 0)),
            }
        
        return prices


# 模块级函数，方便直接调用
_fetcher_instance = None


def get_data_fetcher(use_cache: bool = True) -> DataFetcher:
    """获取数据获取器单例"""
    global _fetcher_instance
    if _fetcher_instance is None:
        _fetcher_instance = DataFetcher(use_cache)
    return _fetcher_instance
