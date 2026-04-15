# -*- coding: utf-8 -*-
"""
数据获取模块
"""
import akshare as ak
import pandas as pd
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import time
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DATA_SOURCE_CONFIG, VALIDATION_CONFIG
from utils.logger import get_logger
from utils.cache import get_stock_cache, get_cache_manager
from utils.helpers import safe_float, safe_int, filter_valid_stocks, generate_id
from models.sample import StockInfo, PriceData, MarketSnapshot, PriceDataStatus

logger = get_logger("DataFetcher")


class DataFetcher:
    """数据获取器"""
    
    def __init__(self, use_cache: bool = True):
        self.use_cache = use_cache and DATA_SOURCE_CONFIG['cache']['enabled']
        self.retry_times = DATA_SOURCE_CONFIG['akshare']['retry_times']
        self.retry_delay = DATA_SOURCE_CONFIG['akshare']['retry_delay']
        
        if self.use_cache:
            self.cache = get_stock_cache()
        else:
            self.cache = None
    
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
        
        try:
            # 获取沪深股票列表
            df = self._retry_request(ak.stock_info_a_code_name)
            
            stock_list = []
            for _, row in df.iterrows():
                code = str(row['code']).zfill(6)
                name = str(row['name'])
                
                stock_list.append({
                    'code': code,
                    'name': name,
                    'market': 'shanghai' if code.startswith('6') else 'shenzhen',
                })
            
            # 过滤有效股票
            stock_list = filter_valid_stocks(stock_list)
            
            # 更新缓存
            if self.cache:
                self.cache.set_stock_list(market, stock_list)
            
            logger.info(f"获取股票列表成功: 共 {len(stock_list)} 只")
            return stock_list
            
        except Exception as e:
            logger.error(f"获取股票列表失败: {e}")
            return []
    
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
            # 获取股票信息
            df = self._retry_request(ak.stock_individual_info_em, symbol=stock_code)
            
            info_dict = {}
            for _, row in df.iterrows():
                info_dict[row['item']] = row['value']
            
            stock_info = StockInfo(
                code=stock_code,
                name=info_dict.get('股票简称', ''),
                market='A',
                sector=info_dict.get('行业', ''),
                market_cap=safe_float(info_dict.get('总市值', 0)),
                float_cap=safe_float(info_dict.get('流通市值', 0)),
                list_date=self._parse_date(info_dict.get('上市时间', '')),
                is_main_board=stock_code.startswith(('6', '0')),
            )
            
            # 更新缓存
            if self.cache:
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
            adjust: 复权类型 (qfq/hfq/none)
        
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
            # 获取历史数据
            df = self._retry_request(
                ak.stock_zh_a_hist,
                symbol=stock_code,
                period="daily",
                start_date=(datetime.now() - timedelta(days=days*2)).strftime("%Y%m%d"),
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust=adjust
            )
            
            prices = []
            for _, row in df.iterrows():
                price = PriceData(
                    date=row['日期'],
                    open=safe_float(row['开盘']),
                    high=safe_float(row['最高']),
                    low=safe_float(row['最低']),
                    close=safe_float(row['收盘']),
                    volume=safe_float(row['成交量']),
                    amount=safe_float(row['成交额']),
                    turnover_rate=safe_float(row['换手率']),
                )
                
                # 判断状态
                if row.get('涨跌额', 0) == 0 and row.get('涨跌幅', 0) == 0:
                    price.status = PriceDataStatus.SUSPENDED
                elif safe_float(row.get('涨跌幅', 0)) >= 9.9:
                    price.status = PriceDataStatus.LIMIT_UP
                elif safe_float(row.get('涨跌幅', 0)) <= -9.9:
                    price.status = PriceDataStatus.LIMIT_DOWN
                
                prices.append(price)
            
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
    
    def get_money_flow(
        self,
        stock_code: str,
        date_str: Optional[str] = None
    ) -> Optional[Dict]:
        """
        获取资金流向
        
        Args:
            stock_code: 股票代码
            date_str: 日期 (YYYYMMDD)
        
        Returns:
            资金流向数据
        """
        try:
            if date_str is None:
                date_str = datetime.now().strftime("%Y%m%d")
            
            df = self._retry_request(
                ak.stock_individual_fund_flow,
                symbol=stock_code,
                market="sh"
            )
            
            if df is None or df.empty:
                return None
            
            # 获取最新数据
            latest = df.iloc[-1]
            
            return {
                'main_net_inflow': safe_float(latest.get('主力净流入净额', 0)),
                'main_net_inflow_rate': safe_float(latest.get('主力净流入净占比', 0)),
                'super_net_inflow': safe_float(latest.get('超大单净流入净额', 0)),
                'big_net_inflow': safe_float(latest.get('大单净流入净额', 0)),
                'mid_net_inflow': safe_float(latest.get('中单净流入净额', 0)),
                'small_net_inflow': safe_float(latest.get('小单净流入净额', 0)),
            }
            
        except Exception as e:
            logger.error(f"获取资金流向失败 ({stock_code}): {e}")
            return None
    
    def get_market_index(
        self,
        index_code: str = "000001",
        days: int = 60
    ) -> List[Dict]:
        """
        获取指数数据
        
        Args:
            index_code: 指数代码 (000001=上证指数, 399001=深证成指)
            days: 天数
        
        Returns:
            指数数据列表
        """
        try:
            df = self._retry_request(
                ak.stock_zh_index_daily,
                symbol=("sh" + index_code if index_code.startswith('0') else "sz" + index_code)
            )
            
            # 获取近期数据
            df = df.tail(days)
            
            result = []
            for _, row in df.iterrows():
                result.append({
                    'date': row['date'],
                    'open': safe_float(row['open']),
                    'high': safe_float(row['high']),
                    'low': safe_float(row['low']),
                    'close': safe_float(row['close']),
                    'volume': safe_float(row.get('volume', 0)),
                })
            
            return result
            
        except Exception as e:
            logger.error(f"获取指数数据失败 ({index_code}): {e}")
            return []
    
    def get_market_snapshot(self, trade_date: date) -> Optional[MarketSnapshot]:
        """
        获取市场快照
        
        Args:
            trade_date: 交易日期
        
        Returns:
            市场快照
        """
        date_str = trade_date.strftime("%Y%m%d")
        
        # 检查缓存
        if self.cache:
            cached = self.cache.get_market_snapshot(date_str)
            if cached:
                return self._dict_to_market_snapshot(cached)
        
        try:
            # 获取涨跌停数量
            limit_up_df = self._retry_request(ak.stock_zt_pool_em, date=date_str)
            limit_down_df = self._retry_request(ak.stock_zt_pool_strong_em, date=date_str)
            
            snapshot = MarketSnapshot(
                date=trade_date,
                index_code="000001",
                index_name="上证指数",
                close=0,
                limit_up_count=len(limit_up_df) if limit_up_df is not None else 0,
                limit_down_count=len(limit_down_df) if limit_down_df is not None else 0,
            )
            
            # 获取指数点位
            index_data = self.get_market_index("000001", days=5)
            if index_data:
                latest = index_data[-1]
                snapshot.close = latest['close']
                snapshot.change_percent = safe_float(
                    (latest['close'] - index_data[-2]['close']) / index_data[-2]['close']
                    if len(index_data) > 1 else 0
                )
            
            # 更新缓存
            if self.cache:
                self.cache.set_market_snapshot(date_str, {
                    'date': str(snapshot.date),
                    'index_code': snapshot.index_code,
                    'index_name': snapshot.index_name,
                    'close': snapshot.close,
                    'change_percent': snapshot.change_percent,
                    'limit_up_count': snapshot.limit_up_count,
                    'limit_down_count': snapshot.limit_down_count,
                })
            
            return snapshot
            
        except Exception as e:
            logger.error(f"获取市场快照失败 ({trade_date}): {e}")
            return None
    
    def get_market_turnover(self, days: int = 5) -> float:
        """
        获取市场成交额
        
        Args:
            days: 天数
        
        Returns:
            平均成交额（亿元）
        """
        try:
            # 获取上证和深证成交额
            sh_df = self._retry_request(
                ak.stock_sh_a_spot_em,
            )
            
            if sh_df is not None and not sh_df.empty:
                total_amount = safe_float(sh_df['成交额'].sum()) / 100000000  # 转换为亿
                return total_amount
            
            return 0
            
        except Exception as e:
            logger.error(f"获取市场成交额失败: {e}")
            return 0
    
    def calculate_indicators(self, prices: List[PriceData]) -> List[PriceData]:
        """
        计算技术指标
        
        Args:
            prices: 价格数据列表
        
        Returns:
            带技术指标的价格数据
        """
        if not prices:
            return prices
        
        closes = [p.close for p in prices]
        volumes = [p.volume for p in prices]
        highs = [p.high for p in prices]
        lows = [p.low for p in prices]
        
        n = len(prices)
        
        # 计算均线
        for i, price in enumerate(prices):
            if i >= 4:
                price.ma5 = sum(closes[i-4:i+1]) / 5
            if i >= 9:
                price.ma10 = sum(closes[i-9:i+1]) / 10
            if i >= 19:
                price.ma20 = sum(closes[i-19:i+1]) / 20
            if i >= 29:
                price.ma30 = sum(closes[i-29:i+1]) / 30
        
        # 计算MACD (12, 26, 9)
        if n >= 34:
            ema12 = self._calculate_ema(closes, 12)
            ema26 = self._calculate_ema(closes, 26)
            
            for i in range(n):
                if i < 33:
                    continue
                diff = ema12[i] - ema26[i]
                if i == 33:
                    dea = diff
                else:
                    dea = 2/10 * diff + 8/10 * dea
                
                price = prices[i]
                price.macd = diff
                price.macd_signal = dea
                price.macd_hist = 2 * (diff - dea)
        
        # 计算KDJ (9, 3, 3)
        if n >= 9:
            for i in range(8, n):
                recent_lows = lows[i-8:i+1]
                recent_highs = highs[i-8:i+1]
                
                lowest = min(recent_lows)
                highest = max(recent_highs)
                
                close = closes[i]
                
                if highest == lowest:
                    rsv = 50
                else:
                    rsv = (close - lowest) / (highest - lowest) * 100
                
                if i == 8:
                    k = 50
                    d = 50
                else:
                    k = 2/3 * prices[i-1].kdj_k + 1/3 * rsv
                    d = 2/3 * prices[i-1].kdj_d + 1/3 * k
                
                j = 3 * k - 2 * d
                
                price = prices[i]
                price.kdj_k = k
                price.kdj_d = d
                price.kdj_j = j
        
        # 计算RSI (14)
        if n >= 15:
            for i in range(14, n):
                gains = []
                losses = []
                
                for j in range(i-13, i+1):
                    if j == 0:
                        continue
                    change = closes[j] - closes[j-1]
                    if change > 0:
                        gains.append(change)
                        losses.append(0)
                    else:
                        gains.append(0)
                        losses.append(abs(change))
                
                avg_gain = sum(gains) / 14
                avg_loss = sum(losses) / 14
                
                if avg_loss == 0:
                    rsi = 100
                else:
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))
                
                prices[i].rsi = rsi
        
        return prices
    
    def _calculate_ema(self, values: List[float], period: int) -> List[float]:
        """计算指数移动平均"""
        ema = []
        multiplier = 2 / (period + 1)
        
        for i, value in enumerate(values):
            if i == 0:
                ema.append(value)
            elif i < period:
                ema.append((value + i * ema[-1]) / (i + 1))
            else:
                ema.append(value * multiplier + ema[-1] * (1 - multiplier))
        
        return ema
    
    def _parse_date(self, date_str: str) -> Optional[date]:
        """解析日期"""
        if not date_str:
            return None
        
        try:
            # 尝试多种格式
            for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(date_str, fmt).date()
                except ValueError:
                    continue
            return None
        except Exception:
            return None
    
    def _dict_to_stock_info(self, d: Dict) -> StockInfo:
        """字典转StockInfo"""
        return StockInfo(
            code=d.get('code', ''),
            name=d.get('name', ''),
            market=d.get('market', 'A'),
            sector=d.get('sector', ''),
            market_cap=d.get('market_cap', 0),
            float_cap=d.get('float_cap', 0),
            list_date=self._parse_date(d.get('list_date', '')) if d.get('list_date') else None,
            is_main_board=d.get('is_main_board', True),
        )
    
    def _dict_to_price_data(self, d: Dict) -> PriceData:
        """字典转PriceData"""
        return PriceData(
            date=self._parse_date(d.get('date', '')) or date.today(),
            open=d.get('open', 0),
            high=d.get('high', 0),
            low=d.get('low', 0),
            close=d.get('close', 0),
            volume=d.get('volume', 0),
            amount=d.get('amount', 0),
            turnover_rate=d.get('turnover_rate', 0),
            status=PriceDataStatus(d.get('status', 'normal')),
        )
    
    def _dict_to_market_snapshot(self, d: Dict) -> MarketSnapshot:
        """字典转MarketSnapshot"""
        return MarketSnapshot(
            date=self._parse_date(d.get('date', '')) or date.today(),
            index_code=d.get('index_code', ''),
            index_name=d.get('index_name', ''),
            close=d.get('close', 0),
            change_percent=d.get('change_percent', 0),
            limit_up_count=d.get('limit_up_count', 0),
            limit_down_count=d.get('limit_down_count', 0),
        )
