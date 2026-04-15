# -*- coding: utf-8 -*-
"""
验证样本选择器
实现时间分散抽样，确保样本独立性
"""
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import SAMPLE_INDEPENDENCE_CONFIG
from utils.logger import get_logger

logger = get_logger("SampleSelector")


def ensure_date(d):
    """确保返回date对象"""
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(d, fmt).date()
            except:
                pass
    return None


@dataclass
class TimeSlot:
    """时间段"""
    start_date: date
    end_date: date
    index: int
    
    def contains(self, d: date) -> bool:
        """检查日期是否在时间段内"""
        return self.start_date <= d <= self.end_date
    
    def __repr__(self):
        return f"TimeSlot({self.start_date} ~ {self.end_date})"


@dataclass
class MarketEnvironment:
    """市场环境"""
    name: str                              # 环境名称
    date_range: Tuple[date, date]          # 对应日期范围
    description: str = ""                   # 描述
    
    def contains(self, d: date) -> bool:
        return self.date_range[0] <= d <= self.date_range[1]


@dataclass
class SampleSelectionResult:
    """样本选择结果"""
    selected_stocks: List[Dict]             # 选中的股票
    selected_dates: List[date]              # 选中的日期
    time_slots: List[TimeSlot]              # 时间段
    market_coverage: Dict[str, int]         # 市场环境覆盖
    exclusion_info: Dict                   # 排除信息
    
    def summary(self) -> Dict:
        return {
            'selected_count': len(self.selected_stocks),
            'time_slots_count': len(self.time_slots),
            'market_coverage': self.market_coverage,
            'exclusion_count': sum(self.exclusion_info.values()),
        }


class SampleSelector:
    """样本选择器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化选择器
        
        Args:
            config: 配置字典
        """
        self.config = config or SAMPLE_INDEPENDENCE_CONFIG
        self.min_days_gap = self.config.get('min_days_gap', 5)
        self.max_samples_per_stock = self.config.get('max_samples_per_stock', 3)
        self.time_disperse_ratio = self.config.get('time_disperse_ratio', 0.3)
        self.market_regime_coverage = self.config.get('market_regime_coverage', True)
        
        # 记录已选择的股票和日期
        self.selected_stocks_dates: Dict[str, List[date]] = {}
    
    def select_samples(
        self,
        stock_pool: List[Dict],
        date_range: Tuple[date, date],
        sample_size: int,
        existing_selections: Dict[str, List[date]] = None
    ) -> SampleSelectionResult:
        """
        选择样本，确保时间分散
        
        Args:
            stock_pool: 股票池
            date_range: 日期范围
            sample_size: 目标样本数
            existing_selections: 已有的选择记录
        
        Returns:
            选择结果
        """
        if existing_selections:
            self.selected_stocks_dates = existing_selections.copy()
        
        # 1. 划分时间段
        time_slots = self._create_time_slots(date_range, sample_size)
        
        # 2. 确保市场环境覆盖
        market_coverage = self._ensure_market_coverage(time_slots)
        
        # 3. 时间分散选择
        selected_stocks = []
        selected_dates = []
        
        # 每个时间段选择样本
        samples_per_slot = max(1, int(sample_size * self.time_disperse_ratio))
        
        for slot in time_slots:
            slot_stocks, slot_dates = self._select_from_slot(
                stock_pool, slot, samples_per_slot
            )
            selected_stocks.extend(slot_stocks)
            selected_dates.extend(slot_dates)
        
        # 4. 补充随机选择（如果不足）
        remaining = sample_size - len(selected_stocks)
        if remaining > 0:
            extra_stocks, extra_dates = self._select_random(
                stock_pool, date_range, remaining
            )
            selected_stocks.extend(extra_stocks)
            selected_dates.extend(extra_dates)
        
        # 5. 去重
        selected_stocks, selected_dates = self._deduplicate(
            selected_stocks, selected_dates
        )
        
        result = SampleSelectionResult(
            selected_stocks=selected_stocks[:sample_size],
            selected_dates=selected_dates[:sample_size],
            time_slots=time_slots,
            market_coverage=market_coverage,
            exclusion_info=self._get_exclusion_info(stock_pool, selected_stocks),
        )
        
        return result
    
    def _create_time_slots(
        self,
        date_range: Tuple[date, date],
        sample_size: int
    ) -> List[TimeSlot]:
        """创建时间段"""
        start_date, end_date = date_range
        total_days = (end_date - start_date).days
        
        # 根据样本量决定时间段数量
        num_slots = min(sample_size, max(5, total_days // 30))  # 至少5个时段，每段约30天
        slot_days = total_days // num_slots
        
        time_slots = []
        current_date = start_date
        
        for i in range(num_slots):
            slot_end = current_date + timedelta(days=slot_days - 1)
            if i == num_slots - 1:
                slot_end = end_date  # 最后一个时段包含剩余所有天
            
            time_slots.append(TimeSlot(
                start_date=current_date,
                end_date=slot_end,
                index=i,
            ))
            current_date = slot_end + timedelta(days=1)
        
        logger.info(f"创建{len(time_slots)}个时间段")
        return time_slots
    
    def _ensure_market_coverage(
        self,
        time_slots: List[TimeSlot]
    ) -> Dict[str, int]:
        """确保市场环境覆盖"""
        # 定义主要市场环境时期
        market_environments = [
            MarketEnvironment(
                name='牛市',
                date_range=(date(2014, 7, 1), date(2015, 6, 15)),
                description='2014-2015牛市',
            ),
            MarketEnvironment(
                name='熊市',
                date_range=(date(2015, 6, 15), date(2016, 2, 29)),
                description='2015股灾',
            ),
            MarketEnvironment(
                name='震荡市',
                date_range=(date(2016, 3, 1), date(2018, 1, 31)),
                description='2016-2018震荡上行',
            ),
            MarketEnvironment(
                name='熊市',
                date_range=(date(2018, 2, 1), date(2019, 1, 31)),
                description='2018熊市',
            ),
            MarketEnvironment(
                name='牛市',
                date_range=(date(2019, 2, 1), date(2021, 2, 28)),
                description='2019-2021牛市',
            ),
            MarketEnvironment(
                name='熊市',
                date_range=(date(2022, 1, 1), date(2022, 10, 31)),
                description='2022熊市',
            ),
            MarketEnvironment(
                name='震荡市',
                date_range=(date(2022, 11, 1), date(2024, 2, 29)),
                description='2022-2024震荡',
            ),
            MarketEnvironment(
                name='牛市',
                date_range=(date(2024, 3, 1), date(2024, 12, 31)),
                description='2024牛市',
            ),
        ]
        
        # 统计各市场环境的覆盖情况
        coverage = {env.name: 0 for env in market_environments}
        for slot in time_slots:
            slot_mid = slot.start_date + timedelta(days=(slot.end_date - slot.start_date).days // 2)
            for env in market_environments:
                if env.contains(slot_mid):
                    coverage[env.name] += 1
                    break
        
        return coverage
    
    def _select_from_slot(
        self,
        stock_pool: List[Dict],
        slot: TimeSlot,
        max_samples: int
    ) -> Tuple[List[Dict], List[date]]:
        """从时间段中选择样本"""
        # 获取可交易的股票
        tradeable_stocks = self._get_tradeable_stocks(stock_pool)
        
        if not tradeable_stocks:
            return [], []
        
        selected_stocks = []
        selected_dates = []
        
        # 随机选择股票
        stocks_to_select = min(max_samples, len(tradeable_stocks))
        candidates = random.sample(tradeable_stocks, stocks_to_select)
        
        for stock in candidates:
            stock_code = stock['code']
            
            # 检查该股票是否可以选择
            if self._can_select_stock(stock_code):
                # 生成随机日期
                available_days = (slot.end_date - slot.start_date).days + 1
                random_offset = random.randint(0, max(0, available_days - 1))
                selected_date = slot.start_date + timedelta(days=random_offset)
                
                # 检查日期是否可用
                if self._can_select_date(stock_code, selected_date):
                    selected_stocks.append(stock)
                    selected_dates.append(selected_date)
                    self._record_selection(stock_code, selected_date)
        
        return selected_stocks, selected_dates
    
    def _select_random(
        self,
        stock_pool: List[Dict],
        date_range: Tuple[date, date],
        count: int
    ) -> Tuple[List[Dict], List[date]]:
        """随机选择样本"""
        selected_stocks = []
        selected_dates = []
        
        tradeable_stocks = self._get_tradeable_stocks(stock_pool)
        
        for _ in range(count * 2):  # 最多尝试2倍次数
            if len(selected_stocks) >= count:
                break
            
            stock = random.choice(tradeable_stocks)
            stock_code = stock['code']
            
            # 随机生成日期
            start_date, end_date = date_range
            available_days = (end_date - start_date).days + 1
            random_offset = random.randint(0, max(0, available_days - 1))
            selected_date = start_date + timedelta(days=random_offset)
            
            # 检查是否可以选择
            if (self._can_select_stock(stock_code) and 
                self._can_select_date(stock_code, selected_date)):
                selected_stocks.append(stock)
                selected_dates.append(selected_date)
                self._record_selection(stock_code, selected_date)
        
        return selected_stocks, selected_dates
    
    def _get_tradeable_stocks(self, stock_pool: List[Dict]) -> List[Dict]:
        """获取可交易的股票"""
        # 不再检查market_cap，直接返回所有股票
        return stock_pool
    
    def _can_select_stock(self, stock_code: str) -> bool:
        """检查股票是否可以选择"""
        if stock_code not in self.selected_stocks_dates:
            return True
        return len(self.selected_stocks_dates[stock_code]) < self.max_samples_per_stock
    
    def _can_select_date(self, stock_code: str, selected_date: date) -> bool:
        """检查日期是否可以选择"""
        # 确保日期类型一致
        selected_date = ensure_date(selected_date)
        
        if stock_code not in self.selected_stocks_dates:
            return True
        
        existing_dates = self.selected_stocks_dates[stock_code]
        for existing_date in existing_dates:
            # 确保现有日期也是date对象
            existing_date = ensure_date(existing_date)
            if selected_date and existing_date:
                if abs((selected_date - existing_date).days) < self.min_days_gap:
                    return False
        return True
    
    def _record_selection(self, stock_code: str, selected_date: date):
        """记录选择"""
        if stock_code not in self.selected_stocks_dates:
            self.selected_stocks_dates[stock_code] = []
        self.selected_stocks_dates[stock_code].append(selected_date)
    
    def _deduplicate(
        self,
        stocks: List[Dict],
        dates: List[date]
    ) -> Tuple[List[Dict], List[date]]:
        """去重"""
        seen = set()
        unique_stocks = []
        unique_dates = []
        
        for stock, stock_date in zip(stocks, dates):
            key = (stock['code'], stock_date)
            if key not in seen:
                seen.add(key)
                unique_stocks.append(stock)
                unique_dates.append(stock_date)
        
        return unique_stocks, unique_dates
    
    def _get_exclusion_info(
        self,
        stock_pool: List[Dict],
        selected_stocks: List[Dict]
    ) -> Dict:
        """获取排除信息"""
        selected_codes = set(s['code'] for s in selected_stocks)
        total_count = len(stock_pool)
        selected_count = len(selected_stocks)
        
        return {
            'total_stocks': total_count,
            'selected_stocks': selected_count,
            'excluded_stocks': total_count - selected_count,
            'exclusion_reasons': {
                'market_cap_filter': total_count - selected_count,
            }
        }
    
    def reset(self):
        """重置选择器状态"""
        self.selected_stocks_dates = {}


def disperse_sampling(
    stock_pool: List[Dict],
    date_range: Tuple[date, date],
    sample_size: int = 50,
    config: Dict = None
) -> SampleSelectionResult:
    """
    时间分散抽样的便捷函数
    
    Args:
        stock_pool: 股票池
        date_range: 日期范围
        sample_size: 样本数量
        config: 配置字典
    
    Returns:
        选择结果
    """
    selector = SampleSelector(config)
    return selector.select_samples(stock_pool, date_range, sample_size)
