# -*- coding: utf-8 -*-
"""
特殊行情处理模块
处理停牌、涨停、跌停等情况
"""
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.sample import PriceData, PriceDataStatus, ValidationSample, SampleStatus
from utils.logger import get_logger

logger = get_logger("SpecialCases")


def parse_date(d):
    """解析日期字符串或返回date对象"""
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(d, fmt).date()
            except:
                pass
    return None


class ExcludeReason(Enum):
    """样本排除原因"""
    SUSPENDED = "suspended"                      # 停牌
    LIMIT_UP = "limit_up"                       # 涨停
    LIMIT_DOWN = "limit_down"                   # 跌停
    INSUFFICIENT_DATA = "insufficient_data"     # 数据不足
    ABNORMAL_PRICE = "abnormal_price"           # 价格异常
    NO_TRADE = "no_trade"                      # 无法交易
    EXTREME_VOLATILITY = "extreme_volatility"  # 极端波动
    LOW_LIQUIDITY = "low_liquidity"            # 流动性不足


@dataclass
class SpecialCaseRecord:
    """特殊行情记录"""
    sample_id: str                              # 样本ID
    stock_code: str                             # 股票代码
    trigger_date: date                         # 触发日期
    case_type: str                             # 特殊行情类型
    exclude_reason: str                         # 排除原因
    severity: str = "normal"                   # 严重程度
    details: Dict = field(default_factory=dict)
    
    def to_dict(self) -> Dict:
        return {
            'sample_id': self.sample_id,
            'stock_code': self.stock_code,
            'trigger_date': self.trigger_date.isoformat(),
            'case_type': self.case_type,
            'exclude_reason': self.exclude_reason,
            'severity': self.severity,
            'details': self.details,
        }


@dataclass
class SpecialCaseStats:
    """特殊行情统计"""
    total_samples: int = 0                      # 总样本数
    excluded_count: int = 0                    # 排除样本数
    suspended_count: int = 0                   # 停牌样本数
    limit_up_count: int = 0                    # 涨停样本数
    limit_down_count: int = 0                  # 跌停样本数
    insufficient_data_count: int = 0            # 数据不足样本数
    abnormal_price_count: int = 0              # 价格异常样本数
    no_trade_count: int = 0                    # 无法交易样本数
    extreme_volatility_count: int = 0           # 极端波动样本数
    low_liquidity_count: int = 0               # 流动性不足样本数
    
    def calculate_ratios(self) -> Dict[str, float]:
        """计算各类型占比"""
        if self.total_samples == 0:
            return {}
        
        total = self.total_samples
        return {
            'excluded_ratio': self.excluded_count / total,
            'suspended_ratio': self.suspended_count / total,
            'limit_up_ratio': self.limit_up_count / total,
            'limit_down_ratio': self.limit_down_count / total,
            'insufficient_data_ratio': self.insufficient_data_count / total,
            'abnormal_price_ratio': self.abnormal_price_count / total,
        }
    
    def to_dict(self) -> Dict:
        return {
            'total_samples': self.total_samples,
            'excluded_count': self.excluded_count,
            'exclusion_rate': self.excluded_count / self.total_samples if self.total_samples > 0 else 0,
            'breakdown': {
                'suspended': self.suspended_count,
                'limit_up': self.limit_up_count,
                'limit_down': self.limit_down_count,
                'insufficient_data': self.insufficient_data_count,
                'abnormal_price': self.abnormal_price_count,
                'no_trade': self.no_trade_count,
                'extreme_volatility': self.extreme_volatility_count,
                'low_liquidity': self.low_liquidity_count,
            },
            'ratios': self.calculate_ratios(),
        }


class SpecialCaseHandler:
    """特殊行情处理器"""
    
    # 涨跌停幅度（A股主板）
    LIMIT_UP_RATE = 0.10    # 10%
    LIMIT_DOWN_RATE = -0.10  # -10%
    
    # 科创板/创业板
    GEM_LIMIT_UP_RATE = 0.20  # 20%
    GEM_LIMIT_DOWN_RATE = -0.20  # -20%
    
    def __init__(self, include_special_cases: bool = False):
        """
        初始化处理器
        
        Args:
            include_special_cases: 是否包含特殊行情样本（不排除）
        """
        self.include_special_cases = include_special_cases
    
    def handle_sample(self, sample: ValidationSample) -> Tuple[bool, Optional[SpecialCaseRecord]]:
        """
        处理样本的特殊行情
        
        Args:
            sample: 验证样本
        
        Returns:
            (是否保留样本, 特殊记录)
        """
        # 找到触发日的价格数据
        trigger_price = None
        trigger_date_obj = parse_date(sample.trigger_date)
        for p in sample.prices:
            p_date = parse_date(p.date)
            if p_date and p_date == trigger_date_obj:
                trigger_price = p
                break
        
        if trigger_price is None:
            record = SpecialCaseRecord(
                sample_id=sample.sample_id,
                stock_code=sample.stock_info.code,
                trigger_date=sample.trigger_date,
                case_type='no_data',
                exclude_reason=ExcludeReason.INSUFFICIENT_DATA.value,
                details={'message': '触发日无价格数据'},
            )
            return False, record
        
        # 检查各种特殊行情
        # 1. 停牌检查
        if trigger_price.status == PriceDataStatus.SUSPENDED:
            record = self._create_record(
                sample, 'suspended', ExcludeReason.SUSPENDED,
                {'suspended_since': None}
            )
            return self.include_special_cases, record
        
        # 2. 涨停检查
        if trigger_price.status == PriceDataStatus.LIMIT_UP:
            record = self._create_record(
                sample, 'limit_up', ExcludeReason.LIMIT_UP,
                {'limit_rate': self._get_limit_rate(sample)}
            )
            if not self.include_special_cases:
                return False, record
        
        # 3. 跌停检查
        if trigger_price.status == PriceDataStatus.LIMIT_DOWN:
            record = self._create_record(
                sample, 'limit_down', ExcludeReason.LIMIT_DOWN,
                {'limit_rate': self._get_limit_rate(sample)}
            )
            if not self.include_special_cases:
                return False, record
        
        # 4. 价格异常检查
        if self._is_abnormal_price(trigger_price):
            record = self._create_record(
                sample, 'abnormal_price', ExcludeReason.ABNORMAL_PRICE,
                {'price': trigger_price.close}
            )
            return False, record
        
        # 5. 流动性检查
        if self._is_low_liquidity(trigger_price):
            record = self._create_record(
                sample, 'low_liquidity', ExcludeReason.LOW_LIQUIDITY,
                {'volume': trigger_price.volume, 'turnover_rate': trigger_price.turnover_rate}
            )
            return False, record
        
        # 6. 极端波动检查
        if self._is_extreme_volatility(sample, trigger_price):
            record = self._create_record(
                sample, 'extreme_volatility', ExcludeReason.EXTREME_VOLATILITY,
                self._get_volatility_details(sample)
            )
            return self.include_special_cases, record
        
        # 7. 无法交易检查（未来几天都停牌）
        if self._is_no_trade(sample):
            record = self._create_record(
                sample, 'no_trade', ExcludeReason.NO_TRADE,
                {'message': '未来几天无法交易'}
            )
            return False, record
        
        return True, None
    
    def _create_record(
        self,
        sample: ValidationSample,
        case_type: str,
        exclude_reason: ExcludeReason,
        details: Dict
    ) -> SpecialCaseRecord:
        """创建特殊行情记录"""
        severity = 'high' if exclude_reason in [
            ExcludeReason.LIMIT_UP,
            ExcludeReason.LIMIT_DOWN,
            ExcludeReason.SUSPENDED,
        ] else 'normal'
        
        return SpecialCaseRecord(
            sample_id=sample.sample_id,
            stock_code=sample.stock_info.code,
            trigger_date=sample.trigger_date,
            case_type=case_type,
            exclude_reason=exclude_reason.value,
            severity=severity,
            details=details,
        )
    
    def _get_limit_rate(self, sample: ValidationSample) -> float:
        """获取涨跌停幅度"""
        # 主板10%，科创/创业20%
        if not sample.stock_info.is_main_board:
            return 0.20
        return 0.10
    
    def _is_abnormal_price(self, price_data: PriceData) -> bool:
        """检查价格是否异常"""
        # 价格<=0
        if price_data.close <= 0:
            return True
        
        # 最高价<最低价（数据错误）
        if price_data.high < price_data.low:
            return True
        
        # 收盘价超出当日范围（数据错误）
        if price_data.close < price_data.low or price_data.close > price_data.high:
            return True
        
        return False
    
    def _is_low_liquidity(self, price_data: PriceData) -> bool:
        """检查流动性是否不足"""
        # 成交量为0
        if price_data.volume <= 0:
            return True
        
        # 换手率过低（<0.1%）
        if price_data.turnover_rate < 0.001:
            return True
        
        return False
    
    def _is_extreme_volatility(
        self,
        sample: ValidationSample,
        trigger_price: PriceData
    ) -> bool:
        """检查是否极端波动"""
        # 获取前一天的价格
        prev_prices = [p for p in sample.prices if p.date < sample.trigger_date]
        if not prev_prices:
            return False
        
        prev_price = prev_prices[-1]
        change = abs((trigger_price.close - prev_price.close) / prev_price.close)
        
        # 日涨跌幅超过8%视为极端波动
        return change > 0.08
    
    def _get_volatility_details(self, sample: ValidationSample) -> Dict:
        """获取波动详情"""
        trigger_date_obj = parse_date(sample.trigger_date)
        prev_prices = [p for p in sample.prices if parse_date(p.date) and parse_date(p.date) < trigger_date_obj]
        if not prev_prices:
            return {}
        
        prev_price = prev_prices[-1]
        trigger_price = None
        for p in sample.prices:
            p_date = parse_date(p.date)
            if p_date and p_date == trigger_date_obj:
                trigger_price = p
                break
        
        if trigger_price is None:
            return {}
        
        change = (trigger_price.close - prev_price.close) / prev_price.close
        
        return {
            'prev_close': prev_price.close,
            'current_close': trigger_price.close,
            'change_rate': change,
        }
    
    def _is_no_trade(self, sample: ValidationSample) -> bool:
        """检查未来是否无法交易"""
        # 找到触发日在价格序列中的位置
        trigger_idx = None
        trigger_date_obj = parse_date(sample.trigger_date)
        for i, p in enumerate(sample.prices):
            p_date = parse_date(p.date)
            if p_date and p_date >= trigger_date_obj:
                trigger_idx = i
                break
        
        if trigger_idx is None:
            return True
        
        # 检查未来几天是否可以交易
        tradeable_days = 0
        for i in range(trigger_idx, min(trigger_idx + sample.holding_days + 1, len(sample.prices))):
            if sample.prices[i].status == PriceDataStatus.NORMAL:
                tradeable_days += 1
        
        # 如果可交易天数<持仓天数的50%，认为无法交易
        return tradeable_days < (sample.holding_days + 1) * 0.5
    
    def handle_batch(
        self,
        samples: List[ValidationSample]
    ) -> Tuple[List[ValidationSample], List[SpecialCaseRecord], SpecialCaseStats]:
        """
        批量处理样本
        
        Args:
            samples: 验证样本列表
        
        Returns:
            (保留的样本, 特殊记录列表, 统计信息)
        """
        valid_samples = []
        records = []
        stats = SpecialCaseStats(total_samples=len(samples))
        
        for sample in samples:
            keep, record = self.handle_sample(sample)
            
            if record:
                records.append(record)
                stats.excluded_count += 1
                
                # 统计各类型
                if record.exclude_reason == ExcludeReason.SUSPENDED.value:
                    stats.suspended_count += 1
                elif record.exclude_reason == ExcludeReason.LIMIT_UP.value:
                    stats.limit_up_count += 1
                elif record.exclude_reason == ExcludeReason.LIMIT_DOWN.value:
                    stats.limit_down_count += 1
                elif record.exclude_reason == ExcludeReason.INSUFFICIENT_DATA.value:
                    stats.insufficient_data_count += 1
                elif record.exclude_reason == ExcludeReason.ABNORMAL_PRICE.value:
                    stats.abnormal_price_count += 1
                elif record.exclude_reason == ExcludeReason.NO_TRADE.value:
                    stats.no_trade_count += 1
                elif record.exclude_reason == ExcludeReason.EXTREME_VOLATILITY.value:
                    stats.extreme_volatility_count += 1
                elif record.exclude_reason == ExcludeReason.LOW_LIQUIDITY.value:
                    stats.low_liquidity_count += 1
            
            if keep:
                valid_samples.append(sample)
                sample.status = SampleStatus.COMPLETED
            else:
                sample.status = SampleStatus.EXCLUDED
        
        logger.info(
            f"特殊行情处理完成: 总样本{stats.total_samples}, "
            f"保留{len(valid_samples)}, 排除{stats.excluded_count}"
        )
        
        return valid_samples, records, stats


def handle_special_cases(
    samples: List[ValidationSample],
    include_special: bool = False
) -> Tuple[List[ValidationSample], List[SpecialCaseRecord], SpecialCaseStats]:
    """
    处理特殊行情的便捷函数
    
    Args:
        samples: 验证样本列表
        include_special: 是否包含特殊行情样本
    
    Returns:
        (保留的样本, 特殊记录列表, 统计信息)
    """
    handler = SpecialCaseHandler(include_special_cases=include_special)
    return handler.handle_batch(samples)
