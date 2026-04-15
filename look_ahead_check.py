# -*- coding: utf-8 -*-
"""
前视偏差检查模块
确保验证时只用当时可获得的数据
"""
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import LOOK_AHEAD_CHECK_CONFIG
from models.sample import PriceData, ValidationSample
from utils.logger import get_logger

logger = get_logger("LookAheadCheck")


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


class DataAvailability(Enum):
    """数据可用性状态"""
    AVAILABLE = "available"           # 可用
    FUTURE = "future"                 # 未来数据
    TOO_RECENT = "too_recent"          # 数据太新（可能未确认）
    EXCLUDED = "excluded"              # 排除


@dataclass
class LookAheadViolation:
    """前视偏差违规记录"""
    rule_id: str                        # 违规的铁律
    violation_type: str                  # 违规类型
    prediction_date: date               # 预测日期
    data_used_date: date                # 使用的数据日期
    indicator_name: str                 # 指标名称
    severity: str = "warning"           # 严重程度
    description: str = ""               # 描述


@dataclass
class DataTimestamp:
    """数据可用性时间戳"""
    price_date: date                    # 价格数据日期
    available_date: date                # 数据可用日期（T+1收盘后）
    indicator_date: date                # 指标计算日期
    confirmed_date: date                 # 数据确认日期
    
    @classmethod
    def create(cls, price_date: date, max_lag: int = 1):
        """创建数据时间戳"""
        return cls(
            price_date=price_date,
            available_date=price_date + timedelta(days=max_lag),
            indicator_date=price_date,  # 指标计算通常基于当日收盘
            confirmed_date=price_date + timedelta(days=max_lag),
        )


@dataclass
class LookAheadCheckResult:
    """前视偏差检查结果"""
    is_valid: bool                      # 是否通过检查
    violations: List[LookAheadViolation] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    used_data_dates: List[date] = field(default_factory=list)
    available_dates: List[date] = field(default_factory=list)
    data_lag_days: int = 0             # 数据滞后天数
    
    def add_violation(self, violation: LookAheadViolation):
        """添加违规记录"""
        self.violations.append(violation)
        self.is_valid = False
    
    def add_warning(self, warning: str):
        """添加警告"""
        self.warnings.append(warning)
    
    def summary(self) -> Dict:
        """生成汇总"""
        return {
            'is_valid': self.is_valid,
            'violation_count': len(self.violations),
            'warning_count': len(self.warnings),
            'data_lag_days': self.data_lag_days,
            'severity_counts': self._count_by_severity(),
        }
    
    def _count_by_severity(self) -> Dict[str, int]:
        """按严重程度统计"""
        counts = {'critical': 0, 'error': 0, 'warning': 0}
        for v in self.violations:
            if v.severity in counts:
                counts[v.severity] += 1
        return counts


class LookAheadChecker:
    """前视偏差检查器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化检查器
        
        Args:
            config: 配置字典，如果为None则使用默认配置
        """
        self.config = config or LOOK_AHEAD_CHECK_CONFIG
        self.max_lag = self.config.get('indicator_max_lag', 5)
        self.strict_mode = self.config.get('strict_mode', True)
    
    def check_sample(self, sample: ValidationSample) -> LookAheadCheckResult:
        """
        检查单个样本是否存在前视偏差
        
        Args:
            sample: 验证样本
        
        Returns:
            检查结果
        """
        result = LookAheadCheckResult(is_valid=True)
        
        # 记录预测日期
        prediction_date = sample.trigger_date
        
        # 检查每个触发铁律使用的数据
        for rule_id in sample.triggered_rules:
            rule_check = self._check_rule_look_ahead(
                sample, rule_id, prediction_date
            )
            
            if not rule_check['is_valid']:
                violation = LookAheadViolation(
                    rule_id=rule_id,
                    violation_type=rule_check['violation_type'],
                    prediction_date=prediction_date,
                    data_used_date=rule_check['data_used_date'],
                    indicator_name=rule_check['indicator_name'],
                    severity=rule_check.get('severity', 'warning'),
                    description=rule_check.get('description', ''),
                )
                result.add_violation(violation)
            
            # 记录警告
            for warning in rule_check.get('warnings', []):
                result.add_warning(warning)
        
        # 检查数据滞后
        result.data_lag_days = self._calculate_data_lag(
            sample.prices, prediction_date
        )
        
        return result
    
    def _check_rule_look_ahead(
        self,
        sample: ValidationSample,
        rule_id: str,
        prediction_date: date
    ) -> Dict:
        """
        检查特定铁律是否存在前视偏差
        
        Args:
            sample: 验证样本
            rule_id: 铁律ID
            prediction_date: 预测日期
        
        Returns:
            检查结果字典
        """
        result = {
            'is_valid': True,
            'violation_type': None,
            'data_used_date': None,
            'indicator_name': None,
            'warnings': [],
        }
        
        # 找到预测日期的价格数据
        prediction_date_obj = ensure_date(prediction_date)
        trigger_price = None
        for p in sample.prices:
            p_date = ensure_date(p.date)
            if p_date and p_date == prediction_date_obj:
                trigger_price = p
                break
        
        if trigger_price is None:
            result['is_valid'] = False
            result['violation_type'] = 'no_data'
            return result
        
        # 检查各项指标的可用性
        prediction_date_obj = ensure_date(prediction_date)
        available_before_date = prediction_date_obj - timedelta(days=self.max_lag)
        
        # 1. 检查均线数据
        if self._uses_future_ma(trigger_price, available_before_date):
            if self.strict_mode:
                result['is_valid'] = False
                result['violation_type'] = 'ma_future_data'
                result['data_used_date'] = prediction_date
                result['indicator_name'] = 'ma'
            else:
                result['warnings'].append(
                    f'均线数据可能在预测日期后计算'
                )
        
        # 2. 检查MACD数据
        if self._uses_future_macd(trigger_price, available_before_date):
            result['is_valid'] = False
            result['violation_type'] = 'macd_future_data'
            result['data_used_date'] = prediction_date
            result['indicator_name'] = 'macd'
        
        # 3. 检查KDJ数据
        if self._uses_future_kdj(trigger_price, available_before_date):
            result['is_valid'] = False
            result['violation_type'] = 'kdj_future_data'
            result['data_used_date'] = prediction_date
            result['indicator_name'] = 'kdj'
        
        # 4. 检查RSI数据
        if self._uses_future_rsi(trigger_price, available_before_date):
            result['is_valid'] = False
            result['violation_type'] = 'rsi_future_data'
            result['data_used_date'] = prediction_date
            result['indicator_name'] = 'rsi'
        
        return result
    
    def _uses_future_ma(
        self,
        price_data: PriceData,
        cutoff_date: date
    ) -> bool:
        """检查是否使用了未来的均线数据"""
        # 均线数据通常在收盘后计算
        # 如果数据可用性检查开启，需要确保均线计算日期<=cutoff_date
        if price_data.ma5 > 0 and price_data.date > cutoff_date:
            return True
        if price_data.ma10 > 0 and price_data.date > cutoff_date:
            return True
        if price_data.ma20 > 0 and price_data.date > cutoff_date:
            return True
        return False
    
    def _uses_future_macd(
        self,
        price_data: PriceData,
        cutoff_date: date
    ) -> bool:
        """检查是否使用了未来的MACD数据"""
        # MACD通常需要至少26天数据
        if price_data.macd != 0 and price_data.date > cutoff_date:
            return True
        return False
    
    def _uses_future_kdj(
        self,
        price_data: PriceData,
        cutoff_date: date
    ) -> bool:
        """检查是否使用了未来的KDJ数据"""
        # KDJ通常需要至少9天数据
        if price_data.kdj_k != 0 and price_data.date > cutoff_date:
            return True
        return False
    
    def _uses_future_rsi(
        self,
        price_data: PriceData,
        cutoff_date: date
    ) -> bool:
        """检查是否使用了未来的RSI数据"""
        # RSI通常需要至少14天数据
        if price_data.rsi != 0 and price_data.date > cutoff_date:
            return True
        return False
    
    def _calculate_data_lag(
        self,
        prices: List[PriceData],
        prediction_date: date
    ) -> int:
        """计算数据滞后天数"""
        prediction_date_obj = ensure_date(prediction_date)
        for i, p in enumerate(prices):
            p_date = ensure_date(p.date)
            if p_date and p_date == prediction_date_obj:
                return i  # 预测日期在历史数据中的索引位置
        return 0
    
    def validate_data_availability(
        self,
        price_data: PriceData,
        prediction_date: date
    ) -> Tuple[DataAvailability, str]:
        """
        验证数据的可用性
        
        Args:
            price_data: 价格数据
            prediction_date: 预测日期
        
        Returns:
            (可用性状态, 说明)
        """
        price_date = ensure_date(price_data.date)
        prediction_date_obj = ensure_date(prediction_date)
        
        if price_date and prediction_date_obj and price_date > prediction_date_obj:
            return (
                DataAvailability.FUTURE,
                f'数据日期{price_data.date}晚于预测日期{prediction_date}'
            )
        
        # T+1规则：当天收盘数据在第二天才能使用
        if price_date and prediction_date_obj and price_date == prediction_date_obj:
            return (
                DataAvailability.TOO_RECENT,
                f'数据日期{price_data.date}与预测日期相同，数据可能未确认'
            )
        
        return (
            DataAvailability.AVAILABLE,
            f'数据日期{price_data.date}可用于预测日期{prediction_date}'
        )
    
    def record_data_timestamp(
        self,
        prices: List[PriceData]
    ) -> List[DataTimestamp]:
        """
        记录所有价格数据的时间戳
        
        Args:
            prices: 价格数据列表
        
        Returns:
            数据时间戳列表
        """
        timestamps = []
        for p in prices:
            ts = DataTimestamp.create(p.date, max_lag=self.max_lag)
            timestamps.append(ts)
        return timestamps
    
    def check_indicator_lag(
        self,
        indicator_name: str,
        calculation_days: int
    ) -> bool:
        """
        检查指标计算是否超过最大允许滞后
        
        Args:
            indicator_name: 指标名称
            calculation_days: 指标计算所需天数
        
        Returns:
            是否通过检查
        """
        return calculation_days <= self.max_lag


def check_look_ahead_bias(
    sample: ValidationSample,
    config: Dict = None
) -> LookAheadCheckResult:
    """
    检查样本是否存在前视偏差的便捷函数
    
    Args:
        sample: 验证样本
        config: 配置字典
    
    Returns:
        检查结果
    """
    checker = LookAheadChecker(config)
    return checker.check_sample(sample)
