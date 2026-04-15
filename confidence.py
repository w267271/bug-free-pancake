# -*- coding: utf-8 -*-
"""
验证结果置信度分级模块
评估验证结果的可靠性和置信度
"""
from typing import List, Dict, Optional
from dataclasses import dataclass, field
from enum import Enum
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import CONFIDENCE_CONFIG
from models.result import ValidationReport, RuleStatistics, SampleTestResult
from utils.logger import get_logger

logger = get_logger("Confidence")


class ConfidenceLevel(Enum):
    """置信度等级"""
    HIGH = "high"                 # 高置信度
    MEDIUM = "medium"             # 中置信度
    LOW = "low"                   # 低置信度
    VERY_LOW = "very_low"         # 极低置信度


@dataclass
class ConfidenceScore:
    """置信度评分"""
    overall_score: float = 0       # 总体得分（0-100）
    level: str = "low"             # 置信度等级
    
    # 各维度得分
    sample_size_score: float = 0   # 样本量得分
    time_dispersion_score: float = 0  # 时间分散度得分
    market_coverage_score: float = 0  # 市场覆盖得分
    outlier_score: float = 0       # 异常值得分
    
    # 扣分项
    penalties: Dict[str, float] = field(default_factory=dict)
    
    # 详细信息
    sample_count: int = 0
    date_range_days: int = 0
    market_regimes_covered: List[str] = field(default_factory=list)
    outlier_count: int = 0
    
    def to_dict(self) -> Dict:
        return {
            'overall_score': round(self.overall_score, 2),
            'level': self.level,
            'sample_size_score': round(self.sample_size_score, 2),
            'time_dispersion_score': round(self.time_dispersion_score, 2),
            'market_coverage_score': round(self.market_coverage_score, 2),
            'outlier_score': round(self.outlier_score, 2),
            'penalties': {k: round(v, 4) for k, v in self.penalties.items()},
            'details': {
                'sample_count': self.sample_count,
                'date_range_days': self.date_range_days,
                'market_regimes_covered': self.market_regimes_covered,
                'outlier_count': self.outlier_count,
            }
        }


class ConfidenceEvaluator:
    """置信度评估器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化评估器
        
        Args:
            config: 配置字典
        """
        self.config = config or CONFIDENCE_CONFIG
        self.min_sample_size = self.config.get('min_sample_size', 20)
        self.sample_size_penalty = self.config.get('sample_size_penalty', 0.1)
        self.time_dispersion_penalty = self.config.get('time_dispersion_penalty', 0.1)
        self.market_coverage_penalty = self.config.get('market_coverage_penalty', 0.1)
        self.outlier_penalty = self.config.get('outlier_penalty', 0.05)
        self.high_threshold = self.config.get('high_confidence_threshold', 0.8)
        self.medium_threshold = self.config.get('medium_confidence_threshold', 0.6)
    
    def calc_verification_confidence(
        self,
        report: ValidationReport,
        sample_results: List[SampleTestResult] = None
    ) -> ConfidenceScore:
        """
        计算验证结果的置信度
        
        Args:
            report: 验证报告
            sample_results: 样本测试结果列表
        
        Returns:
            置信度评分
        """
        score = ConfidenceScore()
        
        # 获取样本数量
        score.sample_count = report.sample_size
        
        # 1. 样本量得分
        score.sample_size_score = self._calc_sample_size_score(score.sample_count)
        
        # 2. 时间分散度得分
        if sample_results:
            date_range = self._calc_date_range(sample_results)
            score.date_range_days = date_range
            score.time_dispersion_score = self._calc_time_dispersion_score(
                score.sample_count, date_range
            )
        else:
            score.time_dispersion_score = 50  # 默认中等
        
        # 3. 市场覆盖得分
        if sample_results:
            score.market_regimes_covered = self._get_market_regimes(sample_results)
            score.market_coverage_score = self._calc_market_coverage_score(
                score.market_regimes_covered
            )
        else:
            score.market_coverage_score = 50
        
        # 4. 异常值得分
        if sample_results:
            score.outlier_count = self._count_outliers(sample_results)
            score.outlier_score = self._calc_outlier_score(
                score.outlier_count, score.sample_count
            )
        else:
            score.outlier_score = 50
        
        # 计算总体得分
        base_score = (
            score.sample_size_score * 0.35 +
            score.time_dispersion_score * 0.25 +
            score.market_coverage_score * 0.25 +
            score.outlier_score * 0.15
        )
        
        # 应用扣分
        score.penalties = self._calc_penalties(score)
        total_penalty = sum(score.penalties.values())
        
        score.overall_score = max(0, min(100, base_score - total_penalty))
        
        # 确定置信度等级
        score.level = self._get_level(score.overall_score)
        
        return score
    
    def _calc_sample_size_score(self, sample_count: int) -> float:
        """计算样本量得分"""
        if sample_count >= self.min_sample_size * 3:
            return 100
        elif sample_count >= self.min_sample_size * 2:
            return 85
        elif sample_count >= self.min_sample_size:
            return 70
        elif sample_count >= self.min_sample_size * 0.5:
            return 50
        else:
            return 30
    
    def _calc_date_range(self, sample_results: List[SampleTestResult]) -> int:
        """计算样本日期范围"""
        from datetime import datetime
        
        dates = []
        for r in sample_results:
            try:
                d = datetime.strptime(r.trigger_date, '%Y-%m-%d').date()
                dates.append(d)
            except:
                continue
        
        if len(dates) < 2:
            return 0
        
        min_date = min(dates)
        max_date = max(dates)
        return (max_date - min_date).days
    
    def _calc_time_dispersion_score(
        self,
        sample_count: int,
        date_range_days: int
    ) -> float:
        """计算时间分散度得分"""
        # 理想情况：样本均匀分布在整个时间范围内
        if date_range_days == 0:
            return 50
        
        # 计算理想间隔
        ideal_gap = date_range_days / sample_count if sample_count > 0 else date_range_days
        
        # 实际间隔（简化计算）
        actual_gap = 5  # 默认5天
        
        # 分散度得分
        if ideal_gap <= actual_gap:
            return 100
        elif ideal_gap <= actual_gap * 2:
            return 80
        elif ideal_gap <= actual_gap * 5:
            return 60
        else:
            return 40
    
    def _get_market_regimes(self, sample_results: List[SampleTestResult]) -> List[str]:
        """获取覆盖的市场状态"""
        regimes = set()
        for r in sample_results:
            if hasattr(r, 'market_regime') and r.market_regime:
                regimes.add(r.market_regime)
        return list(regimes)
    
    def _calc_market_coverage_score(self, regimes: List[str]) -> float:
        """计算市场覆盖得分"""
        # 标准市场状态：牛市、熊市、震荡市
        standard_regimes = {'bull', 'bear', 'volatile'}
        covered = set(regimes)
        
        # 计算覆盖率
        coverage = len(covered & standard_regimes) / len(standard_regimes)
        
        return coverage * 100
    
    def _count_outliers(self, sample_results: List[SampleTestResult]) -> int:
        """统计异常值数量"""
        if not sample_results:
            return 0
        
        returns = [r.return_rate for r in sample_results]
        
        # 使用IQR方法识别异常值
        q1 = np.percentile(returns, 25)
        q3 = np.percentile(returns, 75)
        iqr = q3 - q1
        
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        
        outliers = [r for r in returns if r < lower_bound or r > upper_bound]
        return len(outliers)
    
    def _calc_outlier_score(self, outlier_count: int, sample_count: int) -> float:
        """计算异常值得分"""
        if sample_count == 0:
            return 50
        
        outlier_ratio = outlier_count / sample_count
        
        # 异常值比例越低越好
        if outlier_ratio <= 0.05:
            return 100
        elif outlier_ratio <= 0.10:
            return 80
        elif outlier_ratio <= 0.15:
            return 60
        else:
            return 40
    
    def _calc_penalties(self, score: ConfidenceScore) -> Dict[str, float]:
        """计算扣分项"""
        penalties = {}
        
        # 样本量不足扣分
        if score.sample_count < self.min_sample_size:
            diff = self.min_sample_size - score.sample_count
            ratio = diff / self.min_sample_size
            penalties['insufficient_sample'] = ratio * 20 * self.sample_size_penalty * 100
        
        # 时间分散不足扣分
        if score.date_range_days < 30:
            penalties['low_time_dispersion'] = self.time_dispersion_penalty * 10
        
        # 市场覆盖不足扣分
        if len(score.market_regimes_covered) < 2:
            penalties['low_market_coverage'] = self.market_coverage_penalty * 15
        
        # 异常值过多扣分
        if score.outlier_count > score.sample_count * 0.1:
            penalties['too_many_outliers'] = self.outlier_penalty * 10
        
        return penalties
    
    def _get_level(self, score: float) -> str:
        """根据得分确定置信度等级"""
        if score >= self.high_threshold * 100:
            return ConfidenceLevel.HIGH.value
        elif score >= self.medium_threshold * 100:
            return ConfidenceLevel.MEDIUM.value
        elif score >= 40:
            return ConfidenceLevel.LOW.value
        else:
            return ConfidenceLevel.VERY_LOW.value
    
    def compare_confidence(
        self,
        score1: ConfidenceScore,
        score2: ConfidenceScore
    ) -> str:
        """
        比较两个置信度评分
        
        Args:
            score1: 置信度评分1
            score2: 置信度评分2
        
        Returns:
            比较结果描述
        """
        diff = score1.overall_score - score2.overall_score
        
        if abs(diff) < 5:
            return "置信度相近"
        elif diff > 0:
            return f"评分1置信度更高（+{diff:.1f}分）"
        else:
            return f"评分2置信度更高（{diff:.1f}分）"


def calc_verification_confidence(
    report: ValidationReport,
    sample_results: List[SampleTestResult] = None
) -> ConfidenceScore:
    """
    计算验证结果置信度的便捷函数
    
    Args:
        report: 验证报告
        sample_results: 样本测试结果
    
    Returns:
        置信度评分
    """
    evaluator = ConfidenceEvaluator()
    return evaluator.calc_verification_confidence(report, sample_results)
