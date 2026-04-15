# -*- coding: utf-8 -*-
"""
铁律时效性评估模块
评估铁律在近期是否仍然有效
"""
from enum import Enum
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import TIMELINESS_CONFIG
from models.result import SampleTestResult, RuleStatistics
from utils.logger import get_logger

logger = get_logger("Timeliness")


class TrendDirection(Enum):
    """趋势方向"""
    STABLE = "stable"              # 稳定
    IMPROVING = "improving"        # 上升
    DECLINING = "declining"        # 下降
    UNKNOWN = "unknown"            # 未知


@dataclass
class TimelinessResult:
    """时效性评估结果"""
    rule_id: str
    
    # 时间窗口统计
    recent_accuracy: float = 0     # 最近准确率
    historical_accuracy: float = 0  # 历史准确率
    recent_sample_count: int = 0   # 最近样本数
    historical_sample_count: int = 0  # 历史样本数
    
    # 趋势分析
    accuracy_change: float = 0     # 准确率变化
    trend: str = "unknown"         # 趋势方向
    confidence: float = 0         # 置信度
    
    # 时效性评估
    is_current: bool = True        # 是否仍然有效
    decay_factor: float = 1.0     # 衰减因子
    timeliness_score: float = 0   # 时效性评分（0-100）
    
    def to_dict(self) -> Dict:
        return {
            'rule_id': self.rule_id,
            'recent_accuracy': round(self.recent_accuracy, 4),
            'historical_accuracy': round(self.historical_accuracy, 4),
            'accuracy_change': round(self.accuracy_change, 4),
            'trend': self.trend,
            'is_current': self.is_current,
            'timeliness_score': round(self.timeliness_score, 2),
            'confidence': round(self.confidence, 2),
            'sample_counts': {
                'recent': self.recent_sample_count,
                'historical': self.historical_sample_count,
            },
        }


@dataclass
class TimelinessReport:
    """时效性评估报告"""
    rule_id: str
    timeliness_result: TimelinessResult
    
    # 历史趋势
    accuracy_by_period: Dict[str, float] = field(default_factory=dict)
    
    # 建议
    recommendations: List[str] = field(default_factory=list)
    action_required: bool = False
    
    def to_dict(self) -> Dict:
        return {
            'rule_id': self.rule_id,
            'is_current': self.timeliness_result.is_current,
            'timeliness_score': round(self.timeliness_result.timeliness_score, 2),
            'trend': self.timeliness_result.trend,
            'accuracy_by_period': self.accuracy_by_period,
            'recommendations': self.recommendations,
            'action_required': self.action_required,
        }


class TimelinessEvaluator:
    """时效性评估器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化评估器
        
        Args:
            config: 配置字典
        """
        self.config = config or TIMELINESS_CONFIG
        self.recent_window = self.config.get('recent_window_days', 90)
        self.historical_window = self.config.get('historical_window_days', 365)
        self.trend_threshold = self.config.get('trend_threshold', 0.05)
        self.decay_rate = self.config.get('decay_rate', 0.95)
    
    def evaluate_timeliness(
        self,
        rule_id: str,
        all_results: List[SampleTestResult],
        current_date: date = None
    ) -> TimelinessResult:
        """
        评估铁律的时效性
        
        Args:
            rule_id: 铁律ID
            all_results: 所有测试结果
            current_date: 当前日期（如果为None，使用今天）
        
        Returns:
            时效性评估结果
        """
        if current_date is None:
            current_date = date.today()
        
        # 分离最近和历史样本
        recent_start = current_date - timedelta(days=self.recent_window)
        historical_start = current_date - timedelta(days=self.historical_window)
        
        recent_samples = []
        historical_samples = []
        
        for result in all_results:
            if rule_id not in result.triggered_rules:
                continue
            
            trigger_date = date.fromisoformat(result.trigger_date)
            
            if trigger_date >= recent_start:
                recent_samples.append(result)
            elif trigger_date >= historical_start:
                historical_samples.append(result)
        
        result = TimelinessResult(rule_id=rule_id)
        
        # 计算最近准确率
        if recent_samples:
            result.recent_sample_count = len(recent_samples)
            recent_success = sum(1 for s in recent_samples if s.overall_success)
            result.recent_accuracy = recent_success / len(recent_samples)
        
        # 计算历史准确率
        if historical_samples:
            result.historical_sample_count = len(historical_samples)
            historical_success = sum(1 for s in historical_samples if s.overall_success)
            result.historical_accuracy = historical_success / len(historical_samples)
        
        # 计算趋势
        if result.recent_sample_count > 0 and result.historical_sample_count > 0:
            result.accuracy_change = result.recent_accuracy - result.historical_accuracy
            
            # 判断趋势方向
            if abs(result.accuracy_change) < self.trend_threshold:
                result.trend = TrendDirection.STABLE.value
            elif result.accuracy_change > 0:
                result.trend = TrendDirection.IMPROVING.value
            else:
                result.trend = TrendDirection.DECLINING.value
            
            # 计算置信度（基于样本量）
            total_samples = result.recent_sample_count + result.historical_sample_count
            result.confidence = min(total_samples / 50, 1.0)
        
        # 计算时效性评分
        result.timeliness_score = self._calculate_timeliness_score(result)
        
        # 判断是否仍然有效
        result.is_current = self._check_if_current(result)
        
        return result
    
    def _calculate_timeliness_score(self, result: TimelinessResult) -> float:
        """计算时效性评分"""
        # 基础分：最近准确率 * 100
        base_score = result.recent_accuracy * 100
        
        # 趋势调整
        trend_adjustment = 0
        if result.trend == TrendDirection.IMPROVING.value:
            trend_adjustment = 10
        elif result.trend == TrendDirection.DECLINING.value:
            trend_adjustment = -15
        
        # 置信度调整
        confidence_adjustment = (result.confidence - 0.5) * 20
        
        # 计算最终评分
        final_score = base_score + trend_adjustment + confidence_adjustment
        
        # 确保在0-100范围内
        return max(0, min(100, final_score))
    
    def _check_if_current(self, result: TimelinessResult) -> bool:
        """判断铁律是否仍然有效"""
        # 最近准确率低于50%认为失效
        if result.recent_accuracy < 0.5:
            return False
        
        # 准确率大幅下降（>20%）认为可能失效
        if result.accuracy_change < -0.2:
            return False
        
        # 最近样本太少（<5）置信度不足
        if result.recent_sample_count < 5:
            return result.recent_accuracy >= 0.6
        
        return True
    
    def generate_report(
        self,
        timeliness_result: TimelinessResult
    ) -> TimelinessReport:
        """
        生成时效性报告
        
        Args:
            timeliness_result: 时效性评估结果
        
        Returns:
            时效性报告
        """
        report = TimelinessReport(
            rule_id=timeliness_result.rule_id,
            timeliness_result=timeliness_result,
        )
        
        # 生成建议
        recommendations = []
        
        if timeliness_result.trend == TrendDirection.DECLINING.value:
            recommendations.append('准确率呈下降趋势，建议密切关注')
            if timeliness_result.accuracy_change < -0.15:
                recommendations.append('准确率下降超过15%，建议降低该铁律权重或暂停使用')
        
        if timeliness_result.confidence < 0.5:
            recommendations.append('样本量不足，建议增加验证样本后再做判断')
        
        if not timeliness_result.is_current:
            recommendations.append('该铁律可能已失效，建议暂时停用')
        
        if timeliness_result.timeliness_score >= 80:
            recommendations.append('该铁律表现良好，可继续使用')
        elif timeliness_result.timeliness_score >= 60:
            recommendations.append('该铁律表现一般，建议保持观察')
        
        report.recommendations = recommendations
        report.action_required = (
            not timeliness_result.is_current or
            timeliness_result.trend == TrendDirection.DECLINING.value
        )
        
        return report
    
    def batch_evaluate(
        self,
        rule_ids: List[str],
        all_results: List[SampleTestResult],
        current_date: date = None
    ) -> List[TimelinessReport]:
        """
        批量评估多条铁律
        
        Args:
            rule_ids: 铁律ID列表
            all_results: 所有测试结果
            current_date: 当前日期
        
        Returns:
            时效性报告列表
        """
        reports = []
        
        logger.info(f"开始时效性评估: {len(rule_ids)}条铁律")
        
        for rule_id in rule_ids:
            result = self.evaluate_timeliness(rule_id, all_results, current_date)
            report = self.generate_report(result)
            reports.append(report)
        
        # 排序：需要行动的优先
        reports.sort(key=lambda x: (
            not x.action_required,
            -x.timeliness_result.timeliness_score
        ))
        
        logger.info(f"时效性评估完成: {len(reports)}份报告")
        
        return reports
    
    def find_outdated_rules(
        self,
        reports: List[TimelinessReport],
        score_threshold: float = 60
    ) -> List[str]:
        """
        找出过时的铁律
        
        Args:
            reports: 时效性报告列表
            score_threshold: 评分阈值
        
        Returns:
            过时铁律ID列表
        """
        outdated = []
        
        for report in reports:
            if report.timeliness_result.timeliness_score < score_threshold:
                outdated.append(report.rule_id)
        
        return outdated


def evaluate_timeliness(
    rule_id: str,
    all_results: List[SampleTestResult],
    current_date: date = None
) -> TimelinessResult:
    """
    评估铁律时效性的便捷函数
    
    Args:
        rule_id: 铁律ID
        all_results: 所有测试结果
        current_date: 当前日期
    
    Returns:
        时效性评估结果
    """
    evaluator = TimelinessEvaluator()
    return evaluator.evaluate_timeliness(rule_id, all_results, current_date)
