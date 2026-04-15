# -*- coding: utf-8 -*-
"""
压力测试模块
在极端行情下测试铁律的有效性
"""
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import STRESS_TEST_CONFIG, EXTREME_EVENTS
from models.result import SampleTestResult
from utils.logger import get_logger

logger = get_logger("StressTest")


@dataclass
class ExtremeEvent:
    """极端事件"""
    date_str: str
    event_type: str
    market_drop: float
    description: str
    
    @property
    def event_date(self) -> date:
        return date.fromisoformat(self.date_str)


@dataclass
class StressTestResult:
    """压力测试结果"""
    rule_id: str
    event_type: str
    event_date: date
    
    # 正常市场表现
    normal_accuracy: float = 0
    normal_return: float = 0
    normal_sample_count: int = 0
    
    # 极端市场表现
    stress_accuracy: float = 0
    stress_return: float = 0
    stress_sample_count: int = 0
    
    # 评估指标
    accuracy_drop: float = 0         # 准确率下降
    return_drop: float = 0           # 收益率下降
    performance_ratio: float = 0      # 性能比率（压力/正常）
    resilience_score: float = 0      # 韧性评分
    
    def calculate_metrics(self):
        """计算评估指标"""
        # 准确率下降
        self.accuracy_drop = self.normal_accuracy - self.stress_accuracy
        
        # 收益率下降
        self.return_drop = self.normal_return - self.stress_return
        
        # 性能比率
        if self.normal_accuracy > 0:
            self.performance_ratio = self.stress_accuracy / self.normal_accuracy
        else:
            self.performance_ratio = 0 if self.stress_accuracy == 0 else 1
        
        # 韧性评分（0-100）
        # 考虑准确率保持率和收益率保持率
        acc_retention = self.stress_accuracy / self.normal_accuracy if self.normal_accuracy > 0 else 0
        ret_retention = self.stress_return / self.normal_return if self.normal_return > 0 else 0
        
        # 收益率可能为负，使用更复杂的计算
        if self.normal_return > 0:
            if self.stress_return > 0:
                ret_score = self.stress_return / self.normal_return
            elif self.stress_return > self.normal_return:
                ret_score = 0.5
            else:
                ret_score = 0
        elif self.normal_return == 0:
            ret_score = 1 if self.stress_return == 0 else 0
        else:
            # 正常市场也在亏损
            if self.stress_return >= self.normal_return:
                ret_score = 1
            else:
                ret_score = abs(self.stress_return / self.normal_return)
        
        self.resilience_score = (acc_retention * 0.6 + ret_score * 0.4) * 100
    
    def to_dict(self) -> Dict:
        return {
            'rule_id': self.rule_id,
            'event_type': self.event_type,
            'event_date': self.event_date.isoformat(),
            'normal_accuracy': round(self.normal_accuracy, 4),
            'stress_accuracy': round(self.stress_accuracy, 4),
            'accuracy_drop': round(self.accuracy_drop, 4),
            'return_drop': round(self.return_drop, 4),
            'performance_ratio': round(self.performance_ratio, 4),
            'resilience_score': round(self.resilience_score, 2),
            'sample_count': {
                'normal': self.normal_sample_count,
                'stress': self.stress_sample_count,
            },
        }


@dataclass
class StressTestReport:
    """压力测试报告"""
    rule_id: str
    test_results: List[StressTestResult]
    
    # 汇总统计
    avg_resilience: float = 0
    worst_event: str = ""
    best_event: str = ""
    vulnerable_rules: List[str] = field(default_factory=list)
    
    def calculate_summary(self):
        """计算汇总统计"""
        if not self.test_results:
            return
        
        # 平均韧性
        self.avg_resilience = sum(r.resilience_score for r in self.test_results) / len(self.test_results)
        
        # 最差事件
        worst = min(self.test_results, key=lambda x: x.resilience_score)
        self.worst_event = worst.event_type
        
        # 最佳事件
        best = max(self.test_results, key=lambda x: x.resilience_score)
        self.best_event = best.event_type
        
        # 易受冲击的铁律
        self.vulnerable_rules = [
            r.rule_id for r in self.test_results
            if r.resilience_score < 50
        ]
    
    def to_dict(self) -> Dict:
        return {
            'rule_id': self.rule_id,
            'avg_resilience': round(self.avg_resilience, 2),
            'worst_event': self.worst_event,
            'best_event': self.best_event,
            'vulnerable_rules': self.vulnerable_rules,
            'event_results': [r.to_dict() for r in self.test_results],
        }


class StressTester:
    """压力测试器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化测试器
        
        Args:
            config: 配置字典
        """
        self.config = config or STRESS_TEST_CONFIG
        self.extreme_events = self._load_extreme_events()
    
    def _load_extreme_events(self) -> List[ExtremeEvent]:
        """加载极端事件"""
        events = []
        for e in EXTREME_EVENTS:
            events.append(ExtremeEvent(
                date_str=e['date'],
                event_type=e['type'],
                market_drop=e['market_drop'],
                description=e['description'],
            ))
        return events
    
    def test_rule(
        self,
        rule_id: str,
        all_results: List[SampleTestResult],
        event_date: date,
        window_days: int = 10
    ) -> Tuple[List[SampleTestResult], List[SampleTestResult]]:
        """
        分离正常和压力测试样本
        
        Args:
            rule_id: 铁律ID
            all_results: 所有测试结果
            event_date: 极端事件日期
            window_days: 窗口天数
        
        Returns:
            (正常样本, 压力样本)
        """
        normal_samples = []
        stress_samples = []
        
        for result in all_results:
            if rule_id not in result.triggered_rules:
                continue
            
            trigger_date = date.fromisoformat(result.trigger_date)
            days_diff = (trigger_date - event_date).days
            
            # 事件窗口内的样本为压力样本
            if abs(days_diff) <= window_days:
                stress_samples.append(result)
            else:
                normal_samples.append(result)
        
        return normal_samples, stress_samples
    
    def run_stress_test(
        self,
        rule_id: str,
        all_results: List[SampleTestResult],
        event_date: date,
        window_days: int = 10
    ) -> StressTestResult:
        """
        对指定铁律运行压力测试
        
        Args:
            rule_id: 铁律ID
            all_results: 所有测试结果
            event_date: 极端事件日期
            window_days: 窗口天数
        
        Returns:
            压力测试结果
        """
        normal_samples, stress_samples = self.test_rule(
            rule_id, all_results, event_date, window_days
        )
        
        # 找到事件信息
        event_type = ""
        for e in self.extreme_events:
            if e.event_date == event_date:
                event_type = e.event_type
                break
        
        result = StressTestResult(
            rule_id=rule_id,
            event_type=event_type,
            event_date=event_date,
            normal_sample_count=len(normal_samples),
            stress_sample_count=len(stress_samples),
        )
        
        # 计算正常市场表现
        if normal_samples:
            normal_success = sum(1 for s in normal_samples if s.overall_success)
            result.normal_accuracy = normal_success / len(normal_samples)
            result.normal_return = sum(s.return_rate for s in normal_samples) / len(normal_samples)
        
        # 计算极端市场表现
        if stress_samples:
            stress_success = sum(1 for s in stress_samples if s.overall_success)
            result.stress_accuracy = stress_success / len(stress_samples)
            result.stress_return = sum(s.return_rate for s in stress_samples) / len(stress_samples)
        
        # 计算评估指标
        result.calculate_metrics()
        
        return result
    
    def stress_test_rules(
        self,
        rule_ids: List[str],
        all_results: List[SampleTestResult],
        events: List[ExtremeEvent] = None
    ) -> List[StressTestReport]:
        """
        对多条铁律运行压力测试
        
        Args:
            rule_ids: 铁律ID列表
            all_results: 所有测试结果
            events: 极端事件列表（如果为None，使用默认事件）
        
        Returns:
            压力测试报告列表
        """
        if events is None:
            events = self.extreme_events
        
        reports = []
        
        logger.info(f"开始压力测试: {len(rule_ids)}条铁律, {len(events)}个极端事件")
        
        for rule_id in rule_ids:
            test_results = []
            
            for event in events:
                result = self.run_stress_test(
                    rule_id,
                    all_results,
                    event.event_date
                )
                
                # 只添加有样本的结果
                if result.normal_sample_count > 0 or result.stress_sample_count > 0:
                    test_results.append(result)
            
            if test_results:
                report = StressTestReport(
                    rule_id=rule_id,
                    test_results=test_results,
                )
                report.calculate_summary()
                reports.append(report)
        
        logger.info(f"压力测试完成: {len(reports)}份报告")
        
        return reports
    
    def find_resilient_rules(
        self,
        reports: List[StressTestReport],
        resilience_threshold: float = 70
    ) -> List[str]:
        """
        找出韧性强的铁律
        
        Args:
            reports: 压力测试报告
            resilience_threshold: 韧性阈值
        
        Returns:
            韧性强的铁律ID列表
        """
        resilient = []
        
        for report in reports:
            if report.avg_resilience >= resilience_threshold:
                resilient.append(report.rule_id)
        
        return resilient
    
    def find_vulnerable_rules(
        self,
        reports: List[StressTestReport],
        resilience_threshold: float = 50
    ) -> List[str]:
        """
        找出脆弱的铁律
        
        Args:
            reports: 压力测试报告
            resilience_threshold: 韧性阈值
        
        Returns:
            脆弱的铁律ID列表
        """
        vulnerable = []
        
        for report in reports:
            if report.avg_resilience < resilience_threshold:
                vulnerable.append(report.rule_id)
        
        return vulnerable


def stress_test_rules(
    rule_ids: List[str],
    all_results: List[SampleTestResult],
    events: List[Dict] = None
) -> List[StressTestReport]:
    """
    压力测试的便捷函数
    
    Args:
        rule_ids: 铁律ID列表
        all_results: 所有测试结果
        events: 极端事件列表
    
    Returns:
        压力测试报告列表
    """
    tester = StressTester()
    
    # 转换事件格式
    extreme_events = None
    if events:
        extreme_events = [
            ExtremeEvent(
                date_str=e['date'],
                event_type=e['type'],
                market_drop=e['market_drop'],
                description=e.get('description', ''),
            )
            for e in events
        ]
    
    return tester.stress_test_rules(rule_ids, all_results, extreme_events)
