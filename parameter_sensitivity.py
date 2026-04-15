# -*- coding: utf-8 -*-
"""
参数敏感性分析模块
分析铁律参数的稳定性和敏感性
"""
from typing import List, Dict, Optional, Tuple, Callable
from dataclasses import dataclass, field
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger

logger = get_logger("ParameterSensitivity")


@dataclass
class ParameterRange:
    """参数范围"""
    name: str
    default_value: float
    min_value: float
    max_value: float
    step: float
    unit: str = ""
    
    def generate_values(self) -> List[float]:
        """生成参数值序列"""
        values = []
        current = self.min_value
        while current <= self.max_value:
            values.append(current)
            current += self.step
        return values


@dataclass
class SensitivityResult:
    """敏感性分析结果"""
    parameter_name: str
    test_values: List[float]
    accuracy_scores: List[float]
    return_scores: List[float]
    
    # 统计分析
    optimal_value: float = 0
    optimal_accuracy: float = 0
    sensitivity_index: float = 0  # 敏感性指数
    stability_index: float = 0    # 稳定性指数
    
    def calculate_stats(self):
        """计算统计数据"""
        if not self.accuracy_scores:
            return
        
        # 最优值
        best_idx = np.argmax(self.accuracy_scores)
        self.optimal_value = self.test_values[best_idx]
        self.optimal_accuracy = self.accuracy_scores[best_idx]
        
        # 敏感性指数：准确率变化幅度 / 参数变化幅度
        if len(self.accuracy_scores) >= 2:
            acc_range = max(self.accuracy_scores) - min(self.accuracy_scores)
            param_range = self.test_values[-1] - self.test_values[0]
            self.sensitivity_index = acc_range / param_range if param_range > 0 else 0
        
        # 稳定性指数：准确率的标准差
        self.stability_index = np.std(self.accuracy_scores)
    
    def to_dict(self) -> Dict:
        return {
            'parameter_name': self.parameter_name,
            'optimal_value': self.optimal_value,
            'optimal_accuracy': round(self.optimal_accuracy, 4),
            'sensitivity_index': round(self.sensitivity_index, 4),
            'stability_index': round(self.stability_index, 4),
            'test_points': len(self.test_values),
        }


@dataclass
class SensitivityReport:
    """敏感性分析报告"""
    rule_id: str
    parameter_results: List[SensitivityResult]
    overall_stability: float = 0
    most_sensitive_param: str = ""
    most_stable_param: str = ""
    
    def calculate_overall(self):
        """计算整体评估"""
        if not self.parameter_results:
            return
        
        # 整体稳定性
        stability_scores = [r.stability_index for r in self.parameter_results]
        self.overall_stability = 1 - min(np.mean(stability_scores), 1)
        
        # 最敏感的参数
        if self.parameter_results:
            self.most_sensitive_param = max(
                self.parameter_results,
                key=lambda x: x.sensitivity_index
            ).parameter_name
        
        # 最稳定的参数
        if self.parameter_results:
            self.most_stable_param = min(
                self.parameter_results,
                key=lambda x: x.sensitivity_index
            ).parameter_name
    
    def to_dict(self) -> Dict:
        return {
            'rule_id': self.rule_id,
            'overall_stability': round(self.overall_stability, 4),
            'most_sensitive_param': self.most_sensitive_param,
            'most_stable_param': self.most_stable_param,
            'parameters': [r.to_dict() for r in self.parameter_results],
        }


class ParameterSensitivityAnalyzer:
    """参数敏感性分析器"""
    
    def __init__(self):
        """初始化分析器"""
        self.results: Dict[str, SensitivityReport] = {}
    
    def analyze_rule(
        self,
        rule_id: str,
        validation_func: Callable,
        parameters: List[ParameterRange],
        test_dates: List = None
    ) -> SensitivityReport:
        """
        分析铁律的参数敏感性
        
        Args:
            rule_id: 铁律ID
            validation_func: 验证函数，接收(参数名, 参数值)返回(准确率, 收益率)
            parameters: 参数范围列表
            test_dates: 测试日期列表
        
        Returns:
            敏感性分析报告
        """
        logger.info(f"开始分析铁律{rule_id}的参数敏感性: {[p.name for p in parameters]}")
        
        parameter_results = []
        
        for param in parameters:
            logger.info(f"分析参数: {param.name}")
            
            test_values = param.generate_values()
            accuracy_scores = []
            return_scores = []
            
            for value in test_values:
                # 调用验证函数
                accuracy, returns = validation_func(rule_id, param.name, value)
                accuracy_scores.append(accuracy)
                return_scores.append(returns)
            
            # 创建结果
            result = SensitivityResult(
                parameter_name=param.name,
                test_values=test_values,
                accuracy_scores=accuracy_scores,
                return_scores=return_scores,
            )
            result.calculate_stats()
            parameter_results.append(result)
        
        # 创建报告
        report = SensitivityReport(
            rule_id=rule_id,
            parameter_results=parameter_results,
        )
        report.calculate_overall()
        
        self.results[rule_id] = report
        
        logger.info(
            f"参数敏感性分析完成: {rule_id}, "
            f"整体稳定性={report.overall_stability:.2f}"
        )
        
        return report
    
    def analyze_multiple_rules(
        self,
        rules: List[str],
        validation_func: Callable,
        param_ranges: Dict[str, List[ParameterRange]]
    ) -> Dict[str, SensitivityReport]:
        """
        批量分析多条铁律
        
        Args:
            rules: 铁律ID列表
            validation_func: 验证函数
            param_ranges: 各铁律的参数范围
        
        Returns:
            分析结果字典
        """
        results = {}
        
        for rule_id in rules:
            if rule_id in param_ranges:
                report = self.analyze_rule(
                    rule_id,
                    validation_func,
                    param_ranges[rule_id]
                )
                results[rule_id] = report
        
        return results
    
    def get_optimal_parameters(
        self,
        rule_id: str
    ) -> Dict[str, float]:
        """
        获取最优参数
        
        Args:
            rule_id: 铁律ID
        
        Returns:
            最优参数字典
        """
        if rule_id not in self.results:
            return {}
        
        report = self.results[rule_id]
        optimal_params = {}
        
        for result in report.parameter_results:
            optimal_params[result.parameter_name] = result.optimal_value
        
        return optimal_params
    
    def identify_unstable_rules(
        self,
        stability_threshold: float = 0.7
    ) -> List[str]:
        """
        识别不稳定的铁律
        
        Args:
            stability_threshold: 稳定性阈值
        
        Returns:
            不稳定铁律ID列表
        """
        unstable = []
        
        for rule_id, report in self.results.items():
            if report.overall_stability < stability_threshold:
                unstable.append(rule_id)
        
        return unstable


# 常用参数范围预设
DEFAULT_PARAMETER_RANGES = {
    'threshold': ParameterRange(
        name='threshold',
        default_value=0.5,
        min_value=0.1,
        max_value=0.9,
        step=0.1,
    ),
    'lookback_days': ParameterRange(
        name='lookback_days',
        default_value=20,
        min_value=5,
        max_value=60,
        step=5,
    ),
    'volume_ratio': ParameterRange(
        name='volume_ratio',
        default_value=1.5,
        min_value=1.0,
        max_value=3.0,
        step=0.25,
    ),
    'price_range_min': ParameterRange(
        name='price_range_min',
        default_value=10,
        min_value=5,
        max_value=50,
        step=5,
    ),
    'price_range_max': ParameterRange(
        name='price_range_max',
        default_value=100,
        min_value=50,
        max_value=200,
        step=10,
    ),
}


def parameter_sensitivity_analysis(
    rule_id: str,
    validation_func: Callable,
    parameters: List[ParameterRange]
) -> SensitivityReport:
    """
    参数敏感性分析的便捷函数
    
    Args:
        rule_id: 铁律ID
        validation_func: 验证函数
        parameters: 参数范围列表
    
    Returns:
        敏感性分析报告
    """
    analyzer = ParameterSensitivityAnalyzer()
    return analyzer.analyze_rule(rule_id, validation_func, parameters)
