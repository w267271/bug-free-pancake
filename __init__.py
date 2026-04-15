# -*- coding: utf-8 -*-
"""
铁律验证系统 - 核心模块
"""
from core.data_fetcher import DataFetcher
from core.rule_parser import RuleParser
from core.rule_validator import RuleValidator
from core.market_regime import MarketRegimeIdentifier
from core.failure_analyzer import FailureAnalyzer
from core.look_ahead_check import LookAheadChecker, LookAheadCheckResult, check_look_ahead_bias
from core.special_cases import SpecialCaseHandler, ExcludeReason, handle_special_cases
from core.sample_selector import SampleSelector, disperse_sampling
from core.rule_correlation import RuleCorrelator, CorrelationMatrix, cluster_similar_rules
from core.parameter_sensitivity import ParameterSensitivityAnalyzer, parameter_sensitivity_analysis
from core.self_check import SystemSelfChecker, self_check_verification_system
from core.stress_test import StressTester, stress_test_rules
from core.timeliness import TimelinessEvaluator, evaluate_timeliness
from core.health_score import HealthScoreCalculator, evaluate_library_health
from core.confidence import ConfidenceEvaluator, calc_verification_confidence
from core.dashboard import DashboardProvider, get_dashboard_data
from core.report_accumulator import ReportAccumulator, get_accumulator

__all__ = [
    # 基础模块
    'DataFetcher',
    'RuleParser',
    'RuleValidator',
    'MarketRegimeIdentifier',
    'FailureAnalyzer',
    
    # 前视偏差检查
    'LookAheadChecker',
    'LookAheadCheckResult',
    'check_look_ahead_bias',
    
    # 特殊行情处理
    'SpecialCaseHandler',
    'ExcludeReason',
    'handle_special_cases',
    
    # 样本选择
    'SampleSelector',
    'disperse_sampling',
    
    # 相关性分析
    'RuleCorrelator',
    'CorrelationMatrix',
    'cluster_similar_rules',
    
    # 参数敏感性分析
    'ParameterSensitivityAnalyzer',
    'parameter_sensitivity_analysis',
    
    # 系统自检
    'SystemSelfChecker',
    'self_check_verification_system',
    
    # 压力测试
    'StressTester',
    'stress_test_rules',
    
    # 时效性评估
    'TimelinessEvaluator',
    'evaluate_timeliness',
    
    # 健康度评分
    'HealthScoreCalculator',
    'evaluate_library_health',
    
    # 置信度分级
    'ConfidenceEvaluator',
    'calc_verification_confidence',
    
    # 仪表盘
    'DashboardProvider',
    'get_dashboard_data',
    
    # 报告累计
    'ReportAccumulator',
    'get_accumulator',
]
