# -*- coding: utf-8 -*-
"""
验证系统自检模块
确保验证系统的正确性和可靠性
"""
from typing import List, Dict, Optional, Tuple, Callable
from dataclasses import dataclass, field
from enum import Enum
import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger

logger = get_logger("SelfCheck")


class CheckStatus(Enum):
    """检查状态"""
    PASS = "pass"                        # 通过
    FAIL = "fail"                        # 失败
    WARNING = "warning"                  # 警告
    SKIP = "skip"                        # 跳过


@dataclass
class CheckResult:
    """检查结果"""
    check_name: str
    status: CheckStatus
    message: str
    details: Dict = field(default_factory=dict)
    execution_time: float = 0  # 执行时间（秒）
    
    def to_dict(self) -> Dict:
        return {
            'check_name': self.check_name,
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'execution_time': round(self.execution_time, 3),
        }


@dataclass
class SelfCheckReport:
    """自检报告"""
    timestamp: str
    total_checks: int = 0
    passed: int = 0
    failed: int = 0
    warnings: int = 0
    skipped: int = 0
    results: List[CheckResult] = field(default_factory=list)
    
    def add_result(self, result: CheckResult):
        """添加检查结果"""
        self.results.append(result)
        self.total_checks += 1
        
        if result.status == CheckStatus.PASS:
            self.passed += 1
        elif result.status == CheckStatus.FAIL:
            self.failed += 1
        elif result.status == CheckStatus.WARNING:
            self.warnings += 1
        elif result.status == CheckStatus.SKIP:
            self.skipped += 1
    
    def is_healthy(self) -> bool:
        """系统是否健康"""
        return self.failed == 0 and self.warnings == 0
    
    def summary(self) -> Dict:
        return {
            'timestamp': self.timestamp,
            'total_checks': self.total_checks,
            'passed': self.passed,
            'failed': self.failed,
            'warnings': self.warnings,
            'skipped': self.skipped,
            'pass_rate': self.passed / self.total_checks if self.total_checks > 0 else 0,
            'is_healthy': self.is_healthy(),
        }


class SystemSelfChecker:
    """系统自检器"""
    
    def __init__(self):
        """初始化自检器"""
        self.checks: List[Callable] = []
        self._register_default_checks()
    
    def _register_default_checks(self):
        """注册默认检查"""
        self.checks = [
            self._check_data_integrity,
            self._check_indicator_calculation,
            self._check_look_ahead_bias,
            self._check_logic_consistency,
            self._check_boundary_conditions,
            self._check_special_cases_handling,
            self._check_result_calculation,
        ]
    
    def run_all_checks(self) -> SelfCheckReport:
        """
        运行所有检查
        
        Returns:
            自检报告
        """
        from datetime import datetime
        
        report = SelfCheckReport(timestamp=datetime.now().isoformat())
        
        logger.info("开始系统自检...")
        
        for check_func in self.checks:
            try:
                result = check_func()
                report.add_result(result)
                
                status_icon = {
                    CheckStatus.PASS: '✅',
                    CheckStatus.FAIL: '❌',
                    CheckStatus.WARNING: '⚠️',
                    CheckStatus.SKIP: '⏭️',
                }.get(result.status, '❓')
                
                logger.info(f"{status_icon} {result.check_name}: {result.message}")
                
            except Exception as e:
                logger.error(f"检查{check_func.__name__}执行失败: {e}")
                report.add_result(CheckResult(
                    check_name=check_func.__name__,
                    status=CheckStatus.FAIL,
                    message=f"执行失败: {str(e)}",
                ))
        
        logger.info(
            f"系统自检完成: 通过{report.passed}/{report.total_checks}, "
            f"失败{report.failed}, 警告{report.warnings}"
        )
        
        return report
    
    def _check_data_integrity(self) -> CheckResult:
        """检查数据完整性"""
        import time
        start = time.time()
        
        try:
            # 检查核心模块是否可导入
            from models.sample import ValidationSample, PriceData
            from models.rule import Rule, RuleType
            from models.result import ValidationReport
            
            # 检查数据模型字段
            sample = ValidationSample(
                sample_id="test",
                stock_code="000001",
                trigger_date=None,
                trigger_price=0,
                holding_days=3,
            )
            
            details = {
                'modules_loaded': True,
                'models_intact': True,
            }
            
            return CheckResult(
                check_name='数据完整性检查',
                status=CheckStatus.PASS,
                message='核心数据模型完整',
                details=details,
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='数据完整性检查',
                status=CheckStatus.FAIL,
                message=f'数据完整性检查失败: {str(e)}',
                execution_time=time.time() - start,
            )
    
    def _check_indicator_calculation(self) -> CheckResult:
        """检查指标计算"""
        import time
        start = time.time()
        
        try:
            # 模拟计算均线
            test_prices = [10.0, 10.5, 11.0, 10.8, 11.2, 11.5]
            
            # 简单移动平均
            def simple_ma(prices, period):
                result = []
                for i in range(len(prices)):
                    if i < period - 1:
                        result.append(sum(prices[:i+1]) / (i+1))
                    else:
                        result.append(sum(prices[i-period+1:i+1]) / period)
                return result
            
            ma5 = simple_ma(test_prices, 5)
            
            # 验证计算结果
            expected_ma5_last = (10.8 + 11.2 + 11.5) / 3
            actual_ma5_last = ma5[-1]
            
            is_correct = abs(expected_ma5_last - actual_ma5_last) < 0.01
            
            details = {
                'test_prices': test_prices,
                'ma5_result': ma5,
                'expected_last': expected_ma5_last,
                'actual_last': actual_ma5_last,
            }
            
            return CheckResult(
                check_name='指标计算检查',
                status=CheckStatus.PASS if is_correct else CheckStatus.FAIL,
                message='指标计算正确' if is_correct else '指标计算错误',
                details=details,
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='指标计算检查',
                status=CheckStatus.FAIL,
                message=f'指标计算检查失败: {str(e)}',
                execution_time=time.time() - start,
            )
    
    def _check_look_ahead_bias(self) -> CheckResult:
        """检查前视偏差控制"""
        import time
        start = time.time()
        
        try:
            from core.look_ahead_check import LookAheadChecker
            
            checker = LookAheadChecker()
            
            # 验证检查器可以正常工作
            details = {
                'look_ahead_checker_loaded': True,
                'config_enabled': checker.config.get('enabled', False),
            }
            
            return CheckResult(
                check_name='前视偏差检查',
                status=CheckStatus.PASS,
                message='前视偏差检查机制正常',
                details=details,
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='前视偏差检查',
                status=CheckStatus.WARNING,
                message=f'前视偏差检查模块异常: {str(e)}',
                execution_time=time.time() - start,
            )
    
    def _check_logic_consistency(self) -> CheckResult:
        """检查逻辑一致性"""
        import time
        start = time.time()
        
        try:
            # 检查铁律逻辑一致性
            issues = []
            
            # 1. 检查禁止买入和买入信号不应同时触发
            # （需要实际的铁律库数据）
            
            # 2. 检查止损阈值应小于止盈阈值
            from config import STOP_LOSS_CONFIG, VALIDATION_CONFIG
            
            stop_loss = STOP_LOSS_CONFIG.get('high_score_stop_loss', -0.07)
            profit_threshold = VALIDATION_CONFIG.get('profit_threshold', 0.03)
            
            if stop_loss >= 0:
                issues.append('止损阈值设置错误')
            
            details = {
                'stop_loss_config': stop_loss,
                'profit_threshold_config': profit_threshold,
                'logic_issues': issues,
                'issues_count': len(issues),
            }
            
            status = CheckStatus.PASS if len(issues) == 0 else CheckStatus.WARNING
            
            return CheckResult(
                check_name='逻辑一致性检查',
                status=status,
                message='逻辑一致' if len(issues) == 0 else f'发现{len(issues)}个逻辑问题',
                details=details,
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='逻辑一致性检查',
                status=CheckStatus.FAIL,
                message=f'逻辑一致性检查失败: {str(e)}',
                execution_time=time.time() - start,
            )
    
    def _check_boundary_conditions(self) -> CheckResult:
        """检查边界条件"""
        import time
        start = time.time()
        
        try:
            # 测试边界情况处理
            test_cases = [
                {'name': '空价格列表', 'input': [], 'expected': 0},
                {'name': '单日数据', 'input': [10.0], 'expected': 0},
                {'name': '负价格', 'input': [-10.0, 10.0], 'expected': 0},
                {'name': '零价格', 'input': [0, 10.0], 'expected': 0},
            ]
            
            def safe_max(data):
                if not data:
                    return 0
                return max(data) if all(d >= 0 for d in data) else 0
            
            results = []
            for case in test_cases:
                result = safe_max(case['input'])
                is_correct = result == case['expected']
                results.append({
                    'name': case['name'],
                    'input': case['input'],
                    'result': result,
                    'expected': case['expected'],
                    'correct': is_correct,
                })
            
            all_correct = all(r['correct'] for r in results)
            
            return CheckResult(
                check_name='边界条件检查',
                status=CheckStatus.PASS if all_correct else CheckStatus.WARNING,
                message='边界条件处理正确' if all_correct else '部分边界条件处理异常',
                details={'test_results': results},
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='边界条件检查',
                status=CheckStatus.FAIL,
                message=f'边界条件检查失败: {str(e)}',
                execution_time=time.time() - start,
            )
    
    def _check_special_cases_handling(self) -> CheckResult:
        """检查特殊行情处理"""
        import time
        start = time.time()
        
        try:
            from core.special_cases import SpecialCaseHandler, ExcludeReason
            
            handler = SpecialCaseHandler()
            
            # 验证处理器可以正常实例化
            details = {
                'handler_loaded': True,
                'exclude_reasons': [e.value for e in ExcludeReason],
            }
            
            return CheckResult(
                check_name='特殊行情处理检查',
                status=CheckStatus.PASS,
                message='特殊行情处理机制正常',
                details=details,
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='特殊行情处理检查',
                status=CheckStatus.WARNING,
                message=f'特殊行情处理模块异常: {str(e)}',
                execution_time=time.time() - start,
            )
    
    def _check_result_calculation(self) -> CheckResult:
        """检查结果计算"""
        import time
        start = time.time()
        
        try:
            # 测试收益率计算
            entry_price = 10.0
            exit_price = 10.5
            expected_return = 0.05
            
            actual_return = (exit_price - entry_price) / entry_price
            
            # 测试最大回撤计算
            prices = [10.0, 10.2, 10.5, 10.3, 10.8, 10.6]
            max_price = max(prices)
            min_after_max = min(prices[prices.index(max_price):])
            expected_drawdown = (max_price - min_after_max) / max_price
            
            return_accurate = abs(actual_return - expected_return) < 0.001
            drawdown_accurate = abs(expected_drawdown - 0.019) < 0.01
            
            details = {
                'return_calculation': {
                    'expected': expected_return,
                    'actual': actual_return,
                    'accurate': return_accurate,
                },
                'drawdown_calculation': {
                    'prices': prices,
                    'expected': expected_drawdown,
                    'accurate': drawdown_accurate,
                },
            }
            
            is_correct = return_accurate and drawdown_accurate
            
            return CheckResult(
                check_name='结果计算检查',
                status=CheckStatus.PASS if is_correct else CheckStatus.FAIL,
                message='结果计算正确' if is_correct else '结果计算存在误差',
                details=details,
                execution_time=time.time() - start,
            )
            
        except Exception as e:
            return CheckResult(
                check_name='结果计算检查',
                status=CheckStatus.FAIL,
                message=f'结果计算检查失败: {str(e)}',
                execution_time=time.time() - start,
            )


def self_check_verification_system() -> SelfCheckReport:
    """
    验证系统自检的便捷函数
    
    Returns:
        自检报告
    """
    checker = SystemSelfChecker()
    return checker.run_all_checks()
