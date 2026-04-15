# -*- coding: utf-8 -*-
"""
铁律ICIR加权模块
IC（信息系数）和IR（信息比率）计算，基于ICIR的铁律权重分配
"""
from pathlib import Path
import json
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleType
from utils.logger import get_logger

logger = get_logger("ICIRWeighting")


@dataclass
class ICIRecord:
    """IC记录"""
    date: str
    rule_id: str
    ic: float                    # 信息系数
    ir: float                    # 信息比率
    rank_ic: float               # 排序IC
    sample_count: int            # 样本数


@dataclass
class RuleWeight:
    """铁律权重"""
    rule_id: str
    rule_name: str
    base_weight: float = 1.0           # 基础权重
    icir_weight: float = 1.0            # ICIR权重
    dynamic_weight: float = 1.0         # 动态权重
    final_weight: float = 1.0           # 最终权重
    confidence: float = 0.5              # 置信度
    stability: float = 0.5              # 稳定性
    
    def to_dict(self) -> Dict:
        return {
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'base_weight': f"{self.base_weight:.4f}",
            'icir_weight': f"{self.icir_weight:.4f}",
            'dynamic_weight': f"{self.dynamic_weight:.4f}",
            'final_weight': f"{self.final_weight:.4f}",
            'confidence': f"{self.confidence:.2%}",
            'stability': f"{self.stability:.2%}",
        }


@dataclass
class ICIRTrend:
    """ICIR趋势"""
    rule_id: str
    ic_history: List[float] = field(default_factory=list)
    ir_history: List[float] = field(default_factory=list)
    rank_ic_history: List[float] = field(default_factory=list)
    dates: List[str] = field(default_factory=list)
    
    def add_record(self, date: str, ic: float, ir: float, rank_ic: float):
        """添加记录"""
        self.dates.append(date)
        self.ic_history.append(ic)
        self.ir_history.append(ir)
        self.rank_ic_history.append(rank_ic)
        
        # 保持最近60天数据
        if len(self.dates) > 60:
            self.dates = self.dates[-60:]
            self.ic_history = self.ic_history[-60:]
            self.ir_history = self.ir_history[-60:]
            self.rank_ic_history = self.rank_ic_history[-60:]
    
    def get_ic_mean(self) -> float:
        """获取IC均值"""
        return np.mean(self.ic_history) if self.ic_history else 0
    
    def get_ic_std(self) -> float:
        """获取IC标准差"""
        return np.std(self.ic_history) if self.ic_history else 0
    
    def get_ir(self) -> float:
        """获取IR（信息比率）"""
        ic_mean = self.get_ic_mean()
        ic_std = self.get_ic_std()
        if ic_std == 0:
            return 0
        return ic_mean / ic_std
    
    def get_icir(self) -> float:
        """获取ICIR"""
        return self.get_ir()
    
    def get_rank_ic_mean(self) -> float:
        """获取排序IC均值"""
        return np.mean(self.rank_ic_history) if self.rank_ic_history else 0
    
    def is_stable(self, threshold: float = 0.3) -> bool:
        """判断是否稳定（IC标准差小于阈值）"""
        return self.get_ic_std() < threshold
    
    def get_trend(self) -> str:
        """获取趋势"""
        if len(self.ic_history) < 5:
            return "insufficient_data"
        
        recent = self.ic_history[-5:]
        if all(recent[i] >= recent[i+1] for i in range(len(recent)-1)):
            return "improving"
        elif all(recent[i] <= recent[i+1] for i in range(len(recent)-1)):
            return "declining"
        return "stable"


class ICIRCalculator:
    """ICIR计算器"""
    
    def __init__(self):
        """初始化ICIR计算器"""
        self.records: List[ICIRecord] = []
        self.trends: Dict[str, ICIRTrend] = {}
        self.weights: Dict[str, RuleWeight] = {}
        
        # 配置
        self.config = {
            'min_samples': 10,           # 最小样本数
            'icir_lookback': 20,         # ICIR回溯期
            'weight_smoothing': 0.3,     # 权重平滑系数
            'decay_factor': 0.95,        # 衰减因子
            'stability_threshold': 0.3,  # 稳定性阈值
        }
    
    def calculate_ic(
        self,
        predicted_values: List[float],
        actual_values: List[float],
    ) -> float:
        """
        计算信息系数（IC）
        
        IC = correlation(predicted, actual)
        
        Args:
            predicted_values: 预测值
            actual_values: 实际值
        
        Returns:
            IC值（-1到1）
        """
        if len(predicted_values) < self.config['min_samples']:
            return 0
        
        try:
            correlation = np.corrcoef(predicted_values, actual_values)[0, 1]
            return float(correlation) if not np.isnan(correlation) else 0
        except Exception:
            return 0
    
    def calculate_rank_ic(
        self,
        predicted_values: List[float],
        actual_values: List[float],
    ) -> float:
        """
        计算排序信息系数（Rank IC）
        
        Rank IC = correlation(rank(predicted), rank(actual))
        
        Args:
            predicted_values: 预测值
            actual_values: 实际值
        
        Returns:
            排序IC值
        """
        if len(predicted_values) < self.config['min_samples']:
            return 0
        
        try:
            predicted_ranks = np.argsort(np.argsort(predicted_values))
            actual_ranks = np.argsort(np.argsort(actual_values))
            rank_ic = np.corrcoef(predicted_ranks, actual_ranks)[0, 1]
            return float(rank_ic) if not np.isnan(rank_ic) else 0
        except Exception:
            return 0
    
    def calculate_ir(
        self,
        ic_series: List[float],
    ) -> float:
        """
        计算信息比率（IR）
        
        IR = mean(IC) / std(IC)
        
        Args:
            ic_series: IC时间序列
        
        Returns:
            IR值
        """
        if len(ic_series) < 3:
            return 0
        
        ic_mean = np.mean(ic_series)
        ic_std = np.std(ic_series)
        
        if ic_std == 0:
            return 0
        
        return ic_mean / ic_std
    
    def update_icir(
        self,
        rule_id: str,
        date: str,
        predicted_returns: List[float],
        actual_returns: List[float],
    ):
        """
        更新铁律的ICIR
        
        Args:
            rule_id: 铁律ID
            date: 日期
            predicted_returns: 预测收益
            actual_returns: 实际收益
        """
        ic = self.calculate_ic(predicted_returns, actual_returns)
        rank_ic = self.calculate_rank_ic(predicted_returns, actual_returns)
        
        # 初始化趋势
        if rule_id not in self.trends:
            self.trends[rule_id] = ICIRTrend(rule_id=rule_id)
        
        trend = self.trends[rule_id]
        
        # 计算短期IR（使用最近数据）
        recent_ic = trend.ic_history[-self.config['icir_lookback']:] if trend.ic_history else [ic]
        ir = self.calculate_ir(recent_ic)
        
        # 添加记录
        trend.add_record(date, ic, ir, rank_ic)
        
        # 记录
        record = ICIRecord(
            date=date,
            rule_id=rule_id,
            ic=ic,
            ir=ir,
            rank_ic=rank_ic,
            sample_count=len(predicted_returns),
        )
        self.records.append(record)
        
        logger.debug(f"更新ICIR: {rule_id} IC={ic:.4f} IR={ir:.4f}")
    
    def calculate_rule_weight(self, rule_id: str) -> RuleWeight:
        """
        计算单条铁律的权重
        
        Args:
            rule_id: 铁律ID
        
        Returns:
            铁律权重
        """
        if rule_id not in self.trends:
            return RuleWeight(rule_id=rule_id, rule_name="")
        
        trend = self.trends[rule_id]
        
        # ICIR权重 = IC均值 * IR
        ic_mean = trend.get_ic_mean()
        ir = trend.get_ir()
        icir_weight = max(0, ic_mean * ir * 10)  # 缩放
        
        # 置信度 = 样本数归一化
        confidence = min(len(trend.ic_history) / 50, 1.0)
        
        # 稳定性
        stability = 1.0 if trend.is_stable(self.config['stability_threshold']) else 0.5
        
        # 最终权重
        final_weight = icir_weight * confidence * stability
        
        # 归一化
        if final_weight < 0.1:
            final_weight = 0.1
        if final_weight > 3.0:
            final_weight = 3.0
        
        weight = RuleWeight(
            rule_id=rule_id,
            rule_name="",
            icir_weight=icir_weight,
            confidence=confidence,
            stability=stability,
            final_weight=final_weight,
        )
        
        self.weights[rule_id] = weight
        return weight
    
    def calculate_all_weights(
        self,
        rulesets: List[RuleSet],
    ) -> Dict[str, RuleWeight]:
        """
        计算所有铁律的权重
        
        Args:
            rulesets: 铁律集合
        
        Returns:
            权重字典
        """
        # 获取所有铁律ID
        all_rules = []
        for rs in rulesets:
            for rule in rs.rules:
                all_rules.append(rule)
                rule_name_map = {rule.id: rule.name}
        
        # 计算每条铁律的权重
        for rule in all_rules:
            if rule.id in self.trends:
                weight = self.calculate_rule_weight(rule.id)
                weight.rule_name = rule.name
                self.weights[rule.id] = weight
            else:
                # 没有ICIR数据的铁律使用默认权重
                self.weights[rule.id] = RuleWeight(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    final_weight=1.0,
                )
        
        # 归一化权重
        total_weight = sum(w.final_weight for w in self.weights.values())
        if total_weight > 0:
            for rule_id, weight in self.weights.items():
                weight.final_weight = weight.final_weight / (total_weight / len(self.weights))
                self.weights[rule_id] = weight
        
        return self.weights
    
    def get_weighted_signal(
        self,
        rules: List[Rule],
        signals: Dict[str, float],
    ) -> float:
        """
        计算加权信号
        
        Args:
            rules: 铁律列表
            signals: {rule_id: signal_value}
        
        Returns:
            加权信号值
        """
        if not signals:
            return 0
        
        weighted_sum = 0
        total_weight = 0
        
        for rule in rules:
            if rule.id in signals:
                weight = self.weights.get(rule.id)
                if weight:
                    w = weight.final_weight
                else:
                    w = 1.0
                
                weighted_sum += signals[rule.id] * w
                total_weight += w
        
        if total_weight == 0:
            return 0
        
        return weighted_sum / total_weight
    
    def adjust_weights_dynamically(
        self,
        recent_performance: Dict[str, float],
    ):
        """
        动态调整权重
        
        Args:
            recent_performance: {rule_id: recent_return}
        """
        if not self.weights:
            return
        
        # 计算平均收益
        if not recent_performance:
            return
        
        avg_return = np.mean(list(recent_performance.values()))
        
        for rule_id, recent_return in recent_performance.items():
            if rule_id not in self.weights:
                continue
            
            weight = self.weights[rule_id]
            
            # 如果近期表现好于平均，增加权重
            if recent_return > avg_return:
                adjustment = 1 + (recent_return - avg_return) * self.config['weight_smoothing']
            else:
                adjustment = 1 - (avg_return - recent_return) * self.config['weight_smoothing']
            
            # 应用衰减和调整
            weight.dynamic_weight *= self.config['decay_factor']
            weight.dynamic_weight *= adjustment
            
            # 限制范围
            weight.dynamic_weight = max(0.5, min(2.0, weight.dynamic_weight))
            
            # 更新最终权重
            weight.final_weight = weight.base_weight * weight.icir_weight * weight.dynamic_weight
        
        # 重新归一化
        total_weight = sum(w.final_weight for v in self.weights.values() for w in [v])
        if total_weight > 0:
            for rule_id, weight in self.weights.items():
                weight.final_weight = weight.final_weight / (total_weight / len(self.weights))
    
    def get_icir_report(self) -> Dict:
        """获取ICIR报告"""
        report = {
            'total_rules': len(self.trends),
            'rules': [],
        }
        
        for rule_id, trend in self.trends.items():
            weight = self.weights.get(rule_id)
            report['rules'].append({
                'rule_id': rule_id,
                'ic_mean': f"{trend.get_ic_mean():.4f}",
                'ic_std': f"{trend.get_ic_std():.4f}",
                'ir': f"{trend.get_ir():.4f}",
                'rank_ic_mean': f"{trend.get_rank_ic_mean():.4f}",
                'trend': trend.get_trend(),
                'is_stable': trend.is_stable(),
                'final_weight': f"{weight.final_weight:.4f}" if weight else "N/A",
            })
        
        return report
    
    def export_weights(self, export_path: str):
        """导出权重"""
        data = {
            'generated_at': datetime.now().isoformat(),
            'weights': {k: v.to_dict() for k, v in self.weights.items()},
            'config': self.config,
        }
        
        with open(export_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"导出权重到 {export_path}")


class CombinedSignalGenerator:
    """组合信号生成器"""
    
    def __init__(self, icir_calculator: ICIRCalculator):
        self.icir_calculator = icir_calculator
    
    def generate_combined_signal(
        self,
        rules: List[Rule],
        market_data: Dict[str, Any],
        use_icir_weight: bool = True,
    ) -> Dict[str, Any]:
        """
        生成组合信号
        
        Args:
            rules: 铁律列表
            market_data: 市场数据
            use_icir_weight: 是否使用ICIR权重
        
        Returns:
            组合信号结果
        """
        buy_signals = []
        sell_signals = []
        neutral_signals = []
        
        for rule in rules:
            signal = self._evaluate_rule(rule, market_data)
            
            if signal > 0.5:
                buy_signals.append({
                    'rule_id': rule.id,
                    'rule_name': rule.name,
                    'signal': signal,
                    'weight': self.icir_calculator.weights.get(rule.id, RuleWeight(rule_id=rule.id)).final_weight,
                })
            elif signal < -0.5:
                sell_signals.append({
                    'rule_id': rule.id,
                    'rule_name': rule.name,
                    'signal': signal,
                    'weight': self.icir_calculator.weights.get(rule.id, RuleWeight(rule_id=rule.id)).final_weight,
                })
            else:
                neutral_signals.append({
                    'rule_id': rule.id,
                    'rule_name': rule.name,
                    'signal': signal,
                })
        
        # 计算加权信号
        if use_icir_weight and buy_signals:
            total_weight = sum(s['weight'] for s in buy_signals)
            weighted_signal = sum(s['signal'] * s['weight'] for s in buy_signals) / total_weight
        else:
            weighted_signal = np.mean([s['signal'] for s in buy_signals]) if buy_signals else 0
        
        # 决策
        if weighted_signal > 0.3:
            decision = "BUY"
        elif weighted_signal < -0.3:
            decision = "SELL"
        else:
            decision = "HOLD"
        
        return {
            'decision': decision,
            'weighted_signal': weighted_signal,
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'neutral_signals': neutral_signals,
            'signal_strength': abs(weighted_signal),
        }
    
    def _evaluate_rule(self, rule: Rule, market_data: Dict[str, Any]) -> float:
        """评估单条铁律"""
        # 简化实现：基于规则类型和准确率
        if rule.rule_type == RuleType.BUY_SIGNAL:
            base_signal = 1.0
        elif rule.rule_type == RuleType.SELL_SIGNAL:
            base_signal = -1.0
        elif rule.rule_type == RuleType.FORBIDDEN:
            base_signal = -2.0
        else:
            base_signal = 0
        
        # 乘以准确率
        accuracy = rule.get_current_accuracy()
        return base_signal * accuracy


def integrate_with_validator(validator):
    """
    将ICIR加权模块集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    # 创建ICIR计算器
    icir_calc = ICIRCalculator()
    signal_gen = CombinedSignalGenerator(icir_calc)
    
    # 添加到验证器
    validator.icir_calculator = icir_calc
    validator.signal_generator = signal_gen
    validator.calculate_all_weights = lambda rs: icir_calc.calculate_all_weights(rs)
    validator.get_weighted_signal = lambda rules, signals: icir_calc.get_weighted_signal(rules, signals)
    validator.generate_combined_signal = lambda rules, data: signal_gen.generate_combined_signal(rules, data)
    validator.get_icir_report = icir_calc.get_icir_report
    
    return validator
