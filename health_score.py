# -*- coding: utf-8 -*-
"""
铁律健康度评分模块
评估铁律库的整体健康状况
"""
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import HEALTH_SCORE_CONFIG
from models.rule import Rule, RuleStatus
from models.result import RuleStatistics
from utils.logger import get_logger

logger = get_logger("HealthScore")


@dataclass
class HealthScoreComponent:
    """健康度评分组成部分"""
    name: str
    score: float              # 得分（0-100）
    weight: float             # 权重
    details: Dict = field(default_factory=dict)
    
    @property
    def weighted_score(self) -> float:
        return self.score * self.weight


@dataclass
class HealthScore:
    """健康度评分"""
    overall_score: float = 0  # 总体得分
    
    # 各维度得分
    coverage: HealthScoreComponent = None
    activity: HealthScoreComponent = None
    stability: HealthScoreComponent = None
    diversity: HealthScoreComponent = None
    timeliness: HealthScoreComponent = None
    
    # 详细评估
    grade: str = ""           # 评级（A/B/C/D/F）
    issues: List[str] = field(default_factory=list)
    strengths: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        components = {}
        if self.coverage:
            components['coverage'] = {'score': round(self.coverage.score, 2), 'details': self.coverage.details}
        if self.activity:
            components['activity'] = {'score': round(self.activity.score, 2), 'details': self.activity.details}
        if self.stability:
            components['stability'] = {'score': round(self.stability.score, 2), 'details': self.stability.details}
        if self.diversity:
            components['diversity'] = {'score': round(self.diversity.score, 2), 'details': self.diversity.details}
        if self.timeliness:
            components['timeliness'] = {'score': round(self.timeliness.score, 2), 'details': self.timeliness.details}
        
        return {
            'overall_score': round(self.overall_score, 2),
            'grade': self.grade,
            'components': components,
            'issues': self.issues,
            'strengths': self.strengths,
        }


class HealthScoreCalculator:
    """健康度评分计算器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化计算器
        
        Args:
            config: 配置字典
        """
        self.config = config or HEALTH_SCORE_CONFIG
        self.weights = {
            'coverage': self.config.get('coverage_weight', 0.20),
            'activity': self.config.get('activity_weight', 0.15),
            'stability': self.config.get('stability_weight', 0.25),
            'diversity': self.config.get('diversity_weight', 0.20),
            'timeliness': self.config.get('timeliness_weight', 0.20),
        }
    
    def evaluate_library_health(
        self,
        rules: List[Rule],
        rule_stats: Dict[str, RuleStatistics] = None
    ) -> HealthScore:
        """
        评估铁律库健康度
        
        Args:
            rules: 铁律列表
            rule_stats: 铁律统计字典
        
        Returns:
            健康度评分
        """
        health = HealthScore()
        
        # 1. 计算覆盖度
        health.coverage = self._calc_coverage(rules)
        
        # 2. 计算活跃度
        health.activity = self._calc_activity(rules)
        
        # 3. 计算稳定性
        health.stability = self._calc_stability(rules, rule_stats)
        
        # 4. 计算多样性
        health.diversity = self._calc_diversity(rules)
        
        # 5. 计算时效性
        health.timeliness = self._calc_timeliness(rules, rule_stats)
        
        # 计算总体得分
        total_weighted = sum([
            health.coverage.weighted_score,
            health.activity.weighted_score,
            health.stability.weighted_score,
            health.diversity.weighted_score,
            health.timeliness.weighted_score,
        ])
        
        total_weight = sum(self.weights.values())
        health.overall_score = total_weighted / total_weight * 100
        
        # 评级
        health.grade = self._get_grade(health.overall_score)
        
        # 识别问题和优势
        health.issues, health.strengths = self._analyze_factors(health)
        
        return health
    
    def _calc_coverage(self, rules: List[Rule]) -> HealthScoreComponent:
        """计算覆盖度"""
        if not rules:
            return HealthScoreComponent('coverage', 0, self.weights['coverage'])
        
        # 按系列统计
        series = set(r.id[0] for r in rules if r.id)
        
        # 按类型统计
        rule_types = set(r.rule_type.value for r in rules)
        
        # 覆盖度得分
        series_score = min(len(series) / 8, 1) * 50  # 8个系列为满分
        type_score = min(len(rule_types) / 4, 1) * 50  # 4种类型为满分
        
        score = series_score + type_score
        
        return HealthScoreComponent(
            name='覆盖度',
            score=score,
            weight=self.weights['coverage'],
            details={
                'series_count': len(series),
                'type_count': len(rule_types),
                'total_rules': len(rules),
            }
        )
    
    def _calc_activity(self, rules: List[Rule]) -> HealthScoreComponent:
        """计算活跃度"""
        if not rules:
            return HealthScoreComponent('activity', 0, self.weights['activity'])
        
        # 统计各状态铁律数量
        active_count = sum(1 for r in rules if r.status == RuleStatus.ACTIVE)
        demoted_count = sum(1 for r in rules if r.status == RuleStatus.DEMOTED)
        dormant_count = sum(1 for r in rules if r.status == RuleStatus.DORMANT)
        
        # 活跃度得分：活跃铁律占比
        active_ratio = active_count / len(rules)
        
        # 考虑最近测试的铁律
        recently_tested = sum(1 for r in rules if r.total_tests > 0)
        tested_ratio = recently_tested / len(rules)
        
        score = (active_ratio * 0.6 + tested_ratio * 0.4) * 100
        
        return HealthScoreComponent(
            name='活跃度',
            score=score,
            weight=self.weights['activity'],
            details={
                'active_count': active_count,
                'demoted_count': demoted_count,
                'dormant_count': dormant_count,
                'recently_tested': recently_tested,
            }
        )
    
    def _calc_stability(
        self,
        rules: List[Rule],
        rule_stats: Dict[str, RuleStatistics] = None
    ) -> HealthScoreComponent:
        """计算稳定性"""
        if not rules:
            return HealthScoreComponent('stability', 0, self.weights['stability'])
        
        if rule_stats is None:
            rule_stats = {}
        
        # 统计准确率标准差
        accuracies = []
        for r in rules:
            if r.id in rule_stats:
                accuracies.append(rule_stats[r.id].accuracy)
            elif r.total_tests > 0:
                accuracies.append(r.get_current_accuracy())
        
        if not accuracies:
            return HealthScoreComponent(
                name='稳定性',
                score=50,
                weight=self.weights['stability'],
                details={'message': '无统计数据'}
            )
        
        # 计算变异系数（标准差/均值）
        import numpy as np
        mean_acc = np.mean(accuracies)
        std_acc = np.std(accuracies)
        cv = std_acc / mean_acc if mean_acc > 0 else 0
        
        # 稳定性得分：变异系数越低越稳定
        score = max(0, 100 - cv * 200)
        
        return HealthScoreComponent(
            name='稳定性',
            score=score,
            weight=self.weights['stability'],
            details={
                'mean_accuracy': round(mean_acc, 4),
                'std_accuracy': round(std_acc, 4),
                'cv': round(cv, 4),
            }
        )
    
    def _calc_diversity(self, rules: List[Rule]) -> HealthScoreComponent:
        """计算多样性"""
        if not rules:
            return HealthScoreComponent('diversity', 0, self.weights['diversity'])
        
        # 按系列分组
        series_counts = {}
        for r in rules:
            series = r.id[0] if r.id else 'X'
            series_counts[series] = series_counts.get(series, 0) + 1
        
        # 计算基尼系数（越低越均匀）
        counts = list(series_counts.values())
        n = len(counts)
        if n > 0:
            sorted_counts = sorted(counts)
            cumsum = np.cumsum(sorted_counts)
            gini = (2 * sum((i + 1) * c for i, c in enumerate(sorted_counts))) / (n * cumsum[-1]) - (n + 1) / n
        else:
            gini = 0
        
        # 多样性得分
        diversity_score = max(0, (1 - gini) * 100) if gini > 0 else 100
        
        return HealthScoreComponent(
            name='多样性',
            score=diversity_score,
            weight=self.weights['diversity'],
            details={
                'series_distribution': series_counts,
                'gini_coefficient': round(gini, 4) if gini > 0 else 0,
            }
        )
    
    def _calc_timeliness(
        self,
        rules: List[Rule],
        rule_stats: Dict[str, RuleStatistics] = None
    ) -> HealthScoreComponent:
        """计算时效性"""
        if not rules:
            return HealthScoreComponent('timeliness', 0, self.weights['timeliness'])
        
        # 统计近期有测试的铁律
        recent_threshold = 30  # 30天内
        recent_count = 0
        
        for r in rules:
            if r.total_tests > 0:
                recent_count += 1
        
        timeliness_ratio = recent_count / len(rules)
        score = timeliness_ratio * 100
        
        return HealthScoreComponent(
            name='时效性',
            score=score,
            weight=self.weights['timeliness'],
            details={
                'recent_tested': recent_count,
                'total_rules': len(rules),
            }
        )
    
    def _get_grade(self, score: float) -> str:
        """根据得分获取评级"""
        if score >= 90:
            return 'A'
        elif score >= 80:
            return 'B'
        elif score >= 70:
            return 'C'
        elif score >= 60:
            return 'D'
        else:
            return 'F'
    
    def _analyze_factors(
        self,
        health: HealthScore
    ) -> Tuple[List[str], List[str]]:
        """分析优缺点"""
        issues = []
        strengths = []
        
        # 分析各维度
        components = [
            ('coverage', '覆盖度'),
            ('activity', '活跃度'),
            ('stability', '稳定性'),
            ('diversity', '多样性'),
            ('timeliness', '时效性'),
        ]
        
        for attr, name in components:
            comp = getattr(health, attr)
            if comp:
                if comp.score < 60:
                    issues.append(f'{name}不足（{comp.score:.1f}分）')
                elif comp.score >= 80:
                    strengths.append(f'{name}优秀（{comp.score:.1f}分）')
        
        return issues, strengths


import numpy as np


def evaluate_library_health(
    rules: List[Rule],
    rule_stats: Dict[str, RuleStatistics] = None
) -> HealthScore:
    """
    评估铁律库健康度的便捷函数
    
    Args:
        rules: 铁律列表
        rule_stats: 铁律统计字典
    
    Returns:
        健康度评分
    """
    calculator = HealthScoreCalculator()
    return calculator.evaluate_library_health(rules, rule_stats)
