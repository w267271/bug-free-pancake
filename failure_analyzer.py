# -*- coding: utf-8 -*-
"""
失败归因分析模块
"""
from typing import List, Dict, Optional, Tuple
from collections import Counter
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.result import SampleTestResult, RuleStatistics
from models.rule import Rule, RuleType
from utils.logger import get_logger

logger = get_logger("FailureAnalyzer")


class FailureAnalyzer:
    """失败案例归因分析器"""
    
    def __init__(self, rule_parser):
        self.rule_parser = rule_parser
    
    def analyze_failures(
        self,
        failure_cases: List[SampleTestResult],
        all_results: List[SampleTestResult]
    ) -> Dict:
        """
        分析失败案例
        
        Args:
            failure_cases: 失败案例列表
            all_results: 所有验证结果
        
        Returns:
            分析结果
        """
        if not failure_cases:
            logger.info("没有失败案例需要分析")
            return {}
        
        logger.info(f"开始分析 {len(failure_cases)} 个失败案例")
        
        analysis = {
            'total_failures': len(failure_cases),
            'failure_rate': len(failure_cases) / len(all_results) if all_results else 0,
            'by_rule': {},
            'by_category': {},
            'common_patterns': [],
            'recommendations': [],
        }
        
        # 1. 按铁律分析失败
        analysis['by_rule'] = self._analyze_by_rule(failure_cases)
        
        # 2. 按分类分析失败
        analysis['by_category'] = self._analyze_by_category(failure_cases)
        
        # 3. 识别失败模式
        analysis['common_patterns'] = self._identify_patterns(failure_cases)
        
        # 4. 生成改进建议
        analysis['recommendations'] = self._generate_recommendations(analysis)
        
        return analysis
    
    def _analyze_by_rule(self, failure_cases: List[SampleTestResult]) -> Dict:
        """按铁律分析失败"""
        rule_failures = Counter()
        rule_total = Counter()
        
        for result in failure_cases:
            for rule_id in result.triggered_rules:
                rule_failures[rule_id] += 1
        
        for result in self._get_all_triggered_rules():
            for rule_id in result:
                rule_total[rule_id] += 1
        
        rule_analysis = {}
        for rule_id, fail_count in rule_failures.items():
            total = rule_total.get(rule_id, fail_count)
            fail_rate = fail_count / total if total > 0 else 0
            
            rule = self.rule_parser.get_rule(rule_id)
            rule_analysis[rule_id] = {
                'rule_name': rule.name if rule else '未知',
                'rule_type': rule.rule_type.value if rule else 'unknown',
                'fail_count': fail_count,
                'total_count': total,
                'fail_rate': fail_rate,
                'accuracy': 1 - fail_rate,
                'is_problematic': fail_rate > 0.5,
            }
        
        return rule_analysis
    
    def _get_all_triggered_rules(self) -> List[List[str]]:
        """获取所有触发的铁律"""
        # 这个需要在外部传入或从其他模块获取
        return []
    
    def _analyze_by_category(self, failure_cases: List[SampleTestResult]) -> Dict:
        """按分类分析失败"""
        category_failures = Counter()
        
        for result in failure_cases:
            for rule_id in result.triggered_rules:
                rule = self.rule_parser.get_rule(rule_id)
                if rule:
                    category = rule.category
                    category_failures[category] += 1
        
        category_analysis = {}
        for category, count in category_failures.most_common():
            category_analysis[category] = {
                'fail_count': count,
                'percentage': count / len(failure_cases) if failure_cases else 0,
            }
        
        return category_analysis
    
    def _identify_patterns(self, failure_cases: List[SampleTestResult]) -> List[Dict]:
        """识别失败模式"""
        patterns = []
        
        # 模式1：铁律组合触发导致失败
        combo_failures = self._find_failure_combinations(failure_cases)
        if combo_failures:
            patterns.append({
                'pattern_type': 'rule_combination',
                'description': '某些铁律组合同时触发时失败率较高',
                'details': combo_failures,
                'severity': 'high',
            })
        
        # 模式2：高价位失败
        high_price_failures = [
            r for r in failure_cases 
            if r.trigger_price > 50
        ]
        if len(high_price_failures) > len(failure_cases) * 0.3:
            patterns.append({
                'pattern_type': 'high_price',
                'description': '高价股（>50元）失败率较高',
                'fail_count': len(high_price_failures),
                'severity': 'medium',
            })
        
        # 模式3：持仓时间不足
        short_term_failures = [
            r for r in failure_cases
            if r.holding_days <= 3
        ]
        if len(short_term_failures) > len(failure_cases) * 0.5:
            patterns.append({
                'pattern_type': 'insufficient_holding',
                'description': '短线操作（≤3天）失败率较高，可能需要延长持仓',
                'fail_count': len(short_term_failures),
                'severity': 'medium',
            })
        
        # 模式4：特定铁律误判
        misidentified = self._find_misidentified_rules(failure_cases)
        if misidentified:
            patterns.append({
                'pattern_type': 'misidentification',
                'description': '某些铁律存在误判情况',
                'details': misidentified,
                'severity': 'high',
            })
        
        return patterns
    
    def _find_failure_combinations(self, failure_cases: List[SampleTestResult]) -> List[Dict]:
        """查找导致失败的铁律组合"""
        from itertools import combinations
        
        combo_counts = Counter()
        
        for result in failure_cases:
            rules = sorted(result.triggered_rules)
            for r in rules:
                combo_counts[r] += 1
            for combo in combinations(rules, 2):
                combo_counts[combo] += 1
        
        # 找出高频失败组合
        high_freq = [
            {'rules': list(k), 'count': v}
            for k, v in combo_counts.most_common(10)
            if v >= len(failure_cases) * 0.1
        ]
        
        return high_freq
    
    def _find_misidentified_rules(self, failure_cases: List[SampleTestResult]) -> List[Dict]:
        """查找可能被误判的铁律"""
        misidentified = []
        
        for rule_id, stats in self._analyze_by_rule(failure_cases).items():
            if stats['fail_rate'] > 0.6:
                rule = self.rule_parser.get_rule(rule_id)
                misidentified.append({
                    'rule_id': rule_id,
                    'rule_name': rule.name if rule else '未知',
                    'expected_accuracy': rule.accuracy if rule else 0,
                    'actual_accuracy': stats['accuracy'],
                    'accuracy_gap': (rule.accuracy - stats['accuracy']) if rule else 0,
                    'issue': self._describe_accuracy_issue(rule, stats),
                })
        
        return misidentified
    
    def _describe_accuracy_issue(self, rule: Rule, stats: Dict) -> str:
        """描述准确率问题"""
        if rule is None:
            return "铁律信息不存在"
        
        gap = rule.accuracy - stats['accuracy']
        
        if gap > 0.3:
            return f"实际准确率({stats['accuracy']:.1%})远低于预期({rule.accuracy:.1%})，可能存在误判"
        elif gap > 0.15:
            return f"实际准确率({stats['accuracy']:.1%})低于预期({rule.accuracy:.1%})，需要优化"
        else:
            return "准确率基本符合预期"
    
    def _generate_recommendations(self, analysis: Dict) -> List[Dict]:
        """生成改进建议"""
        recommendations = []
        
        # 检查问题铁律
        by_rule = analysis.get('by_rule', {})
        problematic_rules = [
            (rule_id, stats) for rule_id, stats in by_rule.items()
            if stats.get('is_problematic', False)
        ]
        
        for rule_id, stats in problematic_rules:
            recommendations.append({
                'type': 'rule_adjustment',
                'target': rule_id,
                'priority': 'high',
                'action': '调整或暂停使用',
                'reason': f"失败率{stats['fail_rate']:.1%}过高",
                'suggestion': self._get_rule_adjustment_suggestion(rule_id, stats),
            })
        
        # 检查特定模式
        patterns = analysis.get('common_patterns', [])
        for pattern in patterns:
            if pattern.get('severity') == 'high':
                recommendations.append({
                    'type': 'pattern_avoidance',
                    'target': pattern['pattern_type'],
                    'priority': 'medium',
                    'action': '增加过滤条件',
                    'reason': pattern['description'],
                })
        
        # 通用建议
        if analysis['failure_rate'] > 0.5:
            recommendations.append({
                'type': 'general',
                'priority': 'high',
                'action': '整体降低仓位',
                'reason': f"整体失败率{analysis['failure_rate']:.1%}较高",
            })
        
        return recommendations
    
    def _get_rule_adjustment_suggestion(self, rule_id: str, stats: Dict) -> str:
        """获取铁律调整建议"""
        suggestions = {
            'S04': '增加条件：仅在主线板块+10:30前涨停时适用',
            'S07': '准确率偏低，建议仅作为辅助参考',
            'S08': '需结合市场环境使用，熊市效果更好',
            'S10': '需结合主力净流出比例判断',
            'N06': '市值条件建议调整为50-300亿',
            'N09': '10-50元区间正确，建议保持',
        }
        return suggestions.get(rule_id, '建议增加更多限制条件或暂时停用')
    
    def generate_failure_tree(self, failure_cases: List[SampleTestResult]) -> Dict:
        """生成失败原因归因树"""
        tree = {
            'root': {
                'name': '失败原因',
                'count': len(failure_cases),
                'children': []
            }
        }
        
        # 主分类
        categories = {
            '铁律问题': {'count': 0, 'children': []},
            '市场环境问题': {'count': 0, 'children': []},
            '数据问题': {'count': 0, 'children': []},
            '其他': {'count': 0, 'children': []},
        }
        
        for case in failure_cases:
            reason = self._classify_failure_reason(case)
            categories[reason['main_category']]['count'] += 1
            categories[reason['main_category']]['children'].append({
                'rule_id': case.triggered_rules[0] if case.triggered_rules else 'unknown',
                'sample_id': case.sample_id,
                'return_rate': case.return_rate,
                'reason': reason['detail'],
            })
        
        tree['root']['children'] = [
            {'name': k, **v} for k, v in categories.items()
        ]
        
        return tree
    
    def _classify_failure_reason(self, case: SampleTestResult) -> Dict:
        """分类失败原因"""
        # 简化分类逻辑
        if case.return_rate < -0.1:
            return {
                'main_category': '市场环境问题',
                'detail': '大幅亏损，市场整体下跌'
            }
        elif len(case.triggered_rules) > 3:
            return {
                'main_category': '铁律问题',
                'detail': '触发过多铁律，信号混乱'
            }
        elif case.holding_days <= 3:
            return {
                'main_category': '市场环境问题',
                'detail': '持仓时间不足，无法等待反弹'
            }
        else:
            return {
                'main_category': '铁律问题',
                'detail': '铁律信号误判'
            }
    
    def generate_analysis_report(self, analysis: Dict) -> str:
        """生成分析报告"""
        report = """
# 失败案例归因分析报告

## 总体统计

| 指标 | 值 |
|------|-----|
| 失败案例数 | {total} |
| 失败率 | {rate:.2%} |

## 问题铁律

| 铁律ID | 名称 | 类型 | 失败率 | 问题程度 |
|--------|------|------|--------|----------|
""".format(
            total=analysis['total_failures'],
            rate=analysis['failure_rate']
        )
        
        by_rule = analysis.get('by_rule', {})
        for rule_id, stats in sorted(
            by_rule.items(), 
            key=lambda x: x[1].get('fail_rate', 0), 
            reverse=True
        )[:10]:
            if stats.get('is_problematic', False):
                report += f"| {rule_id} | {stats['rule_name']} | {stats['rule_type']} | {stats['fail_rate']:.1%} | ⚠️问题 |\n"
        
        report += """
## 失败模式

"""
        patterns = analysis.get('common_patterns', [])
        for i, pattern in enumerate(patterns, 1):
            report += f"### {i}. {pattern['pattern_type']}\n\n"
            report += f"- **描述**: {pattern['description']}\n"
            report += f"- **严重程度**: {pattern.get('severity', 'unknown')}\n\n"
        
        report += """
## 改进建议

"""
        recommendations = analysis.get('recommendations', [])
        for i, rec in enumerate(recommendations, 1):
            report += f"### {i}. {rec['action']}\n\n"
            report += f"- **目标**: {rec.get('target', '通用')}\n"
            report += f"- **优先级**: {rec['priority']}\n"
            report += f"- **原因**: {rec['reason']}\n\n"
        
        return report
