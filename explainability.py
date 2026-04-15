# -*- coding: utf-8 -*-
"""
铁律可解释性增强模块
为每条铁律生成人类可读的验证报告和决策解释
"""
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleType, RuleStatus, TermType
from utils.logger import get_logger

logger = get_logger("Explainability")


@dataclass
class DecisionFactor:
    """决策因素"""
    factor_name: str
    factor_value: Any
    contribution: float              # 贡献度 (-1到1)
    explanation: str                # 解释文本
    importance: str                 # high/medium/low


@dataclass
class RiskIndicator:
    """风险指标"""
    risk_type: str                  # 风险类型
    risk_level: str                 # high/medium/low
    description: str
    mitigation: str = ""            # 缓解措施


@dataclass
class RuleExplanation:
    """铁律解释"""
    rule_id: str
    rule_name: str
    generated_at: str
    
    # 总体评价
    overall_rating: str             # excellent/good/fair/poor
    confidence: float               # 置信度 0-1
    
    # 决策因素
    triggering_factors: List[DecisionFactor] = field(default_factory=list)
    blocking_factors: List[DecisionFactor] = field(default_factory=list)
    
    # 风险提示
    risks: List[RiskIndicator] = field(default_factory=list)
    
    # 历史表现
    historical_performance: Dict[str, Any] = field(default_factory=dict)
    
    # 适用条件
    applicable_conditions: List[str] = field(default_factory=list)
    exclusion_conditions: List[str] = field(default_factory=list)
    
    # 建议
    suggestions: List[str] = field(default_factory=list)
    
    def to_markdown(self) -> str:
        """转换为Markdown格式"""
        lines = [
            f"# 铁律解释报告: {self.rule_id} - {self.rule_name}",
            f"\n**生成时间**: {self.generated_at}",
            f"\n---\n",
            f"\n## 总体评价",
            f"\n- **评级**: {self._get_rating_emoji()} {self.overall_rating.upper()}",
            f"- **置信度**: {self.confidence:.1%}",
            f"\n---\n",
        ]
        
        # 触发因素
        if self.triggering_factors:
            lines.append("\n## ✅ 触发因素\n")
            lines.append("| 因素 | 值 | 贡献度 | 解释 | 重要性 |")
            lines.append("|------|-----|--------|------|--------|")
            for factor in sorted(self.triggering_factors, key=lambda x: abs(x.contribution), reverse=True):
                lines.append(
                    f"| {factor.factor_name} | {factor.factor_value} | "
                    f"{factor.contribution:+.1%} | {factor.explanation} | "
                    f"{factor.importance.upper()} |"
                )
        
        # 阻碍因素
        if self.blocking_factors:
            lines.append("\n## ❌ 阻碍因素\n")
            lines.append("| 因素 | 值 | 贡献度 | 解释 | 重要性 |")
            lines.append("|------|-----|--------|------|--------|")
            for factor in sorted(self.blocking_factors, key=lambda x: abs(x.contribution), reverse=True):
                lines.append(
                    f"| {factor.factor_name} | {factor.factor_value} | "
                    f"{factor.contribution:+.1%} | {factor.explanation} | "
                    f"{factor.importance.upper()} |"
                )
        
        # 风险提示
        if self.risks:
            lines.append("\n## ⚠️ 风险提示\n")
            for risk in self.risks:
                emoji = {"high": "🔴", "medium": "🟡", "low": "🟢"}.get(risk.risk_level, "⚪")
                lines.append(f"\n{emoji} **{risk.risk_type}** ({risk.risk_level.upper()})")
                lines.append(f"\n{risk.description}")
                if risk.mitigation:
                    lines.append(f"\n缓解措施: {risk.mitigation}")
        
        # 适用条件
        if self.applicable_conditions:
            lines.append("\n## 📋 适用条件\n")
            for cond in self.applicable_conditions:
                lines.append(f"- {cond}")
        
        # 排除条件
        if self.exclusion_conditions:
            lines.append("\n## 🚫 排除条件\n")
            for cond in self.exclusion_conditions:
                lines.append(f"- {cond}")
        
        # 历史表现
        if self.historical_performance:
            lines.append("\n## 📊 历史表现\n")
            for key, value in self.historical_performance.items():
                if isinstance(value, float):
                    lines.append(f"- **{key}**: {value:.2%}")
                else:
                    lines.append(f"- **{key}**: {value}")
        
        # 建议
        if self.suggestions:
            lines.append("\n## 💡 建议\n")
            for i, suggestion in enumerate(self.suggestions, 1):
                lines.append(f"{i}. {suggestion}")
        
        return "\n".join(lines)
    
    def _get_rating_emoji(self) -> str:
        """获取评级表情"""
        return {
            "excellent": "🌟🌟🌟🌟🌟",
            "good": "🌟🌟🌟🌟",
            "fair": "🌟🌟🌟",
            "poor": "🌟🌟",
        }.get(self.overall_rating, "⭐")


@dataclass
class ValidationExplanation:
    """验证解释"""
    validation_id: str
    timestamp: str
    sample_count: int
    overall_accuracy: float
    explanations: List[RuleExplanation] = field(default_factory=list)
    
    def to_markdown(self) -> str:
        """转换为Markdown格式"""
        lines = [
            f"# 铁律验证解释报告",
            f"\n**验证ID**: {self.validation_id}",
            f"**时间**: {self.timestamp}",
            f"**样本数**: {self.sample_count}",
            f"**总体准确率**: {self.overall_accuracy:.2%}",
            "\n---\n",
        ]
        
        # 按评级分组
        by_rating = defaultdict(list)
        for exp in self.explanations:
            by_rating[exp.overall_rating].append(exp)
        
        for rating in ["excellent", "good", "fair", "poor"]:
            if rating in by_rating:
                lines.append(f"\n## {self._format_rating(rating)} ({len(by_rating[rating])}条)\n")
                for exp in by_rating[rating]:
                    lines.append(f"\n### {exp.rule_id}: {exp.rule_name}")
                    lines.append(f"\n置信度: {exp.confidence:.1%}")
                    if exp.triggering_factors:
                        factors = ", ".join([f.factor_name for f in exp.triggering_factors[:3]])
                        lines.append(f"\n触发因素: {factors}")
                    if exp.risks:
                        lines.append(f"\n风险: {', '.join([r.risk_type for r in exp.risks])}")
        
        return "\n".join(lines)
    
    def _format_rating(self, rating: str) -> str:
        """格式化评级"""
        return {
            "excellent": "🌟🌟🌟🌟🌟 优秀",
            "good": "🌟🌟🌟🌟 良好",
            "fair": "🌟🌟🌟 一般",
            "poor": "🌟🌟 较差",
        }.get(rating, rating.upper())


class ExplainabilityEngine:
    """可解释性引擎"""
    
    def __init__(self):
        """初始化可解释性引擎"""
        # 指标到中文名称的映射
        self.indicator_names = {
            'macd': 'MACD指标',
            'kdj': 'KDJ指标',
            'rsi': 'RSI相对强弱指标',
            'ma5': '5日均线',
            'ma10': '10日均线',
            'ma20': '20日均线',
            'volume': '成交量',
            'turnover': '换手率',
            '主力净流入': '主力资金净流入',
            '北向资金': '北向资金',
            'boll': '布林带指标',
        }
        
        # 风险类型映射
        self.risk_types = {
            'high_volatility': '高波动风险',
            'low_liquidity': '低流动性风险',
            'contrarian': '逆向操作风险',
            'late_signal': '信号滞后风险',
            'overfitting': '过拟合风险',
        }
    
    def explain_rule(
        self,
        rule: Rule,
        market_data: Dict[str, Any] = None,
        validation_history: List[Dict] = None,
    ) -> RuleExplanation:
        """
        为单条铁律生成解释
        
        Args:
            rule: 铁律
            market_data: 市场数据
            validation_history: 验证历史
        
        Returns:
            铁律解释
        """
        explanation = RuleExplanation(
            rule_id=rule.id,
            rule_name=rule.name,
            generated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        
        # 1. 确定总体评级
        explanation.overall_rating, explanation.confidence = self._evaluate_overall(
            rule, validation_history
        )
        
        # 2. 分析决策因素
        explanation.triggering_factors, explanation.blocking_factors = self._analyze_factors(
            rule, market_data
        )
        
        # 3. 生成风险提示
        explanation.risks = self._identify_risks(rule, market_data)
        
        # 4. 生成历史表现
        explanation.historical_performance = self._summarize_history(rule, validation_history)
        
        # 5. 生成适用/排除条件
        explanation.applicable_conditions, explanation.exclusion_conditions = self._generate_conditions(
            rule, validation_history
        )
        
        # 6. 生成建议
        explanation.suggestions = self._generate_suggestions(rule, explanation)
        
        return explanation
    
    def _evaluate_overall(
        self,
        rule: Rule,
        history: List[Dict] = None,
    ) -> Tuple[str, float]:
        """评估总体评级"""
        accuracy = rule.get_current_accuracy()
        
        # 计算置信度（基于样本数）
        if rule.total_tests < 10:
            confidence = 0.3
        elif rule.total_tests < 30:
            confidence = 0.5
        elif rule.total_tests < 50:
            confidence = 0.7
        else:
            confidence = min(0.9, 0.5 + rule.total_tests / 200)
        
        # 根据准确率确定评级
        if accuracy >= 0.85:
            rating = "excellent"
            confidence = min(confidence + 0.05, 0.95)
        elif accuracy >= 0.75:
            rating = "good"
        elif accuracy >= 0.60:
            rating = "fair"
            confidence = max(confidence - 0.1, 0.3)
        else:
            rating = "poor"
            confidence = max(confidence - 0.2, 0.2)
        
        return rating, confidence
    
    def _analyze_factors(
        self,
        rule: Rule,
        market_data: Dict[str, Any] = None,
    ) -> Tuple[List[DecisionFactor], List[DecisionFactor]]:
        """分析决策因素"""
        triggering = []
        blocking = []
        
        if not market_data:
            return triggering, blocking
        
        # 分析各指标
        for indicator, value in market_data.items():
            indicator_name = self.indicator_names.get(indicator, indicator)
            
            # MACD相关
            if 'macd' in indicator.lower():
                if value > 0:
                    triggering.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"MACD={value:.4f}",
                        contribution=0.15,
                        explanation="MACD处于零轴上方，动能偏强",
                        importance="high",
                    ))
                elif value < 0:
                    blocking.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"MACD={value:.4f}",
                        contribution=-0.15,
                        explanation="MACD处于零轴下方，动能偏弱",
                        importance="high",
                    ))
            
            # RSI相关
            elif 'rsi' in indicator.lower():
                if value < 30:
                    triggering.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"RSI={value:.1f}",
                        contribution=0.2,
                        explanation="RSI处于超卖区域，可能存在反弹机会",
                        importance="high",
                    ))
                elif value > 70:
                    blocking.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"RSI={value:.1f}",
                        contribution=-0.15,
                        explanation="RSI处于超买区域，可能面临回调压力",
                        importance="medium",
                    ))
            
            # KDJ相关
            elif 'kdj' in indicator.lower():
                if value < 20:
                    triggering.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"KDJ={value:.1f}",
                        contribution=0.15,
                        explanation="KDJ处于低位，金叉信号较强",
                        importance="medium",
                    ))
            
            # 成交量相关
            elif 'volume' in indicator.lower() or '换手' in indicator_name:
                if value > 1.5:
                    triggering.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"量比={value:.2f}",
                        contribution=0.1,
                        explanation="成交量明显放大，市场关注度高",
                        importance="medium",
                    ))
            
            # 主力资金
            elif '主力' in indicator_name or '净流入' in indicator:
                if value > 0:
                    triggering.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"净流入={value:.2f}亿",
                        contribution=0.25,
                        explanation="主力资金净流入，看涨信号较强",
                        importance="high",
                    ))
                else:
                    blocking.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"净流入={value:.2f}亿",
                        contribution=-0.25,
                        explanation="主力资金净流出，风险较大",
                        importance="high",
                    ))
            
            # 北向资金
            elif '北向' in indicator_name:
                if value > 0:
                    triggering.append(DecisionFactor(
                        factor_name=indicator_name,
                        factor_value=f"净买入={value:.2f}亿",
                        contribution=0.2,
                        explanation="北向资金净买入，跟随外资信号",
                        importance="high",
                    ))
        
        return triggering, blocking
    
    def _identify_risks(
        self,
        rule: Rule,
        market_data: Dict[str, Any] = None,
    ) -> List[RiskIndicator]:
        """识别风险"""
        risks = []
        
        # 准确率风险
        accuracy = rule.get_current_accuracy()
        if accuracy < 0.60:
            risks.append(RiskIndicator(
                risk_type=self.risk_types['overfitting'],
                risk_level="high",
                description=f"历史准确率偏低({accuracy:.1%})，可能存在过拟合或适用条件不明确",
                mitigation="建议增加验证样本或细化适用条件",
            ))
        
        # 样本量风险
        if rule.total_tests < 30:
            risks.append(RiskIndicator(
                risk_type="小样本风险",
                risk_level="medium",
                description=f"验证样本数较少({rule.total_tests})，结果可能不够稳定",
                mitigation="建议积累更多样本后再做决策",
            ))
        
        # 周期匹配风险
        if market_data:
            # 检查铁律周期与持仓周期是否匹配
            holding_days = market_data.get('holding_days', 3)
            if rule.term_type == TermType.SHORT and holding_days > 7:
                risks.append(RiskIndicator(
                    risk_type="周期错配风险",
                    risk_level="medium",
                    description="短线铁律应用于较长期持仓，可能效果不佳",
                    mitigation="考虑使用中线或长线铁律",
                ))
        
        return risks
    
    def _summarize_history(
        self,
        rule: Rule,
        history: List[Dict] = None,
    ) -> Dict[str, Any]:
        """总结历史表现"""
        summary = {
            "总测试次数": rule.total_tests,
            "成功次数": rule.successful_tests,
            "失败次数": rule.failed_tests,
            "当前准确率": rule.get_current_accuracy(),
            "初始准确率": rule.accuracy,
            "状态": rule.status.value,
        }
        
        # 分析趋势
        if history and len(history) >= 3:
            recent = history[-5:]
            accuracies = [h.get('accuracy', 0) for h in recent]
            
            if all(accuracies[i] >= accuracies[i+1] for i in range(len(accuracies)-1)):
                summary["趋势"] = "📉 下降"
            elif all(accuracies[i] <= accuracies[i+1] for i in range(len(accuracies)-1)):
                summary["趋势"] = "📈 上升"
            else:
                summary["趋势"] = "➡️ 稳定"
        
        # 添加最近表现
        if rule.test_history:
            recent = rule.test_history[-5:]
            summary["最近5次"] = f"{sum(1 for h in recent if h.get('success'))}/{len(recent)}"
        
        return summary
    
    def _generate_conditions(
        self,
        rule: Rule,
        history: List[Dict] = None,
    ) -> Tuple[List[str], List[str]]:
        """生成适用和排除条件"""
        applicable = []
        excluded = []
        
        # 基于铁律类型生成条件
        if rule.rule_type == RuleType.BUY_SIGNAL:
            applicable.append("市场处于上升趋势")
            applicable.append("成交量温和放大")
            applicable.append("有主力资金持续流入")
        
        if rule.rule_type == RuleType.SELL_SIGNAL:
            applicable.append("持有盈利超过5%")
            applicable.append("出现放量滞涨")
            applicable.append("主力资金开始流出")
        
        if rule.rule_type == RuleType.FORBIDDEN:
            excluded.append("跌停板开盘")
            excluded.append("连续下跌超过20%")
            excluded.append("重大利空公告")
        
        # 基于历史分析补充
        if history:
            # 找出成功率高的市场条件
            success_by_condition = defaultdict(lambda: {'success': 0, 'total': 0})
            for h in history:
                condition = h.get('market_condition', 'unknown')
                success_by_condition[condition]['total'] += 1
                if h.get('success'):
                    success_by_condition[condition]['success'] += 1
            
            for condition, stats in success_by_condition.items():
                if stats['total'] >= 3:
                    rate = stats['success'] / stats['total']
                    if rate >= 0.8:
                        applicable.append(f"{condition}（成功率{rate:.0%}）")
                    elif rate <= 0.3:
                        excluded.append(f"{condition}（成功率仅{rate:.0%}）")
        
        return applicable[:5], excluded[:5]  # 限制数量
    
    def _generate_suggestions(
        self,
        rule: Rule,
        explanation: RuleExplanation,
    ) -> List[str]:
        """生成建议"""
        suggestions = []
        
        # 基于评级建议
        if explanation.overall_rating == "poor":
            suggestions.append("准确率较低，建议增加验证样本或优化铁律条件")
            suggestions.append("考虑暂时禁用该铁律，等待更多数据支持")
        
        elif explanation.overall_rating == "fair":
            suggestions.append("准确率一般，建议在使用时配合其他确认信号")
            suggestions.append("持续监控该铁律的表现，等待数据积累")
        
        elif explanation.overall_rating == "good":
            suggestions.append("准确率良好，可以作为常规信号参考")
            suggestions.append("建议与其他铁律组合使用以提高可靠性")
        
        else:  # excellent
            suggestions.append("准确率优秀，可以作为核心信号")
            suggestions.append("但仍需注意市场风险，建议设置止损位")
        
        # 基于风险建议
        for risk in explanation.risks:
            if risk.risk_type == self.risk_types['high_volatility']:
                suggestions.append("当前波动较大，建议降低仓位或等待稳定后再操作")
            elif risk.risk_type == self.risk_types['late_signal']:
                suggestions.append("信号可能存在滞后，建议结合其他领先指标")
        
        # 基于趋势建议
        if explanation.historical_performance.get("趋势") == "📉 下降":
            suggestions.append("准确率呈下降趋势，需要分析原因并考虑调整")
        
        return suggestions[:3]  # 限制建议数量
    
    def explain_validation(
        self,
        validation_report: Any,
        rulesets: List[RuleSet],
    ) -> ValidationExplanation:
        """
        解释验证结果
        
        Args:
            validation_report: 验证报告
            rulesets: 铁律集合列表
        
        Returns:
            验证解释
        """
        explanation = ValidationExplanation(
            validation_id=validation_report.report_id if hasattr(validation_report, 'report_id') else "unknown",
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            sample_count=validation_report.summary.get('sample_size', 0) if hasattr(validation_report, 'summary') else 0,
            overall_accuracy=validation_report.summary.get('overall_accuracy', 0) if hasattr(validation_report, 'summary') else 0,
        )
        
        # 为每条铁律生成解释
        for rs in rulesets:
            for rule in rs.rules:
                rule_exp = self.explain_rule(rule)
                explanation.explanations.append(rule_exp)
        
        return explanation
    
    def generate_comparison_report(
        self,
        rule1: Rule,
        rule2: Rule,
    ) -> str:
        """
        生成两条铁律的对比报告
        
        Args:
            rule1: 铁律1
            rule2: 铁律2
        
        Returns:
            对比报告
        """
        exp1 = self.explain_rule(rule1)
        exp2 = self.explain_rule(rule2)
        
        lines = [
            "# 铁律对比报告",
            f"\n## {rule1.id} vs {rule2.id}",
            "\n---\n",
            "\n| 指标 | {0} | {1} |".format(rule1.id, rule2.id),
            "|------|-----|-----|",
            f"| 名称 | {rule1.name} | {rule2.name} |",
            f"| 准确率 | {rule1.get_current_accuracy():.1%} | {rule2.get_current_accuracy():.1%} |",
            f"| 评级 | {exp1.overall_rating} | {exp2.overall_rating} |",
            f"| 置信度 | {exp1.confidence:.1%} | {exp2.confidence:.1%} |",
            f"| 触发因素 | {len(exp1.triggering_factors)} | {len(exp2.triggering_factors)} |",
            f"| 风险提示 | {len(exp1.risks)} | {len(exp2.risks)} |",
            f"| 测试次数 | {rule1.total_tests} | {rule2.total_tests} |",
        ]
        
        # 优劣势分析
        lines.append("\n## 优劣势分析\n")
        
        if rule1.get_current_accuracy() > rule2.get_current_accuracy():
            lines.append(f"\n**{rule1.id}优势**: 准确率更高")
        else:
            lines.append(f"\n**{rule2.id}优势**: 准确率更高")
        
        if exp1.confidence > exp2.confidence:
            lines.append(f"**{rule1.id}优势**: 置信度更高")
        else:
            lines.append(f"**{rule2.id}优势**: 置信度更高")
        
        return "\n".join(lines)


def integrate_with_validator(validator):
    """
    将可解释性模块集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    engine = ExplainabilityEngine()
    
    validator.explainability_engine = engine
    validator.explain_rule = engine.explain_rule
    validator.explain_validation = engine.explain_validation
    validator.generate_comparison_report = engine.generate_comparison_report
    
    return validator
