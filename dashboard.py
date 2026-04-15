# -*- coding: utf-8 -*-
"""
监控仪表盘数据接口
提供系统监控和状态展示所需的数据
"""
from typing import List, Dict, Optional
from datetime import datetime, date, timedelta
from dataclasses import dataclass, field
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import DASHBOARD_CONFIG
from models.rule import Rule, RuleStatus
from models.result import ValidationReport, RuleStatistics
from utils.logger import get_logger

logger = get_logger("Dashboard")


@dataclass
class SystemStatus:
    """系统状态"""
    status: str                          # running/idle/error
    uptime_seconds: int = 0             # 运行时间
    last_validation_time: str = ""       # 上次验证时间
    total_validations: int = 0           # 总验证次数
    health_score: float = 0              # 健康度评分
    
    def to_dict(self) -> Dict:
        return {
            'status': self.status,
            'uptime_seconds': self.uptime_seconds,
            'last_validation_time': self.last_validation_time,
            'total_validations': self.total_validations,
            'health_score': round(self.health_score, 2),
        }


@dataclass
class TodayStats:
    """今日统计"""
    date: str
    new_samples: int = 0                # 新增样本数
    new_validations: int = 0            # 新验证数
    avg_accuracy: float = 0              # 平均准确率
    avg_return: float = 0               # 平均收益率
    
    def to_dict(self) -> Dict:
        return {
            'date': self.date,
            'new_samples': self.new_samples,
            'new_validations': self.new_validations,
            'avg_accuracy': round(self.avg_accuracy, 4),
            'avg_return': round(self.avg_return, 4),
        }


@dataclass
class Alert:
    """告警信息"""
    alert_id: str
    level: str                           # info/warning/error/critical
    title: str
    message: str
    timestamp: str
    rule_id: str = ""                    # 相关铁律（如果有）
    
    def to_dict(self) -> Dict:
        return {
            'alert_id': self.alert_id,
            'level': self.level,
            'title': self.title,
            'message': self.message,
            'timestamp': self.timestamp,
            'rule_id': self.rule_id,
        }


@dataclass
class RuleLibraryStatus:
    """铁律库状态"""
    total_rules: int = 0
    active_count: int = 0
    demoted_count: int = 0
    dormant_count: int = 0
    eliminated_count: int = 0
    avg_accuracy: float = 0
    
    def to_dict(self) -> Dict:
        return {
            'total_rules': self.total_rules,
            'active_count': self.active_count,
            'demoted_count': self.demoted_count,
            'dormant_count': self.dormant_count,
            'eliminated_count': self.eliminated_count,
            'avg_accuracy': round(self.avg_accuracy, 4),
            'active_ratio': self.active_count / self.total_rules if self.total_rules > 0 else 0,
        }


@dataclass
class DashboardData:
    """仪表盘数据"""
    system_status: SystemStatus
    today_stats: TodayStats
    rule_library_status: RuleLibraryStatus
    alerts: List[Alert]
    recent_validations: List[Dict]
    
    def to_dict(self) -> Dict:
        return {
            'system_status': self.system_status.to_dict(),
            'today_stats': self.today_stats.to_dict(),
            'rule_library_status': self.rule_library_status.to_dict(),
            'alerts': [a.to_dict() for a in self.alerts],
            'recent_validations': self.recent_validations,
            'generated_at': datetime.now().isoformat(),
        }


class DashboardProvider:
    """仪表盘数据提供者"""
    
    def __init__(self, config: Dict = None):
        """
        初始化提供者
        
        Args:
            config: 配置字典
        """
        self.config = config or DASHBOARD_CONFIG
        self.max_recent_items = self.config.get('max_recent_items', 10)
        self.alert_thresholds = self.config.get('alert_thresholds', {})
    
    def get_dashboard_data(
        self,
        validator=None,
        recent_reports: List[ValidationReport] = None
    ) -> DashboardData:
        """
        获取仪表盘数据
        
        Args:
            validator: 验证器实例
            recent_reports: 最近的验证报告
        
        Returns:
            仪表盘数据
        """
        # 1. 获取系统状态
        system_status = self._get_system_status(validator)
        
        # 2. 获取今日统计
        today_stats = self._get_today_stats(recent_reports)
        
        # 3. 获取铁律库状态
        rule_library_status = self._get_rule_library_status(validator)
        
        # 4. 生成告警
        alerts = self._generate_alerts(
            validator, recent_reports, rule_library_status
        )
        
        # 5. 获取最近验证
        recent_validations = self._get_recent_validations(recent_reports)
        
        return DashboardData(
            system_status=system_status,
            today_stats=today_stats,
            rule_library_status=rule_library_status,
            alerts=alerts,
            recent_validations=recent_validations,
        )
    
    def _get_system_status(self, validator=None) -> SystemStatus:
        """获取系统状态"""
        status = SystemStatus(
            status='idle',
            uptime_seconds=0,
            last_validation_time='',
            total_validations=0,
            health_score=75.0,  # 默认健康度
        )
        
        if validator is not None:
            # 从验证器获取实际状态
            status.status = 'running'
        
        return status
    
    def _get_today_stats(
        self,
        reports: List[ValidationReport] = None
    ) -> TodayStats:
        """获取今日统计"""
        today = date.today()
        stats = TodayStats(date=today.isoformat())
        
        if not reports:
            return stats
        
        # 统计今天的报告
        today_reports = []
        for r in reports:
            try:
                report_date = datetime.fromisoformat(r.report_id[:8]).date()
                if report_date == today:
                    today_reports.append(r)
            except:
                continue
        
        if today_reports:
            stats.new_validations = len(today_reports)
            accuracies = [r.overall_accuracy for r in today_reports if hasattr(r, 'overall_accuracy')]
            returns = [r.avg_return for r in today_reports if hasattr(r, 'avg_return')]
            
            if accuracies:
                stats.avg_accuracy = sum(accuracies) / len(accuracies)
            if returns:
                stats.avg_return = sum(returns) / len(returns)
        
        return stats
    
    def _get_rule_library_status(
        self,
        validator=None
    ) -> RuleLibraryStatus:
        """获取铁律库状态"""
        status = RuleLibraryStatus()
        
        if validator is None or not hasattr(validator, 'rule_parser'):
            return status
        
        parser = validator.rule_parser
        rules = parser.rules if hasattr(parser, 'rules') else []
        
        status.total_rules = len(rules)
        
        # 统计各状态数量
        for rule in rules:
            if rule.status == RuleStatus.ACTIVE:
                status.active_count += 1
            elif rule.status == RuleStatus.DEMOTED:
                status.demoted_count += 1
            elif rule.status == RuleStatus.DORMANT:
                status.dormant_count += 1
            elif rule.status == RuleStatus.ELIMINATED:
                status.eliminated_count += 1
        
        # 计算平均准确率
        accuracies = [r.accuracy for r in rules if r.accuracy > 0]
        if accuracies:
            status.avg_accuracy = sum(accuracies) / len(accuracies)
        
        return status
    
    def _generate_alerts(
        self,
        validator,
        reports: List[ValidationReport],
        rule_status: RuleLibraryStatus
    ) -> List[Alert]:
        """生成告警"""
        alerts = []
        
        # 1. 低准确率告警
        low_accuracy = self.alert_thresholds.get('low_accuracy', 0.55)
        if rule_status.avg_accuracy < low_accuracy:
            alerts.append(Alert(
                alert_id='low_accuracy',
                level='warning',
                title='铁律库平均准确率偏低',
                message=f'当前平均准确率{rule_status.avg_accuracy:.1%}，低于阈值{low_accuracy:.1%}',
                timestamp=datetime.now().isoformat(),
            ))
        
        # 2. 活跃铁律过少告警
        if rule_status.total_rules > 0:
            active_ratio = rule_status.active_count / rule_status.total_rules
            if active_ratio < 0.5:
                alerts.append(Alert(
                    alert_id='low_active_ratio',
                    level='info',
                    title='活跃铁律比例偏低',
                    message=f'活跃铁律占比{active_ratio:.1%}，建议关注',
                    timestamp=datetime.now().isoformat(),
                ))
        
        # 3. 检查是否有降级铁律
        if rule_status.demoted_count > 0:
            alerts.append(Alert(
                alert_id='demoted_rules',
                level='info',
                title=f'{rule_status.demoted_count}条铁律已降级',
                message='部分铁律准确率下降，建议进行分析',
                timestamp=datetime.now().isoformat(),
            ))
        
        # 4. 检查是否有休眠铁律
        if rule_status.dormant_count > 0:
            alerts.append(Alert(
                alert_id='dormant_rules',
                level='warning',
                title=f'{rule_status.dormant_count}条铁律已休眠',
                message='部分铁律准确率过低，建议暂停使用',
                timestamp=datetime.now().isoformat(),
            ))
        
        return alerts
    
    def _get_recent_validations(
        self,
        reports: List[ValidationReport] = None
    ) -> List[Dict]:
        """获取最近验证记录"""
        recent = []
        
        if reports:
            # 按报告ID排序（假设ID包含时间戳）
            sorted_reports = sorted(
                reports,
                key=lambda x: x.report_id,
                reverse=True
            )[:self.max_recent_items]
            
            for r in sorted_reports:
                recent.append({
                    'report_id': r.report_id,
                    'sample_count': r.sample_size,
                    'accuracy': r.overall_accuracy if hasattr(r, 'overall_accuracy') else 0,
                    'timestamp': r.report_id[:8] if len(r.report_id) >= 8 else '',
                })
        
        return recent


def get_dashboard_data(
    validator=None,
    recent_reports: List[ValidationReport] = None
) -> DashboardData:
    """
    获取仪表盘数据的便捷函数
    
    Args:
        validator: 验证器实例
        recent_reports: 最近的验证报告
    
    Returns:
        仪表盘数据
    """
    provider = DashboardProvider()
    return provider.get_dashboard_data(validator, recent_reports)
