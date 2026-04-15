# -*- coding: utf-8 -*-
"""
铁律反馈学习机制
主动学习弱项铁律，定向抽样验证，动态调整策略
"""
from pathlib import Path
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple, Any, Set
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleStatus, RuleType
from utils.logger import get_logger

logger = get_logger("FeedbackLearning")


class LearningPriority(Enum):
    """学习优先级"""
    CRITICAL = 1    # 关键（准确率<50%）
    HIGH = 2        # 高（准确率50-60%）
    MEDIUM = 3      # 中（准确率60-70%）
    LOW = 4         # 低（准确率>70%）


class LearningStatus(Enum):
    """学习状态"""
    PENDING = "pending"           # 待处理
    IN_PROGRESS = "in_progress"   # 进行中
    COMPLETED = "completed"       # 完成
    FAILED = "failed"             # 失败


@dataclass
class LearningTask:
    """学习任务"""
    task_id: str
    rule_id: str
    rule_name: str
    priority: LearningPriority
    status: LearningStatus
    target_samples: int           # 目标样本数
    completed_samples: int = 0     # 已完成样本数
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now().isoformat())
    reason: str = ""               # 原因说明
    suggested_conditions: List[Dict] = field(default_factory=list)  # 建议的适用条件
    
    def progress(self) -> float:
        """获取进度"""
        if self.target_samples == 0:
            return 0.0
        return self.completed_samples / self.target_samples
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'task_id': self.task_id,
            'rule_id': self.rule_id,
            'rule_name': self.rule_name,
            'priority': self.priority.name,
            'status': self.status.value,
            'target_samples': self.target_samples,
            'completed_samples': self.completed_samples,
            'progress': f"{self.progress():.1%}",
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'reason': self.reason,
            'suggested_conditions': self.suggested_conditions,
        }


@dataclass
class LearningQueue:
    """学习队列"""
    queue_id: str
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    tasks: List[LearningTask] = field(default_factory=list)
    total_tasks: int = 0
    completed_tasks: int = 0
    
    def add_task(self, task: LearningTask):
        """添加任务"""
        self.tasks.append(task)
        self.tasks.sort(key=lambda x: (x.priority.value, x.created_at))
        self.total_tasks += 1
    
    def get_next_task(self) -> Optional[LearningTask]:
        """获取下一个待处理任务"""
        pending_tasks = [t for t in self.tasks if t.status == LearningStatus.PENDING]
        return pending_tasks[0] if pending_tasks else None
    
    def update_task(self, task_id: str, **kwargs):
        """更新任务"""
        for task in self.tasks:
            if task.task_id == task_id:
                for key, value in kwargs.items():
                    if hasattr(task, key):
                        setattr(task, key, value)
                task.updated_at = datetime.now().isoformat()
                break
    
    def get_queue_stats(self) -> Dict:
        """获取队列统计"""
        status_counts = defaultdict(int)
        priority_counts = defaultdict(int)
        
        for task in self.tasks:
            status_counts[task.status.value] += 1
            priority_counts[task.priority.name] += 1
        
        return {
            'queue_id': self.queue_id,
            'total_tasks': self.total_tasks,
            'completed_tasks': self.completed_tasks,
            'pending_tasks': status_counts.get('pending', 0),
            'in_progress_tasks': status_counts.get('in_progress', 0),
            'status_distribution': dict(status_counts),
            'priority_distribution': dict(priority_counts),
        }


@dataclass
class RulePerformanceRecord:
    """铁律表现记录"""
    rule_id: str
    accuracy_history: List[float] = field(default_factory=list)
    sample_count_history: List[int] = field(default_factory=list)
    failure_patterns: List[Dict] = field(default_factory=list)
    success_patterns: List[Dict] = field(default_factory=list)
    last_updated: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def add_sample(self, accuracy: float, sample_count: int):
        """添加样本记录"""
        self.accuracy_history.append(accuracy)
        self.sample_count_history.append(sample_count)
        self.last_updated = datetime.now().isoformat()
    
    def is_stable(self, window: int = 5, threshold: float = 0.05) -> bool:
        """判断准确率是否稳定"""
        if len(self.accuracy_history) < window:
            return False
        
        recent = self.accuracy_history[-window:]
        variance = sum((x - sum(recent)/len(recent))**2 for x in recent) / len(recent)
        return variance < threshold
    
    def get_trend(self) -> str:
        """获取准确率趋势"""
        if len(self.accuracy_history) < 3:
            return "unknown"
        
        recent = self.accuracy_history[-3:]
        if all(recent[i] < recent[i+1] for i in range(len(recent)-1)):
            return "improving"
        elif all(recent[i] > recent[i+1] for i in range(len(recent)-1)):
            return "declining"
        return "stable"


class FeedbackLearning:
    """铁律反馈学习机制"""
    
    def __init__(self, storage_dir: str = None):
        """
        初始化反馈学习系统
        
        Args:
            storage_dir: 存储目录
        """
        if storage_dir is None:
            from config import PATH_CONFIG
            storage_dir = str(Path(PATH_CONFIG['project_root']) / 'data' / 'learning')
        
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # 存储文件
        self.tasks_file = self.storage_dir / 'learning_tasks.json'
        self.performance_file = self.storage_dir / 'rule_performance.json'
        self.queue_file = self.storage_dir / 'learning_queue.json'
        self.progress_file = self.storage_dir / 'learning_progress.json'
        
        # 学习队列
        self.learning_queue = None
        self.performance_records: Dict[str, RulePerformanceRecord] = {}
        self.learning_history: List[Dict] = []
        
        # 阈值配置
        self.config = {
            'critical_threshold': 0.50,     # 关键阈值
            'high_threshold': 0.60,        # 高优先级阈值
            'medium_threshold': 0.70,       # 中优先级阈值
            'min_samples_for_eval': 10,    # 评估最小样本数
            'target_samples_critical': 50,  # 关键铁律目标样本
            'target_samples_high': 30,      # 高优先级目标样本
            'target_samples_medium': 20,     # 中优先级目标样本
            'stability_window': 5,          # 稳定性评估窗口
            'improvement_threshold': 0.05,  # 改进阈值
        }
        
        self._load_data()
    
    def _load_data(self):
        """加载数据"""
        # 加载性能记录
        if self.performance_file.exists():
            try:
                with open(self.performance_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.performance_records = {
                        k: RulePerformanceRecord(**v) for k, v in data.items()
                    }
            except Exception as e:
                logger.error(f"加载性能记录失败: {e}")
        
        # 加载学习队列
        if self.queue_file.exists():
            try:
                with open(self.queue_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.learning_queue = LearningQueue(
                        queue_id=data['queue_id'],
                        created_at=data['created_at'],
                        total_tasks=data['total_tasks'],
                        completed_tasks=data['completed_tasks'],
                    )
            except Exception as e:
                logger.error(f"加载学习队列失败: {e}")
    
    def _save_data(self):
        """保存数据"""
        try:
            # 保存性能记录
            with open(self.performance_file, 'w', encoding='utf-8') as f:
                json.dump({
                    k: {
                        'rule_id': v.rule_id,
                        'accuracy_history': v.accuracy_history,
                        'sample_count_history': v.sample_count_history,
                        'failure_patterns': v.failure_patterns,
                        'success_patterns': v.success_patterns,
                        'last_updated': v.last_updated,
                    } for k, v in self.performance_records.items()
                }, f, ensure_ascii=False, indent=2)
            
            # 保存学习队列
            if self.learning_queue:
                with open(self.queue_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'queue_id': self.learning_queue.queue_id,
                        'created_at': self.learning_queue.created_at,
                        'total_tasks': self.learning_queue.total_tasks,
                        'completed_tasks': self.learning_queue.completed_tasks,
                        'tasks': [t.to_dict() for t in self.learning_queue.tasks],
                    }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存数据失败: {e}")
    
    def _generate_task_id(self) -> str:
        """生成任务ID"""
        return f"learn_{datetime.now().strftime('%Y%m%d%H%M%S')}_{len(self.learning_history)}"
    
    def identify_weak_rules(
        self,
        rulesets: List[RuleSet],
        min_samples: int = None,
    ) -> List[Tuple[Rule, float]]:
        """
        识别弱项铁律
        
        Args:
            rulesets: 铁律集合列表
            min_samples: 最小样本数
        
        Returns:
            (铁律, 评分) 列表，按评分升序
        """
        if min_samples is None:
            min_samples = self.config['min_samples_for_eval']
        
        weak_rules = []
        
        for rs in rulesets:
            for rule in rs.rules:
                # 跳过样本不足的铁律
                if rule.total_tests < min_samples:
                    continue
                
                # 跳过已淘汰的铁律
                if rule.status == RuleStatus.ELIMINATED:
                    continue
                
                # 计算评分（考虑准确率和样本数）
                accuracy = rule.get_current_accuracy()
                sample_score = min(rule.total_tests / 100, 1.0)  # 样本数归一化
                score = accuracy * 0.8 + sample_score * 0.2
                
                # 只返回低于中高阈值的铁律
                if accuracy < self.config['medium_threshold']:
                    weak_rules.append((rule, score))
        
        # 按评分升序排列
        weak_rules.sort(key=lambda x: x[1])
        return weak_rules
    
    def calculate_learning_priority(self, rule: Rule) -> LearningPriority:
        """
        计算学习优先级
        
        Args:
            rule: 铁律
        
        Returns:
            优先级
        """
        accuracy = rule.get_current_accuracy()
        
        if accuracy < self.config['critical_threshold']:
            return LearningPriority.CRITICAL
        elif accuracy < self.config['high_threshold']:
            return LearningPriority.HIGH
        elif accuracy < self.config['medium_threshold']:
            return LearningPriority.MEDIUM
        return LearningPriority.LOW
    
    def get_target_samples(self, priority: LearningPriority) -> int:
        """获取目标样本数"""
        if priority == LearningPriority.CRITICAL:
            return self.config['target_samples_critical']
        elif priority == LearningPriority.HIGH:
            return self.config['target_samples_high']
        elif priority == LearningPriority.MEDIUM:
            return self.config['target_samples_medium']
        return 10
    
    def build_learning_queue(
        self,
        rulesets: List[RuleSet],
        focus_rule_ids: List[str] = None,
    ) -> LearningQueue:
        """
        构建学习队列
        
        Args:
            rulesets: 铁律集合列表
            focus_rule_ids: 重点关注的铁律ID列表
        
        Returns:
            学习队列
        """
        queue = LearningQueue(
            queue_id=f"queue_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        )
        
        weak_rules = self.identify_weak_rules(rulesets)
        
        for rule, score in weak_rules:
            # 检查是否已在队列中
            if self.learning_queue:
                existing = [t for t in self.learning_queue.tasks if t.rule_id == rule.id]
                if existing:
                    continue
            
            priority = self.calculate_learning_priority(rule)
            target_samples = self.get_target_samples(priority)
            
            # 确定原因
            reason = self._analyze_weakness_reason(rule)
            
            task = LearningTask(
                task_id=self._generate_task_id(),
                rule_id=rule.id,
                rule_name=rule.name,
                priority=priority,
                status=LearningStatus.PENDING,
                target_samples=target_samples,
                reason=reason,
            )
            
            queue.add_task(task)
        
        # 添加重点关注的铁律（优先处理）
        if focus_rule_ids:
            for rule_id in focus_rule_ids:
                for rs in rulesets:
                    for rule in rs.rules:
                        if rule.id == rule_id and rule.status != RuleStatus.ELIMINATED:
                            # 检查是否已在队列
                            existing = [t for t in queue.tasks if t.rule_id == rule_id]
                            if not existing:
                                priority = self.calculate_learning_priority(rule)
                                task = LearningTask(
                                    task_id=self._generate_task_id(),
                                    rule_id=rule.id,
                                    rule_name=rule.name,
                                    priority=LearningPriority.HIGH,  # 强制设为高优先级
                                    status=LearningStatus.PENDING,
                                    target_samples=self.get_target_samples(LearningPriority.HIGH),
                                    reason="用户重点关注",
                                )
                                queue.add_task(task)
                            break
        
        self.learning_queue = queue
        self._save_data()
        
        logger.info(f"构建学习队列: {queue.total_tasks} 个任务")
        return queue
    
    def _analyze_weakness_reason(self, rule: Rule) -> str:
        """分析铁律弱点原因"""
        accuracy = rule.get_current_accuracy()
        
        if rule.total_tests < 10:
            return "样本量不足，需要更多验证"
        
        if accuracy < self.config['critical_threshold']:
            return f"准确率过低({accuracy:.1%})，可能存在根本性问题"
        
        # 检查是否有历史性能记录
        if rule.id in self.performance_records:
            record = self.performance_records[rule.id]
            trend = record.get_trend()
            
            if trend == "declining":
                return "准确率持续下降，需分析下降原因"
            elif trend == "stable":
                return "准确率稳定在低位，需优化适用条件"
        
        return "准确率偏低，需加强验证样本或优化条件"
    
    def get_next_learning_task(self) -> Optional[LearningTask]:
        """获取下一个学习任务"""
        if not self.learning_queue:
            return None
        return self.learning_queue.get_next_task()
    
    def update_learning_progress(
        self,
        task_id: str,
        completed_samples: int = None,
        status: LearningStatus = None,
    ):
        """
        更新学习进度
        
        Args:
            task_id: 任务ID
            completed_samples: 已完成样本数
            status: 状态
        """
        if not self.learning_queue:
            return
        
        self.learning_queue.update_task(
            task_id,
            completed_samples=completed_samples,
            status=status.value if status else None,
        )
        
        # 检查是否完成
        for task in self.learning_queue.tasks:
            if task.task_id == task_id:
                if task.completed_samples >= task.target_samples:
                    task.status = LearningStatus.COMPLETED
                    self.learning_queue.completed_tasks += 1
                break
        
        self._save_data()
    
    def record_validation_result(
        self,
        rule_id: str,
        accuracy: float,
        sample_count: int,
        failure_context: Dict = None,
        success_context: Dict = None,
    ):
        """
        记录验证结果用于学习
        
        Args:
            rule_id: 铁律ID
            accuracy: 准确率
            sample_count: 样本数
            failure_context: 失败上下文
            success_context: 成功上下文
        """
        if rule_id not in self.performance_records:
            self.performance_records[rule_id] = RulePerformanceRecord(rule_id=rule_id)
        
        record = self.performance_records[rule_id]
        record.add_sample(accuracy, sample_count)
        
        if failure_context:
            record.failure_patterns.append(failure_context)
        if success_context:
            record.success_patterns.append(success_context)
        
        self._save_data()
    
    def suggest_rule_conditions(
        self,
        rule_id: str,
        market_conditions: List[str] = None,
    ) -> List[Dict]:
        """
        建议铁律的适用条件
        
        Args:
            rule_id: 铁律ID
            market_conditions: 市场条件列表
        
        Returns:
            建议的适用条件
        """
        if rule_id not in self.performance_records:
            return []
        
        record = self.performance_records[rule_id]
        suggestions = []
        
        # 分析失败模式
        failure_by_condition = defaultdict(list)
        for pattern in record.failure_patterns:
            condition = pattern.get('market_condition', 'unknown')
            failure_by_condition[condition].append(pattern)
        
        # 基于失败模式生成建议
        for condition, failures in failure_by_condition.items():
            if len(failures) >= 3:
                suggestions.append({
                    'type': 'exclude',
                    'condition': condition,
                    'reason': f"在{condition}下失败{len(failures)}次",
                    'confidence': min(len(failures) / 10, 0.9),
                })
        
        # 分析成功模式
        success_by_condition = defaultdict(list)
        for pattern in record.success_patterns:
            condition = pattern.get('market_condition', 'unknown')
            success_by_condition[condition].append(pattern)
        
        for condition, successes in success_by_condition.items():
            if len(successes) >= 5:
                suggestions.append({
                    'type': 'prefer',
                    'condition': condition,
                    'reason': f"在{condition}下成功{len(successes)}次",
                    'confidence': min(len(successes) / 15, 0.9),
                })
        
        return suggestions
    
    def get_learning_progress(self) -> Dict:
        """获取学习进度"""
        if not self.learning_queue:
            return {
                'active': False,
                'total_tasks': 0,
                'completed_tasks': 0,
                'progress': "0%",
            }
        
        stats = self.learning_queue.get_queue_stats()
        return {
            'active': True,
            'total_tasks': stats['total_tasks'],
            'completed_tasks': stats['completed_tasks'],
            'progress': f"{stats['completed_tasks'] / max(stats['total_tasks'], 1):.1%}",
            'pending_tasks': stats['pending_tasks'],
            'in_progress_tasks': stats['in_progress_tasks'],
            'priority_distribution': stats['priority_distribution'],
        }
    
    def get_rule_performance_summary(self, rule_id: str) -> Dict:
        """获取铁律表现摘要"""
        if rule_id not in self.performance_records:
            return {
                'rule_id': rule_id,
                'has_record': False,
            }
        
        record = self.performance_records[rule_id]
        
        return {
            'rule_id': rule_id,
            'has_record': True,
            'total_samples': sum(record.sample_count_history),
            'recent_accuracy': record.accuracy_history[-1] if record.accuracy_history else 0,
            'average_accuracy': sum(record.accuracy_history) / len(record.accuracy_history) if record.accuracy_history else 0,
            'trend': record.get_trend(),
            'is_stable': record.is_stable(self.config['stability_window']),
            'failure_patterns': len(record.failure_patterns),
            'success_patterns': len(record.success_patterns),
        }
    
    def adjust_validation_strategy(
        self,
        rulesets: List[RuleSet],
    ) -> Dict[str, Any]:
        """
        动态调整验证策略
        
        Args:
            rulesets: 铁律集合列表
        
        Returns:
            调整建议
        """
        adjustments = {
            'increased_samples': [],     # 需要增加样本的铁律
            'decreased_samples': [],     # 可以减少样本的铁律
            'focus_market_conditions': [],  # 重点验证的市场条件
            'avoid_market_conditions': [],  # 应避免的市场条件
        }
        
        for rs in rulesets:
            for rule in rs.rules:
                if rule.id not in self.performance_records:
                    continue
                
                record = self.performance_records[rule.id]
                suggestions = self.suggest_rule_conditions(rule.id)
                
                # 统计条件偏好
                for suggestion in suggestions:
                    if suggestion['type'] == 'exclude':
                        if suggestion['condition'] not in adjustments['avoid_market_conditions']:
                            adjustments['avoid_market_conditions'].append(suggestion['condition'])
                    elif suggestion['type'] == 'prefer':
                        if suggestion['condition'] not in adjustments['focus_market_conditions']:
                            adjustments['focus_market_conditions'].append(suggestion['condition'])
                
                # 基于趋势调整样本数
                trend = record.get_trend()
                if trend == "improving" and record.is_stable():
                    adjustments['decreased_samples'].append(rule.id)
                elif trend == "declining":
                    adjustments['increased_samples'].append(rule.id)
        
        return adjustments
    
    def export_learning_report(self, export_path: str = None) -> str:
        """
        导出学习报告
        
        Args:
            export_path: 导出路径
        
        Returns:
            报告内容
        """
        if export_path is None:
            export_path = str(self.storage_dir / f"learning_report_{datetime.now().strftime('%Y%m%d')}.md")
        
        lines = [
            "# 铁律学习报告",
            f"\n生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n## 学习进度",
        ]
        
        progress = self.get_learning_progress()
        lines.append(f"- 总任务数: {progress['total_tasks']}")
        lines.append(f"- 已完成任务: {progress['completed_tasks']}")
        lines.append(f"- 进度: {progress['progress']}")
        
        if self.learning_queue:
            lines.append("\n## 待学习铁律")
            lines.append("\n| 优先级 | 铁律ID | 名称 | 目标样本 | 已完成 | 原因 |")
            lines.append("|--------|--------|------|----------|--------|------|")
            
            for task in sorted(self.learning_queue.tasks, key=lambda x: x.priority.value):
                lines.append(
                    f"| {task.priority.name} | {task.rule_id} | {task.rule_name} | "
                    f"{task.target_samples} | {task.completed_samples} | {task.reason} |"
                )
        
        lines.append("\n## 性能摘要")
        for rule_id, record in self.performance_records.items():
            summary = self.get_rule_performance_summary(rule_id)
            lines.append(f"\n### {rule_id}")
            lines.append(f"- 趋势: {summary['trend']}")
            lines.append(f"- 最近准确率: {summary['recent_accuracy']:.1%}")
            lines.append(f"- 平均准确率: {summary['average_accuracy']:.1%}")
            lines.append(f"- 稳定: {'是' if summary['is_stable'] else '否'}")
        
        content = "\n".join(lines)
        
        with open(export_path, 'w', encoding='utf-8') as f:
            f.write(content)
        
        logger.info(f"导出学习报告到 {export_path}")
        return export_path


def integrate_with_validator(validator):
    """
    将反馈学习集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    # 创建反馈学习实例
    feedback = FeedbackLearning()
    
    # 添加到验证器
    validator.feedback_learning = feedback
    validator.identify_weak_rules = lambda: feedback.identify_weak_rules(validator.rule_parser.rulesets)
    validator.build_learning_queue = lambda focus_ids=None: feedback.build_learning_queue(
        validator.rule_parser.rulesets, focus_ids
    )
    validator.get_learning_progress = feedback.get_learning_progress
    validator.record_validation_result = feedback.record_validation_result
    validator.adjust_validation_strategy = lambda: feedback.adjust_validation_strategy(
        validator.rule_parser.rulesets
    )
    
    return validator
