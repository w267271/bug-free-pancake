# -*- coding: utf-8 -*-
"""
铁律验证分级自动化系统
L1-L4四级自动化，支持人工确认接口
"""
from pathlib import Path
import json
from datetime import datetime
from typing import List, Dict, Optional, Any, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from abc import ABC, abstractmethod
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleStatus
from utils.logger import get_logger

logger = get_logger("AutomationLevels")


class AutomationLevel(Enum):
    """自动化等级"""
    L1_FULL_AUTO = "L1"      # 全自动运行
    L2_SEMI_AUTO = "L2"      # 半自动
    L3_ASSIST_DECISION = "L3"  # 辅助决策
    L4_MANUAL = "L4"          # 纯人工


class ConfirmationType(Enum):
    """确认类型"""
    RULE_TRIGGER = "rule_trigger"           # 铁律触发确认
    MARKET_REGIME_CHANGE = "regime_change"   # 市场状态变更确认
    LIFECYCLE_TRANSITION = "lifecycle"      # 生命周期转换确认
    UNCERTAIN_RESULT = "uncertain"          # 不确定结果确认
    HIGH_RISK_ACTION = "high_risk"          # 高风险操作确认


@dataclass
class ConfirmationRequest:
    """确认请求"""
    request_id: str
    confirmation_type: ConfirmationType
    level: AutomationLevel
    title: str
    description: str
    details: Dict[str, Any]
    options: List[str]           # 确认选项
    default_option: str = ""      # 默认选项
    timeout_seconds: int = 300    # 超时时间
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    status: str = "pending"      # pending/approved/rejected/timeout
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'request_id': self.request_id,
            'confirmation_type': self.confirmation_type.value,
            'level': self.level.value,
            'title': self.title,
            'description': self.description,
            'details': self.details,
            'options': self.options,
            'default_option': self.default_option,
            'timeout_seconds': self.timeout_seconds,
            'created_at': self.created_at,
            'status': self.status,
        }


@dataclass
class LevelConfiguration:
    """等级配置"""
    level: AutomationLevel
    name: str
    description: str
    auto_execute: bool
    confirmation_required: bool
    confidence_threshold: float     # 置信度阈值
    max_auto_actions: int            # 最大自动操作数
    escalation_conditions: List[str]  # 升级条件
    confirmed_actions: int = 0       # 已确认操作数
    rejected_actions: int = 0         # 已拒绝操作数
    
    def get_auto_rate(self) -> float:
        """获取自动执行率"""
        total = self.confirmed_actions + self.rejected_actions
        if total == 0:
            return 1.0 if self.auto_execute else 0.0
        return self.confirmed_actions / total


class BaseConfirmationHandler(ABC):
    """确认处理器基类"""
    
    @abstractmethod
    def request_confirmation(self, request: ConfirmationRequest) -> str:
        """请求确认，返回用户选择"""
        pass
    
    @abstractmethod
    def notify_result(self, request_id: str, result: str):
        """通知结果"""
        pass


class ConsoleConfirmationHandler(BaseConfirmationHandler):
    """控制台确认处理器（用于CLI环境）"""
    
    def __init__(self):
        self.pending_requests: Dict[str, ConfirmationRequest] = {}
    
    def request_confirmation(self, request: ConfirmationRequest) -> str:
        """请求控制台确认"""
        self.pending_requests[request.request_id] = request
        
        print("\n" + "=" * 60)
        print(f"⚠️  需要人工确认: {request.title}")
        print("=" * 60)
        print(f"\n{request.description}")
        print(f"\n详细信息:")
        for key, value in request.details.items():
            print(f"  {key}: {value}")
        print(f"\n选项:")
        for i, option in enumerate(request.options, 1):
            marker = " [默认]" if option == request.default_option else ""
            print(f"  {i}. {option}{marker}")
        
        # 获取用户输入
        while True:
            try:
                choice = input(f"\n请选择 (1-{len(request.options)}): ").strip()
                if not choice:
                    # 使用默认选项
                    idx = request.options.index(request.default_option) if request.default_option in request.options else 0
                else:
                    idx = int(choice) - 1
                    if idx < 0 or idx >= len(request.options):
                        print(f"无效选择，请输入 1-{len(request.options)}")
                        continue
                break
            except ValueError:
                print("请输入有效数字")
        
        result = request.options[idx]
        self.notify_result(request.request_id, result)
        return result
    
    def notify_result(self, request_id: str, result: str):
        """通知结果"""
        if request_id in self.pending_requests:
            self.pending_requests[request_id].status = "approved" if result != "拒绝" else "rejected"
            del self.pending_requests[request_id]


class NoOpConfirmationHandler(BaseConfirmationHandler):
    """空操作确认处理器（用于无人值守环境）"""
    
    def request_confirmation(self, request: ConfirmationRequest) -> str:
        """直接批准"""
        logger.warning(f"无人值守模式，自动批准确认: {request.title}")
        return "批准"
    
    def notify_result(self, request_id: str, result: str):
        """忽略"""
        pass


@dataclass
class AutomationMetrics:
    """自动化指标"""
    total_actions: int = 0
    auto_actions: int = 0
    manual_actions: int = 0
    approved_actions: int = 0
    rejected_actions: int = 0
    avg_confirmation_time: float = 0.0
    escalation_count: int = 0
    
    def get_auto_rate(self) -> float:
        """获取自动执行率"""
        if self.total_actions == 0:
            return 1.0
        return self.auto_actions / self.total_actions
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'total_actions': self.total_actions,
            'auto_actions': self.auto_actions,
            'manual_actions': self.manual_actions,
            'approved_actions': self.approved_actions,
            'rejected_actions': self.rejected_actions,
            'auto_rate': f"{self.get_auto_rate():.1%}",
            'avg_confirmation_time': f"{self.avg_confirmation_time:.1f}s",
            'escalation_count': self.escalation_count,
        }


class AutomationLevelManager:
    """自动化等级管理器"""
    
    # 等级配置
    LEVEL_CONFIGS = {
        AutomationLevel.L1_FULL_AUTO: LevelConfiguration(
            level=AutomationLevel.L1_FULL_AUTO,
            name="全自动运行",
            description="高置信度铁律全自动运行，无需人工干预",
            auto_execute=True,
            confirmation_required=False,
            confidence_threshold=0.85,
            max_auto_actions=100,
            escalation_conditions=["置信度骤降", "连续失败", "异常交易量"],
        ),
        AutomationLevel.L2_SEMI_AUTO: LevelConfiguration(
            level=AutomationLevel.L2_SEMI_AUTO,
            name="半自动",
            description="中置信度铁律，异常情况需人工确认",
            auto_execute=True,
            confirmation_required=True,
            confidence_threshold=0.70,
            max_auto_actions=50,
            escalation_conditions=["置信度<60%", "连续3次失败"],
        ),
        AutomationLevel.L3_ASSIST_DECISION: LevelConfiguration(
            level=AutomationLevel.L3_ASSIST_DECISION,
            name="辅助决策",
            description="低置信度铁律，每步需人工确认",
            auto_execute=False,
            confirmation_required=True,
            confidence_threshold=0.50,
            max_auto_actions=10,
            escalation_conditions=["置信度<40%", "单次失败"],
        ),
        AutomationLevel.L4_MANUAL: LevelConfiguration(
            level=AutomationLevel.L4_MANUAL,
            name="纯人工",
            description="新铁律试用期，全部人工操作",
            auto_execute=False,
            confirmation_required=True,
            confidence_threshold=0.0,
            max_auto_actions=0,
            escalation_conditions=["样本数>=50"],
        ),
    }
    
    def __init__(self, handler: BaseConfirmationHandler = None):
        """
        初始化自动化等级管理器
        
        Args:
            handler: 确认处理器
        """
        self.current_level = AutomationLevel.L2_SEMI_AUTO
        self.confirmation_handler = handler or ConsoleConfirmationHandler()
        self.metrics = AutomationMetrics()
        self.confirmation_history: List[ConfirmationRequest] = []
        self.rule_levels: Dict[str, AutomationLevel] = {}  # 铁律专属等级
        
        # 加载历史配置
        self._load_config()
    
    def _load_config(self):
        """加载配置"""
        from config import PATH_CONFIG
        config_file = Path(PATH_CONFIG['project_root']) / 'data' / 'automation_config.json'
        
        if config_file.exists():
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.current_level = AutomationLevel(data.get('current_level', 'L2'))
                    self.rule_levels = {
                        k: AutomationLevel(v) for k, v in data.get('rule_levels', {}).items()
                    }
            except Exception as e:
                logger.error(f"加载自动化配置失败: {e}")
    
    def _save_config(self):
        """保存配置"""
        from config import PATH_CONFIG
        config_file = Path(PATH_CONFIG['project_root']) / 'data' / 'automation_config.json'
        
        try:
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'current_level': self.current_level.value,
                    'rule_levels': {k: v.value for k, v in self.rule_levels.items()},
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存自动化配置失败: {e}")
    
    def _generate_request_id(self) -> str:
        """生成请求ID"""
        return f"conf_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    def get_rule_level(self, rule_id: str, default_confidence: float = 0.75) -> AutomationLevel:
        """
        获取铁律的自动化等级
        
        Args:
            rule_id: 铁律ID
            default_confidence: 默认置信度
        
        Returns:
            自动化等级
        """
        # 检查是否有专属配置
        if rule_id in self.rule_levels:
            return self.rule_levels[rule_id]
        
        # 根据置信度自动分配
        if default_confidence >= 0.85:
            return AutomationLevel.L1_FULL_AUTO
        elif default_confidence >= 0.70:
            return AutomationLevel.L2_SEMI_AUTO
        elif default_confidence >= 0.50:
            return AutomationLevel.L3_ASSIST_DECISION
        return AutomationLevel.L4_MANUAL
    
    def set_rule_level(self, rule_id: str, level: AutomationLevel):
        """设置铁律的自动化等级"""
        self.rule_levels[rule_id] = level
        self._save_config()
        logger.info(f"设置铁律 {rule_id} 自动化等级为 {level.value}")
    
    def should_confirm(
        self,
        rule_id: str,
        confidence: float,
        context: Dict[str, Any] = None,
    ) -> bool:
        """
        判断是否需要确认
        
        Args:
            rule_id: 铁律ID
            confidence: 置信度
            context: 上下文信息
        
        Returns:
            是否需要确认
        """
        level = self.get_rule_level(rule_id, confidence)
        config = self.LEVEL_CONFIGS[level]
        
        if not config.confirmation_required:
            return False
        
        # 检查是否需要升级
        if self._should_escalate(level, confidence, context):
            self._escalate_rule(rule_id)
            return True
        
        # 检查置信度阈值
        return confidence < config.confidence_threshold
    
    def _should_escalate(
        self,
        level: AutomationLevel,
        confidence: float,
        context: Dict[str, Any],
    ) -> bool:
        """检查是否应该升级"""
        config = self.LEVEL_CONFIGS[level]
        
        for condition in config.escalation_conditions:
            if condition == "置信度骤降" and context:
                prev_confidence = context.get('previous_confidence', confidence)
                if prev_confidence - confidence > 0.2:
                    return True
            elif condition == "连续失败" and context:
                if context.get('consecutive_failures', 0) >= 3:
                    return True
            elif condition == "异常交易量" and context:
                if abs(context.get('volume_change', 0)) > 0.5:
                    return True
            elif "置信度<" in condition:
                threshold = float(condition.split("<")[1].replace("%", "")) / 100
                if confidence < threshold:
                    return True
        
        return False
    
    def _escalate_rule(self, rule_id: str):
        """升级铁律的自动化等级"""
        if rule_id in self.rule_levels:
            current = self.rule_levels[rule_id]
            if current == AutomationLevel.L1_FULL_AUTO:
                self.rule_levels[rule_id] = AutomationLevel.L2_SEMI_AUTO
            elif current == AutomationLevel.L2_SEMI_AUTO:
                self.rule_levels[rule_id] = AutomationLevel.L3_ASSIST_DECISION
        else:
            # 如果没有专属配置，降低默认等级
            pass
        
        self.metrics.escalation_count += 1
        logger.warning(f"铁律 {rule_id} 已升级自动化等级")
    
    def request_confirmation(
        self,
        confirmation_type: ConfirmationType,
        title: str,
        description: str,
        details: Dict[str, Any],
        options: List[str] = None,
        rule_id: str = None,
        confidence: float = 0.75,
    ) -> str:
        """
        请求确认
        
        Args:
            confirmation_type: 确认类型
            title: 标题
            description: 描述
            details: 详细信息
            options: 选项列表
            rule_id: 铁律ID
            confidence: 置信度
        
        Returns:
            用户选择的选项
        """
        if options is None:
            options = ["批准", "拒绝"]
        
        # 确定自动化等级
        level = self.get_rule_level(rule_id or "unknown", confidence)
        config = self.LEVEL_CONFIGS[level]
        
        request = ConfirmationRequest(
            request_id=self._generate_request_id(),
            confirmation_type=confirmation_type,
            level=level,
            title=title,
            description=description,
            details=details,
            options=options,
            default_option=config.auto_execute and options[0] or "",
        )
        
        self.metrics.total_actions += 1
        self.metrics.manual_actions += 1
        
        # 记录历史
        self.confirmation_history.append(request)
        
        # 请求确认
        result = self.confirmation_handler.request_confirmation(request)
        
        if result == "批准" or result == options[0]:
            self.metrics.approved_actions += 1
        else:
            self.metrics.rejected_actions += 1
        
        return result
    
    def execute_with_confirmation(
        self,
        action: Callable,
        confirmation_type: ConfirmationType,
        title: str,
        description: str,
        details: Dict[str, Any],
        rule_id: str = None,
        confidence: float = 0.75,
    ) -> Tuple[bool, Any]:
        """
        带确认的执行
        
        Args:
            action: 要执行的操作
            confirmation_type: 确认类型
            title: 标题
            description: 描述
            details: 详细信息
            rule_id: 铁律ID
            confidence: 置信度
        
        Returns:
            (是否成功, 结果)
        """
        # 检查是否需要确认
        if self.should_confirm(rule_id, confidence, details):
            result = self.request_confirmation(
                confirmation_type=confirmation_type,
                title=title,
                description=description,
                details=details,
                rule_id=rule_id,
                confidence=confidence,
            )
            
            if result != "批准" and result != "确认":
                return False, "用户拒绝"
        
        # 执行操作
        try:
            result = action()
            self.metrics.auto_actions += 1
            return True, result
        except Exception as e:
            logger.error(f"执行操作失败: {e}")
            return False, str(e)
    
    def get_level_stats(self) -> Dict:
        """获取等级统计"""
        stats = {}
        for level, config in self.LEVEL_CONFIGS.items():
            rules_at_level = sum(1 for r, l in self.rule_levels.items() if l == level)
            stats[level.value] = {
                'name': config.name,
                'description': config.description,
                'auto_execute': config.auto_execute,
                'confidence_threshold': f"{config.confidence_threshold:.0%}",
                'rules_count': rules_at_level,
            }
        return stats
    
    def get_metrics(self) -> Dict:
        """获取自动化指标"""
        return self.metrics.to_dict()
    
    def switch_level(self, level: AutomationLevel):
        """切换自动化等级"""
        self.current_level = level
        self._save_config()
        logger.info(f"切换自动化等级到 {level.value}: {self.LEVEL_CONFIGS[level].name}")


def create_confirmation_for_rule(
    manager: AutomationLevelManager,
    rule: Rule,
    validation_result: Dict[str, Any],
) -> Optional[ConfirmationRequest]:
    """
    为铁律创建确认请求
    
    Args:
        manager: 自动化等级管理器
        rule: 铁律
        validation_result: 验证结果
    
    Returns:
        确认请求
    """
    if not manager.should_confirm(
        rule.id,
        rule.accuracy,
        validation_result
    ):
        return None
    
    # 构建确认详情
    details = {
        'rule_id': rule.id,
        'rule_name': rule.name,
        'current_accuracy': f"{rule.accuracy:.1%}",
        'total_tests': rule.total_tests,
        'validation_result': validation_result.get('result', 'unknown'),
        'expected_return': f"{validation_result.get('expected_return', 0):.2%}",
    }
    
    return manager.request_confirmation(
        confirmation_type=ConfirmationType.RULE_TRIGGER,
        title=f"铁律 {rule.id} 触发确认",
        description=f"铁律「{rule.name}」验证结果需要确认",
        details=details,
        rule_id=rule.id,
        confidence=rule.accuracy,
    )


def create_lifecycle_confirmation(
    manager: AutomationLevelManager,
    rule: Rule,
    transition_type: str,
    from_status: RuleStatus,
    to_status: RuleStatus,
) -> str:
    """
    创建生命周期转换确认
    
    Args:
        manager: 自动化等级管理器
        rule: 铁律
        transition_type: 转换类型
        from_status: 原状态
        to_status: 目标状态
    
    Returns:
        用户选择
    """
    confidence = rule.accuracy
    level = manager.get_rule_level(rule.id, confidence)
    
    if level == AutomationLevel.L4_MANUAL or level == AutomationLevel.L3_ASSIST_DECISION:
        return manager.request_confirmation(
            confirmation_type=ConfirmationType.LIFECYCLE_TRANSITION,
            title=f"铁律生命周期变更: {rule.id}",
            description=f"铁律「{rule.name}」状态从 {from_status.value} 变更为 {to_status.value}",
            details={
                'rule_id': rule.id,
                'rule_name': rule.name,
                'transition_type': transition_type,
                'from_status': from_status.value,
                'to_status': to_status.value,
                'current_accuracy': f"{confidence:.1%}",
                'total_tests': rule.total_tests,
            },
            options=["确认变更", "取消", "保持当前状态"],
            rule_id=rule.id,
            confidence=confidence,
        )
    return "自动批准"


def integrate_with_validator(validator):
    """
    将分级自动化集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    # 创建自动化管理器
    automation = AutomationLevelManager()
    
    # 添加到验证器
    validator.automation_manager = automation
    validator.should_confirm = automation.should_confirm
    validator.request_confirmation = automation.request_confirmation
    validator.get_automation_metrics = automation.get_metrics
    validator.get_level_stats = automation.get_level_stats
    validator.switch_automation_level = automation.switch_level
    
    return validator
