# -*- coding: utf-8 -*-
"""
铁律版本控制系统
Git风格的版本管理，支持版本历史、回滚、标签管理
"""
import json
import hashlib
import shutil
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
from dataclasses import dataclass, field, asdict
from enum import Enum
import copy
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleVersion, RuleStatus
from utils.logger import get_logger

logger = get_logger("VersionControl")


class VersionTag(Enum):
    """版本标签类型"""
    STABLE = "stable"           # 稳定版
    EXPERIMENTAL = "experimental"  # 实验版
    DEPRECATED = "deprecated"   # 已废弃
    LATEST = "latest"           # 最新版


class ChangeType(Enum):
    """变更类型"""
    ADD = "add"                 # 新增
    MODIFY = "modify"          # 修改
    DELETE = "delete"           # 删除
    DEMOTE = "demote"           # 降级
    UPGRADE = "upgrade"         # 升级


@dataclass
class VersionChange:
    """版本变更记录"""
    change_type: ChangeType
    rule_id: str
    rule_name: str
    before_value: Any = None
    after_value: Any = None
    description: str = ""
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class VersionSnapshot:
    """版本快照"""
    version_id: str                    # 版本ID（基于时间戳）
    version_number: str                 # 版本号（如v1.0, v1.1）
    created_at: str                     # 创建时间
    description: str                    # 版本描述
    changes: List[VersionChange]       # 本次变更
    rules_snapshot: Dict[str, Dict]    # 铁律快照
    tags: List[str]                     # 标签列表
    author: str = "system"             # 作者
    parent_version: str = ""           # 父版本
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'version_id': self.version_id,
            'version_number': self.version_number,
            'created_at': self.created_at,
            'description': self.description,
            'changes': [asdict(c) for c in self.changes],
            'rules_snapshot': self.rules_snapshot,
            'tags': self.tags,
            'author': self.author,
            'parent_version': self.parent_version,
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'VersionSnapshot':
        """从字典创建"""
        changes = [VersionChange(**c) for c in data.get('changes', [])]
        return cls(
            version_id=data['version_id'],
            version_number=data['version_number'],
            created_at=data['created_at'],
            description=data['description'],
            changes=changes,
            rules_snapshot=data['rules_snapshot'],
            tags=data.get('tags', []),
            author=data.get('author', 'system'),
            parent_version=data.get('parent_version', ''),
        )


class VersionControl:
    """铁律版本控制系统"""
    
    def __init__(self, storage_dir: str = None):
        """
        初始化版本控制系统
        
        Args:
            storage_dir: 版本存储目录
        """
        if storage_dir is None:
            from config import PATH_CONFIG
            storage_dir = str(Path(PATH_CONFIG['project_root']) / 'data' / 'versions')
        
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # 版本文件
        self.versions_file = self.storage_dir / 'versions.json'
        self.current_version_file = self.storage_dir / 'current_version.txt'
        self.tags_file = self.storage_dir / 'tags.json'
        
        # 加载版本历史
        self.versions: List[VersionSnapshot] = []
        self.tags: Dict[str, str] = {}  # 标签名 -> 版本ID
        self.current_version_id: str = ""
        
        self._load_versions()
        self._load_tags()
    
    def _load_versions(self):
        """加载版本历史"""
        if self.versions_file.exists():
            try:
                with open(self.versions_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.versions = [VersionSnapshot.from_dict(v) for v in data]
            except Exception as e:
                logger.error(f"加载版本历史失败: {e}")
                self.versions = []
        
        if self.current_version_file.exists():
            self.current_version_id = self.current_version_file.read_text().strip()
    
    def _save_versions(self):
        """保存版本历史"""
        try:
            with open(self.versions_file, 'w', encoding='utf-8') as f:
                json.dump([v.to_dict() for v in self.versions], f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存版本历史失败: {e}")
    
    def _load_tags(self):
        """加载标签"""
        if self.tags_file.exists():
            try:
                with open(self.tags_file, 'r', encoding='utf-8') as f:
                    self.tags = json.load(f)
            except Exception as e:
                logger.error(f"加载标签失败: {e}")
                self.tags = {}
    
    def _save_tags(self):
        """保存标签"""
        try:
            with open(self.tags_file, 'w', encoding='utf-8') as f:
                json.dump(self.tags, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存标签失败: {e}")
    
    def _generate_version_id(self) -> str:
        """生成版本ID"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hash_str = hashlib.md5(timestamp.encode()).hexdigest()[:8]
        return f"{timestamp}_{hash_str}"
    
    def _serialize_rules(self, rulesets: List[RuleSet]) -> Dict[str, Dict]:
        """序列化铁律集合"""
        snapshot = {}
        for rs in rulesets:
            for rule in rs.rules:
                snapshot[rule.id] = {
                    'name': rule.name,
                    'content': rule.content,
                    'accuracy': rule.accuracy,
                    'rule_type': rule.rule_type.value,
                    'term_type': rule.term_type.value,
                    'status': rule.status.value,
                    'version': rule.version,
                    'total_tests': rule.total_tests,
                    'successful_tests': rule.successful_tests,
                    'failed_tests': rule.failed_tests,
                }
        return snapshot
    
    def _deserialize_rules(self, snapshot: Dict[str, Dict]) -> List[RuleSet]:
        """反序列化铁律集合"""
        from models.rule import Rule, RuleSet, RuleType, TermType, RuleStatus
        
        rules_by_series = {}
        for rule_id, data in snapshot.items():
            series_id = rule_id[0]  # 从规则ID提取系列ID
            
            rule = Rule(
                id=rule_id,
                name=data['name'],
                content=data['content'],
                accuracy=data['accuracy'],
                rule_type=RuleType(data['rule_type']),
                term_type=TermType(data['term_type']),
                status=RuleStatus(data['status']),
                version=data.get('version', 'v1.0'),
                total_tests=data.get('total_tests', 0),
                successful_tests=data.get('successful_tests', 0),
                failed_tests=data.get('failed_tests', 0),
            )
            
            if series_id not in rules_by_series:
                rules_by_series[series_id] = []
            rules_by_series[series_id].append(rule)
        
        rulesets = []
        series_names = {'S': '短线铁律', 'M': '中线铁律', 'L': '长线铁律', 
                       'T': '成交量铁律', 'P': '板块轮动铁律', 'E': '市场情绪铁律',
                       'R': '融资融券铁律', 'B': '龙虎榜铁律', 'N': '综合铁律',
                       'Z': '其他铁律', 'Q': '量化铁律'}
        
        for series_id, rules in rules_by_series.items():
            rulesets.append(RuleSet(
                series_id=series_id,
                series_name=series_names.get(series_id, '未知系列'),
                rules=rules,
            ))
        
        return rulesets
    
    def _compute_diff(self, before: Dict, after: Dict) -> List[VersionChange]:
        """计算两个版本之间的差异"""
        changes = []
        
        # 找出新增和修改的规则
        for rule_id, after_data in after.items():
            if rule_id not in before:
                changes.append(VersionChange(
                    change_type=ChangeType.ADD,
                    rule_id=rule_id,
                    rule_name=after_data['name'],
                    after_value=after_data,
                    description=f"新增铁律 {rule_id}",
                ))
            else:
                before_data = before[rule_id]
                diff_fields = []
                for field in ['accuracy', 'status', 'content']:
                    if before_data.get(field) != after_data.get(field):
                        diff_fields.append(field)
                        changes.append(VersionChange(
                            change_type=ChangeType.MODIFY,
                            rule_id=rule_id,
                            rule_name=after_data['name'],
                            before_value={field: before_data.get(field)},
                            after_value={field: after_data.get(field)},
                            description=f"修改铁律 {rule_id} 的 {field}",
                        ))
        
        # 找出删除的规则
        for rule_id, before_data in before.items():
            if rule_id not in after:
                changes.append(VersionChange(
                    change_type=ChangeType.DELETE,
                    rule_id=rule_id,
                    rule_name=before_data['name'],
                    before_value=before_data,
                    description=f"删除铁律 {rule_id}",
                ))
        
        return changes
    
    def create_version(
        self,
        rulesets: List[RuleSet],
        description: str,
        author: str = "system",
        tags: List[str] = None,
    ) -> VersionSnapshot:
        """
        创建新版本
        
        Args:
            rulesets: 铁律集合列表
            description: 版本描述
            author: 作者
            tags: 标签列表
        
        Returns:
            版本快照
        """
        # 生成版本号
        version_numbers = [v.version_number for v in self.versions]
        if not version_numbers:
            version_number = "v1.0"
        else:
            # 解析最新版本号
            latest = sorted(version_numbers, key=lambda x: [int(n) for n in x[1:].split('.')])[-1]
            parts = latest[1:].split('.')
            major, minor = int(parts[0]), int(parts[1]) if len(parts) > 1 else 0
            # 如果有变更则增加小版本号，否则增加大版本号
            current_snapshot = self.versions[-1].rules_snapshot if self.versions else {}
            new_snapshot = self._serialize_rules(rulesets)
            changes = self._compute_diff(current_snapshot, new_snapshot)
            
            if changes:
                version_number = f"v{major}.{minor + 1}"
            else:
                version_number = f"v{major + 1}.0"
        
        # 创建快照
        snapshot = self._serialize_rules(rulesets)
        changes = []
        if self.versions:
            changes = self._compute_diff(self.versions[-1].rules_snapshot, snapshot)
        
        version_snapshot = VersionSnapshot(
            version_id=self._generate_version_id(),
            version_number=version_number,
            created_at=datetime.now().isoformat(),
            description=description,
            changes=changes,
            rules_snapshot=snapshot,
            tags=tags or [],
            author=author,
            parent_version=self.current_version_id,
        )
        
        # 保存铁律快照文件
        snapshot_file = self.storage_dir / f"{version_snapshot.version_id}_rules.json"
        with open(snapshot_file, 'w', encoding='utf-8') as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        
        # 添加到版本历史
        self.versions.append(version_snapshot)
        self._save_versions()
        
        # 更新当前版本
        self.current_version_id = version_snapshot.version_id
        self.current_version_file.write_text(self.current_version_id)
        
        # 保存标签
        if tags:
            for tag in tags:
                self.tags[tag] = version_snapshot.version_id
            self._save_tags()
        
        logger.info(f"创建新版本: {version_snapshot.version_number} ({version_snapshot.version_id})")
        return version_snapshot
    
    def get_version(self, version_id: str = None, version_number: str = None) -> Optional[VersionSnapshot]:
        """
        获取指定版本
        
        Args:
            version_id: 版本ID
            version_number: 版本号
        
        Returns:
            版本快照
        """
        if version_id:
            for v in self.versions:
                if v.version_id == version_id:
                    return v
        elif version_number:
            for v in self.versions:
                if v.version_number == version_number:
                    return v
        return None
    
    def get_current_version(self) -> Optional[VersionSnapshot]:
        """获取当前版本"""
        if self.current_version_id:
            return self.get_version(version_id=self.current_version_id)
        return self.versions[-1] if self.versions else None
    
    def get_version_history(self, limit: int = 10) -> List[VersionSnapshot]:
        """
        获取版本历史
        
        Args:
            limit: 返回数量限制
        
        Returns:
            版本快照列表
        """
        return sorted(self.versions, key=lambda x: x.created_at, reverse=True)[:limit]
    
    def diff_versions(
        self,
        version1_id: str,
        version2_id: str,
    ) -> Dict[str, Any]:
        """
        对比两个版本的差异
        
        Args:
            version1_id: 版本1 ID
            version2_id: 版本2 ID
        
        Returns:
            差异报告
        """
        v1 = self.get_version(version_id=version1_id)
        v2 = self.get_version(version_id=version2_id)
        
        if not v1 or not v2:
            return {'error': '版本不存在'}
        
        changes = self._compute_diff(v1.rules_snapshot, v2.rules_snapshot)
        
        # 统计变更类型
        stats = {
            'add': 0,
            'modify': 0,
            'delete': 0,
            'demote': 0,
            'upgrade': 0,
        }
        for change in changes:
            stats[change.change_type.value] += 1
        
        return {
            'version1': v1.version_number,
            'version2': v2.version_number,
            'total_changes': len(changes),
            'statistics': stats,
            'changes': [asdict(c) for c in changes],
        }
    
    def rollback_to_version(
        self,
        version_id: str = None,
        version_number: str = None,
    ) -> Tuple[bool, str, List[RuleSet]]:
        """
        回滚到指定版本
        
        Args:
            version_id: 版本ID
            version_number: 版本号
        
        Returns:
            (是否成功, 消息, 恢复的铁律集合)
        """
        version = self.get_version(version_id=version_id, version_number=version_number)
        
        if not version:
            return False, f"版本不存在: {version_id or version_number}", []
        
        # 从快照文件加载铁律
        snapshot_file = self.storage_dir / f"{version.version_id}_rules.json"
        if not snapshot_file.exists():
            # 使用内存中的快照
            snapshot = version.rules_snapshot
        else:
            with open(snapshot_file, 'r', encoding='utf-8') as f:
                snapshot = json.load(f)
        
        rulesets = self._deserialize_rules(snapshot)
        
        # 创建回滚版本记录
        rollback_desc = f"回滚到版本 {version.version_number}"
        self.create_version(
            rulesets=rulesets,
            description=rollback_desc,
            author="system",
            tags=["rollback"],
        )
        
        logger.info(f"成功回滚到版本: {version.version_number}")
        return True, f"成功回滚到版本 {version.version_number}", rulesets
    
    def add_tag(self, tag: str, version_id: str = None, version_number: str = None):
        """
        添加版本标签
        
        Args:
            tag: 标签名
            version_id: 版本ID
            version_number: 版本号
        """
        version = self.get_version(version_id=version_id, version_number=version_number)
        
        if not version:
            logger.error(f"无法添加标签: 版本不存在")
            return False
        
        # 添加标签
        if tag not in version.tags:
            version.tags.append(tag)
        
        self.tags[tag] = version.version_id
        self._save_tags()
        self._save_versions()
        
        logger.info(f"添加标签 '{tag}' 到版本 {version.version_number}")
        return True
    
    def remove_tag(self, tag: str):
        """
        移除版本标签
        
        Args:
            tag: 标签名
        """
        if tag in self.tags:
            version_id = self.tags[tag]
            version = self.get_version(version_id=version_id)
            if version and tag in version.tags:
                version.tags.remove(tag)
            
            del self.tags[tag]
            self._save_tags()
            self._save_versions()
            logger.info(f"移除标签 '{tag}'")
            return True
        return False
    
    def get_versions_by_tag(self, tag: str) -> List[VersionSnapshot]:
        """
        获取指定标签的所有版本
        
        Args:
            tag: 标签名
        
        Returns:
            版本列表
        """
        if tag in self.tags:
            version = self.get_version(version_id=self.tags[tag])
            if version:
                return [version]
        return [v for v in self.versions if tag in v.tags]
    
    def export_version(self, version_id: str, export_path: str) -> bool:
        """
        导出版本快照
        
        Args:
            version_id: 版本ID
            export_path: 导出路径
        
        Returns:
            是否成功
        """
        version = self.get_version(version_id=version_id)
        if not version:
            logger.error(f"导出失败: 版本不存在")
            return False
        
        try:
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(version.to_dict(), f, ensure_ascii=False, indent=2)
            logger.info(f"导出版本 {version.version_number} 到 {export_path}")
            return True
        except Exception as e:
            logger.error(f"导出失败: {e}")
            return False
    
    def get_version_stats(self) -> Dict:
        """
        获取版本统计信息
        
        Returns:
            统计信息
        """
        if not self.versions:
            return {
                'total_versions': 0,
                'total_changes': 0,
                'tags': [],
                'first_version': None,
                'latest_version': None,
            }
        
        total_changes = sum(len(v.changes) for v in self.versions)
        
        return {
            'total_versions': len(self.versions),
            'total_changes': total_changes,
            'tags': list(self.tags.keys()),
            'first_version': self.versions[0].version_number if self.versions else None,
            'latest_version': self.versions[-1].version_number if self.versions else None,
            'current_version': self.get_current_version().version_number if self.get_current_version() else None,
        }


def integrate_with_validator(validator):
    """
    将版本控制集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    from config import PATH_CONFIG
    
    # 创建版本控制实例
    version_control = VersionControl()
    
    # 在验证器中添加版本控制方法
    def create_snapshot(description: str, tags: List[str] = None):
        """创建版本快照"""
        return version_control.create_version(
            rulesets=validator.rule_parser.rulesets,
            description=description,
            tags=tags,
        )
    
    def rollback_to(version_id: str = None, version_number: str = None):
        """回滚到指定版本"""
        success, msg, rulesets = version_control.rollback_to_version(version_id, version_number)
        if success:
            # 更新验证器的铁律集合
            validator.rule_parser.rulesets = rulesets
        return success, msg
    
    # 添加到验证器
    validator.version_control = version_control
    validator.create_snapshot = create_snapshot
    validator.rollback_to = rollback_to
    
    return validator
