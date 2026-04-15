# -*- coding: utf-8 -*-
"""
铁律相关性分析模块
分析铁律之间的相关性，识别冗余
"""
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass, field
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from config import CORRELATION_CONFIG
from models.result import RuleStatistics, SampleTestResult
from utils.logger import get_logger

logger = get_logger("RuleCorrelation")


@dataclass
class CorrelationEntry:
    """相关性条目"""
    rule_id_1: str
    rule_id_2: str
    correlation: float
    sample_overlap: float  # 样本重叠度
    
    def is_high_correlation(self, threshold: float) -> bool:
        return abs(self.correlation) >= threshold
    
    def to_dict(self) -> Dict:
        return {
            'rule_1': self.rule_id_1,
            'rule_2': self.rule_id_2,
            'correlation': round(self.correlation, 4),
            'sample_overlap': round(self.sample_overlap, 4),
        }


@dataclass
class RuleCluster:
    """铁律聚类"""
    cluster_id: int
    rules: List[str]
    avg_correlation: float
    description: str = ""
    
    def to_dict(self) -> Dict:
        return {
            'cluster_id': self.cluster_id,
            'rules': self.rules,
            'rule_count': len(self.rules),
            'avg_correlation': round(self.avg_correlation, 4),
            'description': self.description,
        }


@dataclass
class CorrelationMatrix:
    """相关性矩阵"""
    rules: List[str]
    matrix: np.ndarray
    entries: List[CorrelationEntry] = field(default_factory=list)
    
    def get_correlation(self, rule_1: str, rule_2: str) -> float:
        """获取两个铁律的相关性"""
        idx1 = self.rules.index(rule_1) if rule_1 in self.rules else -1
        idx2 = self.rules.index(rule_2) if rule_2 in self.rules else -1
        
        if idx1 < 0 or idx2 < 0:
            return 0.0
        
        return self.matrix[idx1, idx2]
    
    def get_high_correlations(self, threshold: float) -> List[CorrelationEntry]:
        """获取高相关性条目"""
        return [e for e in self.entries if e.is_high_correlation(threshold)]


class RuleCorrelator:
    """铁律相关性分析器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化分析器
        
        Args:
            config: 配置字典
        """
        self.config = config or CORRELATION_CONFIG
        self.high_threshold = self.config.get('high_correlation_threshold', 0.8)
        self.medium_threshold = self.config.get('medium_correlation_threshold', 0.5)
    
    def build_correlation_matrix(
        self,
        sample_results: List[SampleTestResult],
        rule_ids: List[str] = None
    ) -> CorrelationMatrix:
        """
        构建铁律相关性矩阵
        
        Args:
            sample_results: 样本测试结果
            rule_ids: 铁律ID列表（如果为None，从结果中提取）
        
        Returns:
            相关性矩阵
        """
        # 如果未指定铁律ID，从结果中提取
        if rule_ids is None:
            rule_ids = set()
            for result in sample_results:
                rule_ids.update(result.triggered_rules)
            rule_ids = sorted(list(rule_ids))
        
        n_rules = len(rule_ids)
        
        # 创建规则ID到索引的映射
        rule_to_idx = {rid: i for i, rid in enumerate(rule_ids)}
        
        # 创建规则触发矩阵
        trigger_matrix = np.zeros((len(sample_results), n_rules))
        for i, result in enumerate(sample_results):
            for rule_id in result.triggered_rules:
                if rule_id in rule_to_idx:
                    trigger_matrix[i, rule_to_idx[rule_id]] = 1
        
        # 计算相关性矩阵
        corr_matrix = np.corrcoef(trigger_matrix.T)
        
        # 处理NaN（当标准差为0时）
        corr_matrix = np.nan_to_num(corr_matrix, nan=0.0)
        
        # 创建相关性条目
        entries = []
        for i in range(n_rules):
            for j in range(i + 1, n_rules):
                if corr_matrix[i, j] != 0:  # 只记录非零相关性
                    # 计算样本重叠度
                    overlap = self._calculate_overlap(
                        trigger_matrix[:, i], trigger_matrix[:, j]
                    )
                    entries.append(CorrelationEntry(
                        rule_id_1=rule_ids[i],
                        rule_id_2=rule_ids[j],
                        correlation=corr_matrix[i, j],
                        sample_overlap=overlap,
                    ))
        
        logger.info(f"构建相关性矩阵: {n_rules}条铁律, {len(entries)}对存在相关性")
        
        return CorrelationMatrix(
            rules=rule_ids,
            matrix=corr_matrix,
            entries=entries,
        )
    
    def _calculate_overlap(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """计算两个向量的重叠度"""
        # Jaccard相似度
        intersection = np.sum((vec1 == 1) & (vec2 == 1))
        union = np.sum((vec1 == 1) | (vec2 == 1))
        
        if union == 0:
            return 0.0
        return intersection / union
    
    def cluster_similar_rules(
        self,
        corr_matrix: CorrelationMatrix,
        threshold: float = None
    ) -> List[RuleCluster]:
        """
        聚类高度相关的铁律
        
        Args:
            corr_matrix: 相关性矩阵
            threshold: 聚类阈值（默认为高相关阈值）
        
        Returns:
            聚类列表
        """
        if threshold is None:
            threshold = self.high_threshold
        
        # 构建邻接图
        n_rules = len(corr_matrix.rules)
        adj_matrix = np.abs(corr_matrix.matrix) >= threshold
        
        # 移除对角线
        np.fill_diagonal(adj_matrix, False)
        
        # 使用并查集进行聚类
        parent = list(range(n_rules))
        
        def find(x):
            if parent[x] != x:
                parent[x] = find(parent[x])
            return parent[x]
        
        def union(x, y):
            px, py = find(x), find(y)
            if px != py:
                parent[px] = py
        
        # 遍历所有边
        for i in range(n_rules):
            for j in range(i + 1, n_rules):
                if adj_matrix[i, j]:
                    union(i, j)
        
        # 收集聚类
        clusters_dict = {}
        for i in range(n_rules):
            root = find(i)
            if root not in clusters_dict:
                clusters_dict[root] = []
            clusters_dict[root].append(i)
        
        # 创建聚类对象
        clusters = []
        for idx, (root, members) in enumerate(clusters_dict.items()):
            if len(members) < 1:
                continue
            
            rule_ids = [corr_matrix.rules[i] for i in members]
            
            # 计算聚类内平均相关性
            if len(members) > 1:
                corrs = []
                for i in range(len(members)):
                    for j in range(i + 1, len(members)):
                        corrs.append(corr_matrix.matrix[members[i], members[j]])
                avg_corr = np.mean(corrs) if corrs else 0
            else:
                avg_corr = 1.0
            
            # 生成描述
            desc = self._generate_cluster_description(rule_ids)
            
            clusters.append(RuleCluster(
                cluster_id=idx,
                rules=rule_ids,
                avg_correlation=avg_corr,
                description=desc,
            ))
        
        logger.info(f"聚类完成: {len(clusters)}个聚类")
        
        return clusters
    
    def _generate_cluster_description(self, rule_ids: List[str]) -> str:
        """生成聚类描述"""
        # 按系列分组
        series_groups = {}
        for rid in rule_ids:
            series = rid[0] if rid else ''
            if series not in series_groups:
                series_groups[series] = []
            series_groups[series].append(rid)
        
        parts = []
        for series, rules in series_groups.items():
            parts.append(f"{series}系列{len(rules)}条")
        
        return "、".join(parts)
    
    def find_redundant_rules(
        self,
        clusters: List[RuleCluster]
    ) -> List[Dict]:
        """
        找出冗余铁律
        
        Args:
            clusters: 聚类列表
        
        Returns:
            冗余铁律信息列表
        """
        redundant = []
        
        for cluster in clusters:
            if len(cluster.rules) < 2:
                continue
            
            # 在同一聚类中，保留第一条，其余标记为冗余
            for i, rule_id in enumerate(cluster.rules[1:], 1):
                redundant.append({
                    'rule_id': rule_id,
                    'cluster_id': cluster.cluster_id,
                    'similar_rules': cluster.rules[:i] + cluster.rules[i+1:],
                    'reason': f'与{cluster.rules[0]}高度相关，可能冗余',
                    'recommendation': '建议合并或保留其一',
                })
        
        return redundant
    
    def analyze_correlation_patterns(
        self,
        corr_matrix: CorrelationMatrix
    ) -> Dict:
        """
        分析相关性模式
        
        Args:
            corr_matrix: 相关性矩阵
        
        Returns:
            分析结果
        """
        # 高相关铁律对
        high_corr = corr_matrix.get_high_correlations(self.high_threshold)
        
        # 中等相关铁律对
        medium_corr = [
            e for e in corr_matrix.entries
            if abs(e.correlation) >= self.medium_threshold
            and abs(e.correlation) < self.high_threshold
        ]
        
        # 按系列分析
        series_corr = self._analyze_series_correlation(corr_matrix)
        
        return {
            'high_correlation_pairs': [e.to_dict() for e in high_corr],
            'high_correlation_count': len(high_corr),
            'medium_correlation_count': len(medium_corr),
            'series_correlation': series_corr,
            'most_correlated_pairs': sorted(
                corr_matrix.entries,
                key=lambda x: abs(x.correlation),
                reverse=True
            )[:10],
        }
    
    def _analyze_series_correlation(self, corr_matrix: CorrelationMatrix) -> Dict:
        """分析各系列之间的相关性"""
        series_groups = {}
        for i, rid in enumerate(corr_matrix.rules):
            series = rid[0] if rid else 'UNKNOWN'
            if series not in series_groups:
                series_groups[series] = []
            series_groups[series].append(i)
        
        series_corr = {}
        series_list = sorted(series_groups.keys())
        
        for i, s1 in enumerate(series_list):
            for s2 in series_list[i+1:]:
                indices1 = series_groups[s1]
                indices2 = series_groups[s2]
                
                corrs = []
                for idx1 in indices1:
                    for idx2 in indices2:
                        corr = corr_matrix.matrix[idx1, idx2]
                        if not np.isnan(corr):
                            corrs.append(abs(corr))
                
                if corrs:
                    series_corr[f"{s1}-{s2}"] = {
                        'avg_correlation': float(np.mean(corrs)),
                        'max_correlation': float(np.max(corrs)),
                        'pair_count': len(corrs),
                    }
        
        return series_corr


def build_correlation_matrix(
    sample_results: List[SampleTestResult],
    rule_ids: List[str] = None
) -> CorrelationMatrix:
    """
    构建相关矩阵的便捷函数
    
    Args:
        sample_results: 样本测试结果
        rule_ids: 铁律ID列表
    
    Returns:
        相关性矩阵
    """
    correlator = RuleCorrelator()
    return correlator.build_correlation_matrix(sample_results, rule_ids)


def cluster_similar_rules(
    corr_matrix: CorrelationMatrix,
    threshold: float = 0.8
) -> List[RuleCluster]:
    """
    聚类相似铁律的便捷函数
    
    Args:
        corr_matrix: 相关性矩阵
        threshold: 聚类阈值
    
    Returns:
        聚类列表
    """
    correlator = RuleCorrelator()
    return correlator.cluster_similar_rules(corr_matrix, threshold)
