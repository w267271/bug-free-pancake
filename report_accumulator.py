# -*- coding: utf-8 -*-
"""
报告累计模块 - 多次验证结果自动合并统计
"""
import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import defaultdict
from config import PATH_CONFIG


class ReportAccumulator:
    """报告累计器"""
    
    def __init__(self, cumulative_file: str = None):
        """初始化累计器"""
        if cumulative_file is None:
            results_dir = PATH_CONFIG['results_dir']
            os.makedirs(results_dir, exist_ok=True)
            cumulative_file = os.path.join(results_dir, 'cumulative_report.json')
        
        self.cumulative_file = cumulative_file
        self.data = self._load_cumulative_data()
    
    def _load_cumulative_data(self) -> Dict:
        """加载累计数据"""
        if os.path.exists(self.cumulative_file):
            try:
                with open(self.cumulative_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        
        return self._create_empty_data()
    
    def _create_empty_data(self) -> Dict:
        """创建空数据结构"""
        return {
            'version': '1.0',
            'created_at': datetime.now().isoformat(),
            'last_updated': datetime.now().isoformat(),
            'total_validations': 0,
            'total_samples': 0,
            'total_success': 0,
            'total_failure': 0,
            'overall_accuracy': 0.0,
            'rule_stats': {},  # rule_id -> 累计的铁律统计
            'validation_runs': [],  # 历次验证记录
            'batch_history': []  # 批次历史
        }
    
    def accumulate(self, new_samples: List[Dict], metadata: Dict = None) -> Dict:
        """
        合并新样本到累计报告
        
        Args:
            new_samples: 新样本结果列表
            metadata: 元数据（batch_id, timestamp等）
        
        Returns:
            更新后的累计统计
        """
        if not new_samples:
            return self.get_cumulative_stats()
        
        # 更新基本信息
        self.data['last_updated'] = datetime.now().isoformat()
        self.data['total_validations'] += 1
        self.data['total_samples'] += len(new_samples)
        
        # 统计成功/失败
        success_count = sum(1 for s in new_samples if s.get('overall_success', False))
        failure_count = len(new_samples) - success_count
        self.data['total_success'] += success_count
        self.data['total_failure'] += failure_count
        
        # 计算总体准确率
        if self.data['total_samples'] > 0:
            self.data['overall_accuracy'] = round(
                self.data['total_success'] / self.data['total_samples'], 4
            )
        
        # 累计铁律统计
        self._accumulate_rule_stats(new_samples)
        
        # 记录验证运行
        validation_record = {
            'timestamp': datetime.now().isoformat(),
            'batch_id': metadata.get('batch_id', 'unknown') if metadata else 'unknown',
            'sample_count': len(new_samples),
            'success_count': success_count,
            'accuracy': round(success_count / len(new_samples), 4) if new_samples else 0
        }
        self.data['validation_runs'].append(validation_record)
        
        # 记录批次历史
        if metadata and metadata.get('batch_id'):
            batch_record = {
                'batch_id': metadata['batch_id'],
                'timestamp': datetime.now().isoformat(),
                'samples_added': len(new_samples)
            }
            self.data['batch_history'].append(batch_record)
        
        # 保存
        self._save()
        
        return self.get_cumulative_stats()
    
    def _accumulate_rule_stats(self, samples: List[Dict]):
        """累计铁律统计"""
        # 收集每条铁律的测试结果
        rule_tests = defaultdict(lambda: {'success': 0, 'failure': 0, 'returns': []})
        
        for sample in samples:
            triggered_rules = sample.get('triggered_rules', [])
            success = sample.get('overall_success', False)
            return_rate = sample.get('return_rate', 0)
            
            for rule_id in triggered_rules:
                if success:
                    rule_tests[rule_id]['success'] += 1
                else:
                    rule_tests[rule_id]['failure'] += 1
                rule_tests[rule_id]['returns'].append(return_rate)
        
        # 更新累计统计
        for rule_id, stats in rule_tests.items():
            if rule_id not in self.data['rule_stats']:
                self.data['rule_stats'][rule_id] = {
                    'rule_id': rule_id,
                    'total_tests': 0,
                    'success_count': 0,
                    'failure_count': 0,
                    'total_return': 0,
                    'returns_list': []
                }
            
            rule_data = self.data['rule_stats'][rule_id]
            rule_data['total_tests'] += stats['success'] + stats['failure']
            rule_data['success_count'] += stats['success']
            rule_data['failure_count'] += stats['failure']
            rule_data['total_return'] += sum(stats['returns'])
            
            # 保存最近100个收益率记录
            if 'recent_returns' not in rule_data:
                rule_data['recent_returns'] = []
            rule_data['recent_returns'].extend(stats['returns'])
            if len(rule_data['recent_returns']) > 100:
                rule_data['recent_returns'] = rule_data['recent_returns'][-100:]
    
    def get_cumulative_stats(self) -> Dict:
        """获取累计统计"""
        stats = {
            'total_validations': self.data['total_validations'],
            'total_samples': self.data['total_samples'],
            'total_success': self.data['total_success'],
            'total_failure': self.data['total_failure'],
            'overall_accuracy': self.data['overall_accuracy'],
            'avg_return': 0.0,
            'avg_success_rate': 0.0
        }
        
        # 计算平均收益率
        if self.data['rule_stats']:
            total_return = sum(r.get('total_return', 0) for r in self.data['rule_stats'].values())
            total_tests = sum(r.get('total_tests', 0) for r in self.data['rule_stats'].values())
            if total_tests > 0:
                stats['avg_return'] = round(total_return / total_tests, 4)
        
        # 计算平均成功率
        if self.data['validation_runs']:
            stats['avg_success_rate'] = round(
                sum(v['accuracy'] for v in self.data['validation_runs']) / len(self.data['validation_runs']), 4
            )
        
        # 铁律排名
        rule_rankings = []
        for rule_id, rule_data in self.data['rule_stats'].items():
            if rule_data['total_tests'] >= 5:  # 至少5次测试
                accuracy = rule_data['success_count'] / rule_data['total_tests']
                rule_rankings.append({
                    'rule_id': rule_id,
                    'accuracy': round(accuracy, 4),
                    'total_tests': rule_data['total_tests'],
                    'success_count': rule_data['success_count']
                })
        
        rule_rankings.sort(key=lambda x: x['accuracy'], reverse=True)
        stats['top_rules'] = rule_rankings[:20]
        stats['total_rules_tested'] = len(rule_rankings)
        
        return stats
    
    def get_rule_stats(self, rule_id: str) -> Optional[Dict]:
        """获取指定铁律的统计"""
        if rule_id not in self.data['rule_stats']:
            return None
        
        rule_data = self.data['rule_stats'][rule_id]
        total = rule_data['total_tests']
        
        return {
            'rule_id': rule_id,
            'total_tests': total,
            'success_count': rule_data['success_count'],
            'failure_count': rule_data['failure_count'],
            'accuracy': round(rule_data['success_count'] / total, 4) if total > 0 else 0,
            'avg_return': round(rule_data['total_return'] / total, 4) if total > 0 else 0
        }
    
    def generate_report(self) -> Dict:
        """生成完整的累计报告"""
        stats = self.get_cumulative_stats()
        
        report = {
            'title': '铁律验证累计报告',
            'generated_at': datetime.now().isoformat(),
            'summary': stats,
            'rule_statistics': self._generate_rule_report(),
            'validation_history': self.data['validation_runs'][-20:],  # 最近20次
            'batch_history': self.data['batch_history'][-20:]
        }
        
        return report
    
    def _generate_rule_report(self) -> List[Dict]:
        """生成铁律报告"""
        rules = []
        
        for rule_id, rule_data in self.data['rule_stats'].items():
            total = rule_data['total_tests']
            if total >= 5:  # 至少5次测试
                accuracy = rule_data['success_count'] / total
                
                # 计算收益率统计
                returns = rule_data.get('recent_returns', [])
                avg_return = sum(returns) / len(returns) if returns else 0
                max_return = max(returns) if returns else 0
                min_return = min(returns) if returns else 0
                
                rules.append({
                    'rule_id': rule_id,
                    'total_tests': total,
                    'success_count': rule_data['success_count'],
                    'failure_count': rule_data['failure_count'],
                    'accuracy': round(accuracy, 4),
                    'avg_return': round(avg_return, 4),
                    'max_return': round(max_return, 4),
                    'min_return': round(min_return, 4),
                    'status': self._determine_status(accuracy)
                })
        
        return sorted(rules, key=lambda x: x['accuracy'], reverse=True)
    
    def _determine_status(self, accuracy: float) -> str:
        """确定铁律状态"""
        if accuracy >= 0.70:
            return 'active'
        elif accuracy >= 0.60:
            return 'demoted'
        elif accuracy >= 0.50:
            return 'dormant'
        else:
            return 'eliminated'
    
    def _save(self):
        """保存到文件"""
        try:
            with open(self.cumulative_file, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存累计报告失败: {e}")
    
    def export_markdown(self, output_path: str = None) -> str:
        """导出为Markdown格式"""
        if output_path is None:
            results_dir = PATH_CONFIG['results_dir']
            output_path = os.path.join(results_dir, 'cumulative_report.md')
        
        report = self.generate_report()
        stats = report['summary']
        
        md = f"""# 铁律验证累计报告

生成时间: {report['generated_at']}

## 总体统计

| 指标 | 数值 |
|------|------|
| 总验证次数 | {stats['total_validations']} |
| 总样本数 | {stats['total_samples']} |
| 成功样本 | {stats['total_success']} |
| 失败样本 | {stats['total_failure']} |
| 总体准确率 | {stats['overall_accuracy']:.2%} |
| 平均成功率 | {stats['avg_success_rate']:.2%} |
| 平均收益率 | {stats['avg_return']:.2%} |
| 测试铁律数 | {stats['total_rules_tested']} |

## 铁律准确率排名 (Top 20)

| 排名 | 铁律ID | 准确率 | 测试次数 | 成功次数 |
|------|--------|--------|----------|----------|
"""
        
        for i, rule in enumerate(stats['top_rules'][:20], 1):
            status_icon = {'active': '🟢', 'demoted': '🟡', 'dormant': '🟠', 'eliminated': '🔴'}
            md += f"| {i} | {rule['rule_id']} | {rule['accuracy']:.2%} | {rule['total_tests']} | {rule['success_count']} |\n"
        
        md += """
## 验证历史 (最近20次)

| 时间 | 批次ID | 样本数 | 成功率 |
|------|--------|--------|--------|
"""
        
        for run in report['validation_history'][-20:]:
            md += f"| {run['timestamp'][:19]} | {run['batch_id']} | {run['sample_count']} | {run['accuracy']:.2%} |\n"
        
        md += f"""
---
*本报告由铁律验证系统自动生成*
"""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(md)
        
        return output_path
    
    def clear(self):
        """清除所有累计数据"""
        self.data = self._create_empty_data()
        self._save()


# 全局实例
_accumulator = None


def get_accumulator() -> ReportAccumulator:
    """获取累计器单例"""
    global _accumulator
    if _accumulator is None:
        _accumulator = ReportAccumulator()
    return _accumulator
