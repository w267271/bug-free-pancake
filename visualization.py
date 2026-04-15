# -*- coding: utf-8 -*-
"""
铁律可视化图表生成模块
生成准确率趋势图、收益分布直方图、相关性热力图等
"""
import json
import numpy as np
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logger import get_logger

logger = get_logger("Visualization")

# 尝试导入matplotlib
try:
    import matplotlib
    matplotlib.use('Agg')  # 使用非交互式后端
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import PercentFormatter
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    logger.warning("matplotlib未安装，将无法生成图表")

try:
    import seaborn as sns
    HAS_SEABORN = True
except ImportError:
    HAS_SEABORN = False


@dataclass
class ChartConfig:
    """图表配置"""
    title: str = ""
    xlabel: str = ""
    ylabel: str = ""
    figsize: Tuple[int, int] = (12, 6)
    dpi: int = 100
    style: str = "seaborn-v0_8-darkgrid" if HAS_SEABORN else "ggplot"
    colors: List[str] = None
    
    def __post_init__(self):
        if self.colors is None:
            self.colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                          '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']


@dataclass
class VisualizationResult:
    """可视化结果"""
    chart_type: str
    file_path: str
    description: str = ""
    
    def to_dict(self) -> Dict:
        return {
            'chart_type': self.chart_type,
            'file_path': self.file_path,
            'description': self.description,
        }


class VisualizationEngine:
    """可视化引擎"""
    
    def __init__(self, output_dir: str = None):
        """
        初始化可视化引擎
        
        Args:
            output_dir: 输出目录
        """
        if not HAS_MATPLOTLIB:
            logger.warning("matplotlib未安装，可视化功能不可用")
            return
        
        if output_dir is None:
            from config import PATH_CONFIG
            output_dir = str(Path(PATH_CONFIG['project_root']) / 'data' / 'charts')
        
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 样式设置
        plt.style.use('ggplot')
        if HAS_SEABORN:
            sns.set_palette("husl")
        
        # 存储生成的文件
        self.generated_charts: List[VisualizationResult] = []
    
    def _get_color(self, index: int) -> str:
        """获取颜色"""
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
                 '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
        return colors[index % len(colors)]
    
    def plot_accuracy_trend(
        self,
        rules_data: Dict[str, List[Tuple[str, float]]],
        title: str = "铁律准确率趋势",
        save_path: str = None,
    ) -> Optional[str]:
        """
        绘制铁律准确率趋势图
        
        Args:
            rules_data: {rule_id: [(date, accuracy), ...]}
            title: 标题
            save_path: 保存路径
        
        Returns:
            文件路径
        """
        if not HAS_MATPLOTLIB:
            return None
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        for i, (rule_id, data) in enumerate(rules_data.items()):
            if not data:
                continue
            
            dates = [datetime.strptime(d, '%Y-%m-%d') for d, _ in data]
            accuracies = [a for _, a in data]
            
            ax.plot(dates, accuracies, label=rule_id, 
                   color=self._get_color(i), linewidth=2, alpha=0.8)
        
        ax.axhline(y=0.6, color='red', linestyle='--', alpha=0.5, label='降级线(60%)')
        ax.axhline(y=0.5, color='darkred', linestyle='--', alpha=0.5, label='休眠线(50%)')
        ax.axhline(y=0.85, color='green', linestyle='--', alpha=0.5, label='升级线(85%)')
        
        ax.set_xlabel('日期', fontsize=12)
        ax.set_ylabel('准确率', fontsize=12)
        ax.set_title(title, fontsize=14, fontweight='bold')
        ax.yaxis.set_major_formatter(PercentFormatter(1.0))
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        
        # 格式化日期
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        plt.xticks(rotation=45)
        
        plt.tight_layout()
        
        if save_path is None:
            save_path = str(self.output_dir / f"accuracy_trend_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        result = VisualizationResult(
            chart_type='accuracy_trend',
            file_path=save_path,
            description=f'铁律准确率趋势图，包含{len(rules_data)}条铁律'
        )
        self.generated_charts.append(result)
        
        logger.info(f"生成准确率趋势图: {save_path}")
        return save_path
    
    def plot_return_distribution(
        self,
        returns: List[float],
        title: str = "收益分布",
        bins: int = 50,
        save_path: str = None,
    ) -> Optional[str]:
        """
        绘制收益分布直方图
        
        Args:
            returns: 收益率列表
            title: 标题
            bins: 分箱数
            save_path: 保存路径
        
        Returns:
            文件路径
        """
        if not HAS_MATPLOTLIB or not returns:
            return None
        
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
        
        # 直方图
        ax1.hist(returns, bins=bins, alpha=0.7, color='steelblue', edgecolor='white')
        ax1.axvline(x=0, color='red', linestyle='--', linewidth=2, label='零线')
        ax1.axvline(x=np.mean(returns), color='green', linestyle='-', linewidth=2, 
                   label=f'均值:{np.mean(returns):.2%}')
        ax1.set_xlabel('收益率', fontsize=12)
        ax1.set_ylabel('频数', fontsize=12)
        ax1.set_title(f'{title} - 分布', fontsize=12, fontweight='bold')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # 箱线图
        bp = ax2.boxplot(returns, patch_artist=True)
        bp['boxes'][0].set_facecolor('lightblue')
        ax2.set_ylabel('收益率', fontsize=12)
        ax2.set_title(f'{title} - 箱线图', fontsize=12, fontweight='bold')
        ax2.grid(True, alpha=0.3)
        
        # 添加统计信息
        stats_text = f"均值: {np.mean(returns):.2%}\n中位数: {np.median(returns):.2%}\n标准差: {np.std(returns):.2%}"
        ax2.text(1.2, np.median(returns), stats_text, fontsize=10, 
                verticalalignment='center', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        
        if save_path is None:
            save_path = str(self.output_dir / f"return_dist_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        result = VisualizationResult(
            chart_type='return_distribution',
            file_path=save_path,
            description=f'收益分布直方图，{len(returns)}个样本'
        )
        self.generated_charts.append(result)
        
        logger.info(f"生成收益分布图: {save_path}")
        return save_path
    
    def plot_correlation_heatmap(
        self,
        correlation_matrix: np.ndarray,
        labels: List[str],
        title: str = "铁律相关性热力图",
        save_path: str = None,
    ) -> Optional[str]:
        """
        绘制相关性热力图
        
        Args:
            correlation_matrix: 相关性矩阵
            labels: 标签列表
            title: 标题
            save_path: 保存路径
        
        Returns:
            文件路径
        """
        if not HAS_MATPLOTLIB:
            return None
        
        fig, ax = plt.subplots(figsize=(12, 10))
        
        # 使用seaborn绘制热力图
        if HAS_SEABORN:
            sns.heatmap(correlation_matrix, 
                       xticklabels=labels, 
                       yticklabels=labels,
                       cmap='RdYlGn',
                       center=0,
                       annot=True,
                       fmt='.2f',
                       square=True,
                       linewidths=0.5,
                       cbar_kws={"shrink": 0.8},
                       ax=ax)
        else:
            im = ax.imshow(correlation_matrix, cmap='RdYlGn', vmin=-1, vmax=1)
            ax.set_xticks(range(len(labels)))
            ax.set_yticks(range(len(labels)))
            ax.set_xticklabels(labels, rotation=45, ha='right')
            ax.set_yticklabels(labels)
            
            # 添加数值标注
            for i in range(len(labels)):
                for j in range(len(labels)):
                    text = ax.text(j, i, f'{correlation_matrix[i, j]:.2f}',
                                  ha="center", va="center", color="black", fontsize=8)
            
            plt.colorbar(im, ax=ax)
        
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)
        
        plt.tight_layout()
        
        if save_path is None:
            save_path = str(self.output_dir / f"correlation_heatmap_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        result = VisualizationResult(
            chart_type='correlation_heatmap',
            file_path=save_path,
            description=f'铁律相关性热力图，{len(labels)}条铁律'
        )
        self.generated_charts.append(result)
        
        logger.info(f"生成相关性热力图: {save_path}")
        return save_path
    
    def plot_market_regime_pie(
        self,
        regime_counts: Dict[str, int],
        title: str = "市场状态分布",
        save_path: str = None,
    ) -> Optional[str]:
        """
        绘制市场状态饼图
        
        Args:
            regime_counts: {状态: 数量}
            title: 标题
            save_path: 保存路径
        
        Returns:
            文件路径
        """
        if not HAS_MATPLOTLIB or not regime_counts:
            return None
        
        fig, ax = plt.subplots(figsize=(10, 8))
        
        labels = list(regime_counts.keys())
        sizes = list(regime_counts.values())
        colors = [self._get_color(i) for i in range(len(labels))]
        
        # 分离小于5%的扇区
        explode = [0.05 if s/sum(sizes) < 0.05 else 0 for s in sizes]
        
        wedges, texts, autotexts = ax.pie(sizes, 
                                         explode=explode,
                                         labels=labels,
                                         colors=colors,
                                         autopct='%1.1f%%',
                                         shadow=True,
                                         startangle=90)
        
        # 美化文字
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontweight('bold')
        
        ax.set_title(title, fontsize=14, fontweight='bold')
        
        # 添加图例
        ax.legend(wedges, [f'{l}: {s}' for l, s in zip(labels, sizes)],
                 title="市场状态",
                 loc="center left",
                 bbox_to_anchor=(1, 0, 0.5, 1))
        
        plt.tight_layout()
        
        if save_path is None:
            save_path = str(self.output_dir / f"market_regime_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        result = VisualizationResult(
            chart_type='market_regime_pie',
            file_path=save_path,
            description=f'市场状态分布饼图'
        )
        self.generated_charts.append(result)
        
        logger.info(f"生成市场状态饼图: {save_path}")
        return save_path
    
    def plot_dashboard(
        self,
        metrics: Dict[str, Any],
        save_path: str = None,
    ) -> Optional[str]:
        """
        绘制验证进度仪表盘
        
        Args:
            metrics: 指标字典
            save_path: 保存路径
        
        Returns:
            文件路径
        """
        if not HAS_MATPLOTLIB:
            return None
        
        fig = plt.figure(figsize=(16, 10))
        
        # 创建子图布局
        gs = fig.add_gridspec(3, 3, hspace=0.3, wspace=0.3)
        
        # 1. 准确率仪表盘
        ax1 = fig.add_subplot(gs[0, 0])
        accuracy = metrics.get('accuracy', 0.5)
        self._draw_gauge(ax1, accuracy, '准确率', ['red', 'yellow', 'green'])
        
        # 2. 样本进度
        ax2 = fig.add_subplot(gs[0, 1])
        completed = metrics.get('completed_samples', 0)
        total = metrics.get('total_samples', 100)
        self._draw_progress_bar(ax2, completed, total, '验证进度')
        
        # 3. 胜率仪表盘
        ax3 = fig.add_subplot(gs[0, 2])
        win_rate = metrics.get('win_rate', 0.5)
        self._draw_gauge(ax3, win_rate, '胜率', ['red', 'yellow', 'green'])
        
        # 4. 收益曲线
        ax4 = fig.add_subplot(gs[1, :2])
        returns = metrics.get('returns', [])
        if returns:
            ax4.plot(range(len(returns)), returns, 'b-', linewidth=2)
            ax4.fill_between(range(len(returns)), returns, alpha=0.3)
            ax4.axhline(y=0, color='red', linestyle='--')
            ax4.set_title('累计收益曲线', fontsize=12, fontweight='bold')
            ax4.set_xlabel('交易次数')
            ax4.set_ylabel('累计收益')
            ax4.grid(True, alpha=0.3)
        
        # 5. 铁律状态分布
        ax5 = fig.add_subplot(gs[1, 2])
        rule_status = metrics.get('rule_status', {'active': 10, 'demoted': 5})
        if rule_status:
            bars = ax5.bar(rule_status.keys(), rule_status.values(), 
                          color=[self._get_color(i) for i in range(len(rule_status))])
            ax5.set_title('铁律状态分布', fontsize=12, fontweight='bold')
            ax5.set_ylabel('数量')
            plt.setp(ax5.xaxis.get_majorticklabels(), rotation=45, ha='right')
        
        # 6. 关键指标
        ax6 = fig.add_subplot(gs[2, :])
        ax6.axis('off')
        
        # 文本展示关键指标
        metrics_text = [
            f"总交易次数: {metrics.get('total_trades', 0)}",
            f"盈利次数: {metrics.get('winning_trades', 0)}",
            f"亏损次数: {metrics.get('losing_trades', 0)}",
            f"平均盈利: {metrics.get('avg_profit', 0):.2%}",
            f"平均亏损: {metrics.get('avg_loss', 0):.2%}",
            f"最大回撤: {metrics.get('max_drawdown', 0):.2%}",
            f"夏普比率: {metrics.get('sharpe_ratio', 0):.2f}",
        ]
        
        ax6.text(0.1, 0.5, '\n'.join(metrics_text), fontsize=12, 
                family='monospace', verticalalignment='center',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.suptitle('铁律验证仪表盘', fontsize=16, fontweight='bold', y=0.98)
        
        if save_path is None:
            save_path = str(self.output_dir / f"dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        result = VisualizationResult(
            chart_type='dashboard',
            file_path=save_path,
            description='铁律验证综合仪表盘'
        )
        self.generated_charts.append(result)
        
        logger.info(f"生成仪表盘: {save_path}")
        return save_path
    
    def _draw_gauge(self, ax, value: float, title: str, colors: List[str]):
        """绘制仪表盘"""
        ax.set_xlim(-1, 1)
        ax.set_ylim(-0.1, 1.2)
        ax.set_aspect('equal')
        ax.axis('off')
        
        # 绘制半圆弧
        theta = np.linspace(0, np.pi, 100)
        r = 0.8
        
        for i, (start, end, color) in enumerate([(0, 0.33, colors[0]), 
                                                   (0.33, 0.66, colors[1]),
                                                   (0.66, 1, colors[2])]):
            t = np.linspace(start * np.pi, end * np.pi, 50)
            ax.plot(r * np.cos(t), r * np.sin(t), color=color, linewidth=20, alpha=0.5)
        
        # 绘制指针
        angle = (1 - value) * np.pi
        ax.arrow(0, 0, 0.6 * np.cos(angle), 0.6 * np.sin(angle),
                head_width=0.05, head_length=0.05, fc='black', ec='black')
        
        # 绘制数值
        ax.text(0, -0.15, f'{value:.1%}', fontsize=16, ha='center', fontweight='bold')
        ax.text(0, 1.05, title, fontsize=12, ha='center')
    
    def _draw_progress_bar(self, ax, value: int, total: int, title: str):
        """绘制进度条"""
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.axis('off')
        
        progress = value / total if total > 0 else 0
        
        # 背景
        ax.add_patch(plt.Rectangle((0.1, 0.4), 0.8, 0.2, facecolor='lightgray', edgecolor='none'))
        # 进度
        ax.add_patch(plt.Rectangle((0.1, 0.4), 0.8 * progress, 0.2, facecolor='steelblue', edgecolor='none'))
        
        ax.text(0.5, 0.7, title, fontsize=12, ha='center')
        ax.text(0.5, 0.3, f'{value}/{total} ({progress:.1%})', fontsize=14, ha='center', fontweight='bold')
    
    def plot_icir_trend(
        self,
        icir_data: Dict[str, List[Tuple[str, float]]],
        title: str = "ICIR趋势",
        save_path: str = None,
    ) -> Optional[str]:
        """绘制ICIR趋势图"""
        if not HAS_MATPLOTLIB:
            return None
        
        fig, axes = plt.subplots(2, 1, figsize=(12, 8), sharex=True)
        
        for i, (rule_id, data) in enumerate(icir_data.items()):
            if not data:
                continue
            
            dates = [datetime.strptime(d, '%Y-%m-%d') for d, _ in data]
            values = [v for _, v in data]
            
            axes[0].plot(dates, values, label=rule_id, 
                       color=self._get_color(i), linewidth=2, alpha=0.8)
            axes[1].bar(dates, values, color=self._get_color(i), alpha=0.5)
        
        axes[0].set_ylabel('IC', fontsize=12)
        axes[0].legend(loc='best')
        axes[0].grid(True, alpha=0.3)
        axes[0].axhline(y=0, color='red', linestyle='--')
        
        axes[1].set_ylabel('IC', fontsize=12)
        axes[1].set_xlabel('日期', fontsize=12)
        axes[1].grid(True, alpha=0.3)
        axes[1].axhline(y=0, color='red', linestyle='--')
        
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        plt.xticks(rotation=45)
        
        plt.suptitle(title, fontsize=14, fontweight='bold')
        plt.tight_layout()
        
        if save_path is None:
            save_path = str(self.output_dir / f"icir_trend_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
        
        plt.savefig(save_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        result = VisualizationResult(
            chart_type='icir_trend',
            file_path=save_path,
            description=f'ICIR趋势图'
        )
        self.generated_charts.append(result)
        
        return save_path
    
    def export_charts_summary(self) -> str:
        """导出图表清单"""
        summary = {
            'generated_at': datetime.now().isoformat(),
            'total_charts': len(self.generated_charts),
            'charts': [c.to_dict() for c in self.generated_charts],
        }
        
        summary_path = str(self.output_dir / f"charts_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        return summary_path
    
    def generate_all_charts(
        self,
        rulesets: List[Any],
        validation_results: Dict[str, Any],
    ) -> List[str]:
        """
        生成所有图表
        
        Args:
            rulesets: 铁律集合
            validation_results: 验证结果
        
        Returns:
            生成的文件路径列表
        """
        paths = []
        
        # 准确率趋势图
        accuracy_data = {}
        for rs in rulesets:
            for rule in rs.rules:
                if rule.test_history:
                    accuracy_data[rule.id] = [
                        (h.get('date', ''), h.get('accuracy', 0)) 
                        for h in rule.test_history[-30:]
                    ]
        
        if accuracy_data:
            path = self.plot_accuracy_trend(accuracy_data)
            if path:
                paths.append(path)
        
        # 收益分布图
        returns = validation_results.get('returns', [])
        if returns:
            path = self.plot_return_distribution(returns)
            if path:
                paths.append(path)
        
        # 仪表盘
        path = self.plot_dashboard(validation_results)
        if path:
            paths.append(path)
        
        return paths


def integrate_with_validator(validator):
    """
    将可视化模块集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    viz_engine = VisualizationEngine()
    
    validator.viz_engine = viz_engine
    validator.plot_accuracy_trend = viz_engine.plot_accuracy_trend
    validator.plot_return_distribution = viz_engine.plot_return_distribution
    validator.plot_correlation_heatmap = viz_engine.plot_correlation_heatmap
    validator.plot_market_regime_pie = viz_engine.plot_market_regime_pie
    validator.plot_dashboard = viz_engine.plot_dashboard
    validator.generate_all_charts = viz_engine.generate_all_charts
    
    return validator
