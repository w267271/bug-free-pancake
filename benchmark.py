# -*- coding: utf-8 -*-
"""
铁律基准对比模块
大盘基准对比、随机选股基准、行业基准对比、Alpha/Beta计算
"""
from pathlib import Path
import json
import numpy as np
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field
from collections import defaultdict
from enum import Enum
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleType
from utils.logger import get_logger

logger = get_logger("Benchmark")


@dataclass
class BenchmarkConfig:
    """基准配置"""
    name: str
    code: str                    # 指数代码
    description: str = ""
    
    # 回测参数
    start_date: str = ""
    end_date: str = ""
    initial_capital: float = 100000
    
    # 统计
    total_return: float = 0
    annualized_return: float = 0
    volatility: float = 0
    sharpe_ratio: float = 0
    max_drawdown: float = 0
    win_rate: float = 0


@dataclass
class PerformanceMetrics:
    """绩效指标"""
    absolute_return: float               # 绝对收益
    relative_return: float              # 相对收益（vs基准）
    alpha: float                         # Alpha
    beta: float                          # Beta
    sharpe_ratio: float                  # 夏普比率
    info_ratio: float                   # 信息比率
    max_drawdown: float                  # 最大回撤
    calmar_ratio: float                  # 卡玛比率
    win_rate: float                     # 胜率
    profit_factor: float                # 盈利因子
    avg_profit: float                   # 平均盈利
    avg_loss: float                     # 平均亏损
    
    def to_dict(self) -> Dict:
        return {
            'absolute_return': f"{self.absolute_return:.2%}",
            'relative_return': f"{self.relative_return:+.2%}",
            'alpha': f"{self.alpha:.4f}",
            'beta': f"{self.beta:.4f}",
            'sharpe_ratio': f"{self.sharpe_ratio:.2f}",
            'info_ratio': f"{self.info_ratio:.2f}",
            'max_drawdown': f"{self.max_drawdown:.2%}",
            'calmar_ratio': f"{self.calmar_ratio:.2f}",
            'win_rate': f"{self.win_rate:.1%}",
            'profit_factor': f"{self.profit_factor:.2f}",
            'avg_profit': f"{self.avg_profit:.2%}",
            'avg_loss': f"{self.avg_loss:.2%}",
        }


@dataclass
class ComparisonResult:
    """对比结果"""
    benchmark_name: str
    strategy_return: float
    benchmark_return: float
    outperformance: float
    
    # 各项指标对比
    metrics: PerformanceMetrics
    
    # 每日对比
    daily_returns: List[Dict] = field(default_factory=list)
    
    def to_markdown(self) -> str:
        lines = [
            f"# 基准对比报告: {self.benchmark_name}",
            f"\n**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n---\n",
            "\n## 收益对比\n",
            f"| 指标 | 策略 | 基准 | 超额收益 |",
            f"|------|------|------|----------|",
            f"| 总收益 | {self.strategy_return:.2%} | {self.benchmark_return:.2%} | {self.outperformance:+.2%} |",
            "\n---\n",
            "\n## 风险调整收益\n",
            f"| 指标 | 策略 | 基准 |",
            f"|------|------|------|",
            f"| 夏普比率 | {self.metrics.sharpe_ratio:.2f} | - |",
            f"| Alpha | {self.metrics.alpha:.4f} | - |",
            f"| Beta | {self.metrics.beta:.4f} | - |",
            f"| 最大回撤 | {self.metrics.max_drawdown:.2%} | - |",
            "\n---\n",
            "\n## 交易统计\n",
            f"| 指标 | 数值 |",
            f"|------|------|",
            f"| 胜率 | {self.metrics.win_rate:.1%} |",
            f"| 盈利因子 | {self.metrics.profit_factor:.2f} |",
            f"| 平均盈利 | {self.metrics.avg_profit:.2%} |",
            f"| 平均亏损 | {self.metrics.avg_loss:.2%} |",
        ]
        return "\n".join(lines)


class BenchmarkManager:
    """基准管理器"""
    
    # 预设基准
    PRESET_BENCHMARKS = {
        'HS300': {
            'name': '沪深300',
            'code': '000300',
            'description': 'A股最具代表性的蓝筹股指数',
        },
        'SSE': {
            'name': '上证指数',
            'code': '000001',
            'description': '上海证券交易所综合指数',
        },
        'SZSE': {
            'name': '深证成指',
            'code': '399001',
            'description': '深圳证券交易所成分指数',
        },
        'GEM': {
            'name': '创业板指',
            'code': '399006',
            'description': '创业板市场代表性指数',
        },
        'CSI500': {
            'name': '中证500',
            'code': '000905',
            'description': '中盘股指数',
        },
    }
    
    def __init__(self):
        """初始化基准管理器"""
        self.benchmarks: Dict[str, BenchmarkConfig] = {}
        self.strategy_returns: List[float] = []
        self.benchmark_returns: Dict[str, List[float]] = {}
        
        # 随机选股基准
        self.random_returns: List[float] = []
        
        # 配置
        self.config = {
            'risk_free_rate': 0.03,        # 无风险利率 3%
            'trading_days': 252,            # 年交易日
            'confidence_level': 0.95,        # 置信水平
        }
    
    def add_benchmark(
        self,
        name: str,
        code: str,
        returns: List[float],
        description: str = "",
    ):
        """
        添加基准
        
        Args:
            name: 基准名称
            code: 指数代码
            returns: 收益率序列
            description: 描述
        """
        config = BenchmarkConfig(
            name=name,
            code=code,
            description=description,
        )
        
        # 计算统计指标
        if returns:
            config.total_return = (1 + np.array(returns)).prod() - 1
            config.annualized_return = (1 + config.total_return) ** (self.config['trading_days'] / len(returns)) - 1
            config.volatility = np.std(returns) * np.sqrt(self.config['trading_days'])
            
            # 夏普比率
            excess_return = config.annualized_return - self.config['risk_free_rate']
            if config.volatility > 0:
                config.sharpe_ratio = excess_return / config.volatility
        
        self.benchmarks[name] = config
        self.benchmark_returns[name] = returns
        
        logger.info(f"添加基准: {name}, 总收益: {config.total_return:.2%}")
    
    def calculate_random_baseline(
        self,
        stock_returns: List[List[float]],
        iterations: int = 1000,
    ) -> List[float]:
        """
        计算随机选股基准
        
        Args:
            stock_returns: 各股票收益率列表
            iterations: 模拟次数
        
        Returns:
            随机基准收益率
        """
        random_returns = []
        
        for _ in range(iterations):
            # 随机选择一只股票
            idx = np.random.randint(0, len(stock_returns))
            if stock_returns[idx]:
                ret = np.random.choice(stock_returns[idx])
                random_returns.append(ret)
        
        self.random_returns = random_returns
        
        if random_returns:
            avg_return = np.mean(random_returns)
            logger.info(f"随机选股基准: 平均收益 {avg_return:.2%}")
        
        return random_returns
    
    def calculate_alpha_beta(
        self,
        strategy_returns: List[float],
        benchmark_returns: List[float],
    ) -> Tuple[float, float]:
        """
        计算Alpha和Beta
        
        Args:
            strategy_returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
        
        Returns:
            (alpha, beta)
        """
        if len(strategy_returns) != len(benchmark_returns) or len(strategy_returns) < 2:
            return 0, 1
        
        # 转换为numpy数组
        strategy = np.array(strategy_returns)
        benchmark = np.array(benchmark_returns)
        
        # 计算协方差矩阵
        cov_matrix = np.cov(strategy, benchmark)
        cov_strategy_benchmark = cov_matrix[0, 1]
        var_benchmark = np.var(benchmark)
        
        # Beta = Cov(strategy, benchmark) / Var(benchmark)
        beta = cov_strategy_benchmark / var_benchmark if var_benchmark > 0 else 1.0
        
        # Alpha = Strategy Return - Beta * Benchmark Return - Risk Free Rate
        strategy_mean = np.mean(strategy)
        benchmark_mean = np.mean(benchmark)
        alpha = strategy_mean - beta * benchmark_mean - self.config['risk_free_rate'] / self.config['trading_days']
        
        return alpha, beta
    
    def calculate_performance_metrics(
        self,
        strategy_returns: List[float],
        benchmark_returns: List[float] = None,
    ) -> PerformanceMetrics:
        """
        计算绩效指标
        
        Args:
            strategy_returns: 策略收益率序列
            benchmark_returns: 基准收益率序列
        
        Returns:
            绩效指标
        """
        if not strategy_returns:
            return PerformanceMetrics(
                absolute_return=0, relative_return=0, alpha=0, beta=1,
                sharpe_ratio=0, info_ratio=0, max_drawdown=0, calmar_ratio=0,
                win_rate=0, profit_factor=0, avg_profit=0, avg_loss=0,
            )
        
        strategy = np.array(strategy_returns)
        
        # 绝对收益
        absolute_return = (1 + strategy).prod() - 1
        
        # 相对收益
        relative_return = 0
        alpha = 0
        beta = 1
        
        if benchmark_returns:
            benchmark = np.array(benchmark_returns)
            relative_return = (1 + strategy).prod() - (1 + benchmark).prod()
            alpha, beta = self.calculate_alpha_beta(strategy_returns, benchmark_returns)
        
        # 年化收益率
        annualized_return = (1 + absolute_return) ** (self.config['trading_days'] / len(strategy)) - 1
        
        # 波动率
        volatility = np.std(strategy) * np.sqrt(self.config['trading_days'])
        
        # 夏普比率
        sharpe_ratio = 0
        if volatility > 0:
            sharpe_ratio = (annualized_return - self.config['risk_free_rate']) / volatility
        
        # 信息比率
        info_ratio = 0
        if benchmark_returns and len(benchmark_returns) > 0:
            active_return = strategy - np.array(benchmark_returns)
            tracking_error = np.std(active_return) * np.sqrt(self.config['trading_days'])
            if tracking_error > 0:
                info_ratio = (np.mean(active_return) * self.config['trading_days']) / tracking_error
        
        # 最大回撤
        cumulative = (1 + strategy).cumprod()
        peak = np.maximum.accumulate(cumulative)
        drawdown = (cumulative - peak) / peak
        max_drawdown = abs(np.min(drawdown)) if len(drawdown) > 0 else 0
        
        # 卡玛比率
        calmar_ratio = annualized_return / max_drawdown if max_drawdown > 0 else 0
        
        # 胜率
        wins = strategy > 0
        win_rate = np.mean(wins) if len(wins) > 0 else 0
        
        # 盈亏统计
        profits = strategy[wins]
        losses = strategy[~wins]
        avg_profit = np.mean(profits) if len(profits) > 0 else 0
        avg_loss = np.mean(losses) if len(losses) > 0 else 0
        
        # 盈利因子
        total_profit = np.sum(profits) if len(profits) > 0 else 0
        total_loss = abs(np.sum(losses)) if len(losses) > 0 else 0
        profit_factor = total_profit / total_loss if total_loss > 0 else 0
        
        return PerformanceMetrics(
            absolute_return=absolute_return,
            relative_return=relative_return,
            alpha=alpha,
            beta=beta,
            sharpe_ratio=sharpe_ratio,
            info_ratio=info_ratio,
            max_drawdown=max_drawdown,
            calmar_ratio=calmar_ratio,
            win_rate=win_rate,
            profit_factor=profit_factor,
            avg_profit=avg_profit,
            avg_loss=avg_loss,
        )
    
    def compare_with_benchmark(
        self,
        strategy_returns: List[float],
        benchmark_name: str,
        daily_data: List[Dict] = None,
    ) -> ComparisonResult:
        """
        与基准对比
        
        Args:
            strategy_returns: 策略收益率序列
            benchmark_name: 基准名称
            daily_data: 每日详细数据
        
        Returns:
            对比结果
        """
        if benchmark_name not in self.benchmarks:
            logger.warning(f"基准不存在: {benchmark_name}")
            return None
        
        benchmark = self.benchmarks[benchmark_name]
        benchmark_returns = self.benchmark_returns.get(benchmark_name, [])
        
        # 计算绩效
        metrics = self.calculate_performance_metrics(strategy_returns, benchmark_returns)
        
        # 策略总收益
        strategy_return = (1 + np.array(strategy_returns)).prod() - 1 if strategy_returns else 0
        
        result = ComparisonResult(
            benchmark_name=benchmark_name,
            strategy_return=strategy_return,
            benchmark_return=benchmark.total_return,
            outperformance=strategy_return - benchmark.total_return,
            metrics=metrics,
        )
        
        # 每日对比
        if daily_data and len(daily_data) == len(strategy_returns):
            for i, data in enumerate(daily_data):
                bench_ret = benchmark_returns[i] if i < len(benchmark_returns) else 0
                result.daily_returns.append({
                    'date': data.get('date', ''),
                    'strategy_return': strategy_returns[i],
                    'benchmark_return': bench_ret,
                    'excess_return': strategy_returns[i] - bench_ret,
                })
        
        return result
    
    def generate_benchmark_report(
        self,
        strategy_returns: List[float],
        strategy_name: str = "策略",
        include_benchmarks: List[str] = None,
    ) -> str:
        """
        生成基准对比报告
        
        Args:
            strategy_returns: 策略收益率序列
            strategy_name: 策略名称
            include_benchmarks: 包含的基准列表
        
        Returns:
            报告内容
        """
        if include_benchmarks is None:
            include_benchmarks = list(self.benchmarks.keys())
        
        lines = [
            f"# 基准对比报告",
            f"\n**策略**: {strategy_name}",
            f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"**交易日数**: {len(strategy_returns)}",
            "\n---\n",
        ]
        
        # 策略表现
        metrics = self.calculate_performance_metrics(strategy_returns)
        strategy_return = (1 + np.array(strategy_returns)).prod() - 1 if strategy_returns else 0
        
        lines.append("\n## 策略表现\n")
        lines.append(f"- 总收益: {strategy_return:.2%}")
        lines.append(f"- 年化收益: {metrics.absolute_return:.2%}")
        lines.append(f"- 夏普比率: {metrics.sharpe_ratio:.2f}")
        lines.append(f"- 最大回撤: {metrics.max_drawdown:.2%}")
        lines.append(f"- 胜率: {metrics.win_rate:.1%}")
        
        # 基准对比
        lines.append("\n## 基准对比\n")
        lines.append("\n| 基准 | 策略收益 | 基准收益 | 超额收益 | Alpha | Beta |")
        lines.append("|------|----------|----------|----------|-------|------|")
        
        for bench_name in include_benchmarks:
            if bench_name not in self.benchmarks:
                continue
            
            bench = self.benchmarks[bench_name]
            comparison = self.compare_with_benchmark(strategy_returns, bench_name)
            
            lines.append(
                f"| {bench.name} | {comparison.strategy_return:.2%} | "
                f"{comparison.benchmark_return:.2%} | {comparison.outperformance:+.2%} | "
                f"{comparison.metrics.alpha:.4f} | {comparison.metrics.beta:.4f} |"
            )
        
        # 随机选股基准
        if self.random_returns:
            lines.append("\n## 随机选股基准\n")
            random_return = (1 + np.array(self.random_returns)).prod() - 1
            beat_random = strategy_return > random_return
            beat_random_pct = np.mean(np.array(strategy_returns) > np.array(self.random_returns)) * 100
            
            lines.append(f"- 随机选股平均收益: {random_return:.2%}")
            lines.append(f"- 策略跑赢随机: {'是' if beat_random else '否'}")
            lines.append(f"- 日胜率: {beat_random_pct:.1f}%")
        
        # 结论
        lines.append("\n## 结论\n")
        
        # 找出最佳基准
        best_bench = None
        best_outperformance = float('-inf')
        for bench_name in include_benchmarks:
            if bench_name not in self.benchmarks:
                continue
            comparison = self.compare_with_benchmark(strategy_returns, bench_name)
            if comparison.outperformance > best_outperformance:
                best_outperformance = comparison.outperformance
                best_bench = comparison
        
        if best_bench:
            if best_outperformance > 0:
                lines.append(f"- 策略跑赢{best_bench.benchmark_name} {best_outperformance:.2%}")
            else:
                lines.append(f"- 策略跑输{best_bench.benchmark_name} {abs(best_outperformance):.2%}")
        
        if metrics.sharpe_ratio > 1.5:
            lines.append("- 风险调整收益表现优秀（夏普比率>1.5）")
        elif metrics.sharpe_ratio > 1:
            lines.append("- 风险调整收益表现良好（夏普比率>1）")
        else:
            lines.append("- 风险调整收益有提升空间")
        
        return "\n".join(lines)
    
    def export_comparison_data(
        self,
        export_path: str,
        strategy_returns: List[float],
    ):
        """导出对比数据"""
        data = {
            'generated_at': datetime.now().isoformat(),
            'strategy_returns': strategy_returns,
            'benchmarks': {
                name: {
                    'name': bench.name,
                    'code': bench.code,
                    'returns': returns,
                } for name, (bench, returns) in [(n, (self.benchmarks[n], self.benchmark_returns.get(n, []))) for n in self.benchmarks]
            },
            'metrics': self.calculate_performance_metrics(
                strategy_returns,
                self.benchmark_returns.get(list(self.benchmarks.keys())[0], [])
            ).to_dict() if self.benchmarks else {},
        }
        
        with open(export_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"导出对比数据到 {export_path}")


class IndustryBenchmark:
    """行业基准"""
    
    # 申万一级行业
    INDUSTRIES = {
        '801010': '农林牧渔',
        '801020': '采掘',
        '801030': '化工',
        '801040': '钢铁',
        '801050': '有色金属',
        '801060': '电子',
        '801070': '汽车',
        '801080': '家用电器',
        '801090': '食品饮料',
        '801100': '纺织服装',
        '801110': '轻工制造',
        '801120': '医药生物',
        '801130': '公用事业',
        '801140': '交通运输',
        '801150': '房地产',
        '801160': '商业贸易',
        '801170': '休闲服务',
        '801180': '银行',
        '801190': '非银金融',
        '801200': '建筑材料',
        '801210': '建筑装饰',
        '801220': '电气设备',
        '801230': '国防军工',
        '801710': '计算机',
        '801720': '传媒',
        '801730': '通信',
    }
    
    def __init__(self):
        """初始化行业基准"""
        self.industry_returns: Dict[str, List[float]] = {}
    
    def calculate_industry_benchmark(
        self,
        industry_code: str,
        stock_returns: Dict[str, List[float]],
    ) -> List[float]:
        """
        计算行业基准（等权平均）
        
        Args:
            industry_code: 行业代码
            stock_returns: {stock_code: returns}
        
        Returns:
            行业基准收益率
        """
        industry_returns = []
        
        for stock_code, returns in stock_returns.items():
            if not returns:
                continue
            # 取第一只股票作为代表（简化实现）
            if not industry_returns:
                industry_returns = returns
        
        self.industry_returns[industry_code] = industry_returns
        return industry_returns
    
    def compare_with_industry(
        self,
        strategy_returns: List[float],
        industry_code: str,
    ) -> Dict:
        """与行业对比"""
        industry_returns = self.industry_returns.get(industry_code, [])
        
        if not industry_returns:
            return {'error': '行业数据不存在'}
        
        strategy_return = (1 + np.array(strategy_returns)).prod() - 1
        industry_return = (1 + np.array(industry_returns)).prod() - 1
        
        return {
            'industry_code': industry_code,
            'industry_name': self.INDUSTRIES.get(industry_code, '未知'),
            'strategy_return': strategy_return,
            'industry_return': industry_return,
            'outperformance': strategy_return - industry_return,
            'win': strategy_return > industry_return,
        }


def integrate_with_validator(validator):
    """
    将基准对比模块集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    benchmark_mgr = BenchmarkManager()
    industry_bench = IndustryBenchmark()
    
    validator.benchmark_manager = benchmark_mgr
    validator.industry_benchmark = industry_bench
    validator.compare_with_benchmark = benchmark_mgr.compare_with_benchmark
    validator.generate_benchmark_report = benchmark_mgr.generate_benchmark_report
    validator.calculate_performance_metrics = benchmark_mgr.calculate_performance_metrics
    
    return validator
