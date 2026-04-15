# -*- coding: utf-8 -*-
"""
铁律验证引擎
"""
import random
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

def parse_date(d):
    """解析日期，支持多种格式"""
    from datetime import datetime, date
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
            try:
                return datetime.strptime(d, fmt).date()
            except:
                pass
    return None



from config import VALIDATION_CONFIG, MARKET_REGIME_CONFIG
from models.rule import Rule, RuleType, TermType, RuleStatus
from models.sample import ValidationSample, SampleStatus, StockInfo, PriceData, PriceDataStatus
from models.result import (
    ValidationReport, SampleTestResult, RuleTestResult, RuleStatistics
)
from core.data_fetcher import DataFetcher
from core.rule_parser import RuleParser
from utils.logger import get_logger
from utils.helpers import (
    generate_id, generate_sample_id, set_random_seed,
    select_random_stocks, calculate_max_drawdown, calculate_confidence_interval,
    safe_float
)

# 导入新增的高级功能模块
from core.look_ahead_check import LookAheadChecker, LookAheadCheckResult
from core.special_cases import SpecialCaseHandler, handle_special_cases
from core.sample_selector import SampleSelector

logger = get_logger("RuleValidator")


class RuleValidator:
    """铁律验证引擎"""
    
    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.rule_parser = RuleParser()
        self.samples: List[ValidationSample] = []
        self.results: List[SampleTestResult] = []
        
        # 初始化高级功能模块
        self.look_ahead_checker = LookAheadChecker()
        self.special_case_handler = SpecialCaseHandler()
        self.sample_selector = SampleSelector()
        
        # 设置随机种子
        set_random_seed()
    
    def initialize(self, rule_library_path: str):
        """初始化验证器"""
        logger.info("初始化铁律验证引擎...")
        
        # 解析铁律库
        self.rule_parser.parse_rule_library(rule_library_path)
        
        logger.info(f"初始化完成: {len(self.rule_parser.rules)} 条铁律")
        return self
    
    def run_validation_round(
        self,
        sample_size: int = None,
        start_date: date = None,
        end_date: date = None,
        term_type: TermType = TermType.SHORT
    ) -> ValidationReport:
        """
        运行一轮验证（集成前视偏差检查和特殊行情处理）
        
        Args:
            sample_size: 样本数量
            start_date: 开始日期
            end_date: 结束日期
            term_type: 周期类型
        
        Returns:
            验证报告
        """
        if sample_size is None:
            sample_size = VALIDATION_CONFIG['sample_size']
        
        if end_date is None:
            end_date = date.today()
        
        if start_date is None:
            # 默认回溯250个交易日
            start_date = end_date - timedelta(days=250)
        
        # 获取持仓天数
        holding_days = VALIDATION_CONFIG['position_cycle'].get(term_type.value, 3)
        
        logger.info(f"开始验证: 样本数={sample_size}, 日期范围={start_date}至{end_date}, 持仓={holding_days}天")
        
        # 1. 获取股票列表
        stock_list = self.data_fetcher.get_stock_list()
        if not stock_list:
            logger.error("获取股票列表失败")
            return None
        
        logger.info(f"获取股票列表: {len(stock_list)} 只")
        
        # 2. 使用时间分散抽样选择样本
        selection_result = self.sample_selector.select_samples(
            stock_list,
            (start_date, end_date),
            sample_size
        )
        selected_stocks = selection_result.selected_stocks
        logger.info(f"时间分散抽样: {len(selected_stocks)} 只")
        
        # 3. 生成验证样本
        self.samples = []
        for stock in selected_stocks:
            sample = self._create_sample(
                stock, start_date, end_date, holding_days
            )
            if sample:
                self.samples.append(sample)
        
        logger.info(f"生成样本: {len(self.samples)} 个")
        
        # 4. 前视偏差检查
        look_ahead_violations = []
        for sample in self.samples:
            check_result = self.look_ahead_checker.check_sample(sample)
            if not check_result.is_valid:
                look_ahead_violations.extend(check_result.violations)
        
        if look_ahead_violations:
            logger.warning(f"发现 {len(look_ahead_violations)} 个前视偏差问题")
        
        # 5. 特殊行情处理
        self.samples, special_records, special_stats = handle_special_cases(self.samples)
        logger.info(f"特殊行情处理后: {len(self.samples)} 个有效样本")
        
        # 6. 执行验证
        self.results = []
        for sample in self.samples:
            result = self._validate_sample(sample)
            if result:
                self.results.append(result)
        
        logger.info(f"完成验证: {len(self.results)} 个结果")
        
        # 7. 生成报告
        report = self._generate_report(term_type, start_date, end_date)
        
        # 添加高级功能信息到报告
        report.look_ahead_violations = look_ahead_violations
        report.special_case_stats = special_stats.to_dict() if special_stats else {}
        
        return report
    
    def _create_sample(
        self,
        stock: Dict,
        start_date: date,
        end_date: date,
        holding_days: int
    ) -> Optional[ValidationSample]:
        """创建验证样本"""
        stock_code = stock['code']
        
        # 获取股票信息
        stock_info = self.data_fetcher.get_stock_info(stock_code)
        if stock_info is None:
            stock_info = StockInfo(
                code=stock_code,
                name=stock.get('name', ''),
                market=stock.get('market', 'A'),
                sector=stock.get('sector', ''),
            )
        
        # 获取价格历史
        prices = self.data_fetcher.get_price_history(stock_code, days=250)
        if len(prices) < holding_days + 10:
            return None
        
        # 计算技术指标
        prices = self.data_fetcher.calculate_indicators(prices)
        
        # 随机选择触发日期
        def parse_date(d):
            """解析日期"""
            if isinstance(d, date):
                return d
            if isinstance(d, str):
                for fmt in ["%Y-%m-%d", "%Y%m%d", "%Y/%m/%d"]:
                    try:
                        return datetime.strptime(d, fmt).date()
                    except:
                        pass
            return None
        
        start_date_obj = parse_date(start_date) if not isinstance(start_date, date) else start_date
        end_date_obj = parse_date(end_date) if not isinstance(end_date, date) else end_date
        
        available_dates = [
            p.date for p in prices
            if parse_date(p.date) and start_date_obj <= parse_date(p.date) <= end_date_obj
            and p.status == PriceDataStatus.NORMAL
        ]
        
        if not available_dates:
            return None
        
        trigger_date = random.choice(available_dates)
        
        # 找到触发日的价格
        trigger_price = None
        for p in prices:
            p_date = parse_date(p.date)
            if p_date and p_date == trigger_date:
                trigger_price = p.close
                break
        
        if trigger_price is None or trigger_price <= 0:
            return None
        
        # 创建样本
        sample = ValidationSample(
            sample_id=generate_sample_id(stock_code, trigger_date),
            stock_info=stock_info,
            trigger_date=trigger_date,
            trigger_price=trigger_price,
            holding_days=holding_days,
            prices=prices,
            status=SampleStatus.PENDING,
        )
        
        # 识别触发铁律
        sample.triggered_rules = self._identify_triggered_rules(sample)
        
        return sample
    
    def _identify_triggered_rules(self, sample: ValidationSample) -> List[str]:
        """识别触发的铁律"""
        triggered = []
        
        # 获取触发日的价格数据
        trigger_price_data = None
        trigger_date_obj = parse_date(sample.trigger_date) if not isinstance(sample.trigger_date, date) else sample.trigger_date
        for p in sample.prices:
            p_date = parse_date(p.date)
            if p_date and p_date == trigger_date_obj:
                trigger_price_data = p
                break
        
        if trigger_price_data is None:
            return triggered
        
        # 检查每条铁律
        for rule in self.rule_parser.rules:
            if self._check_rule_trigger(rule, trigger_price_data, sample):
                triggered.append(rule.id)
        
        return triggered
    
    def _check_rule_trigger(
        self,
        rule: Rule,
        price_data: PriceData,
        sample: ValidationSample
    ) -> bool:
        """检查铁律是否触发"""
        try:
            # 根据铁律ID检查不同的条件
            rule_id = rule.id
            
            # S03: 高价股禁买
            if rule_id == 'S03':
                return price_data.close > 100
            
            # S04: 追涨停板
            if rule_id == 'S04':
                return price_data.status == PriceDataStatus.LIMIT_UP
            
            # N01: 高位股禁买
            if rule_id == 'N01':
                if len(sample.prices) >= 250:
                    high_250 = max(p.high for p in sample.prices[-250:])
                    return price_data.close >= high_250 * 0.8  # 距高点<20%
            
            # N05: 次新股禁买
            if rule_id == 'N05':
                if sample.stock_info.list_date:
                    trade_days = (sample.trigger_date - sample.stock_info.list_date).days
                    return trade_days < 60
            
            # N09: 股价10-50元区间
            if rule_id == 'N09':
                return 10 <= price_data.close <= 50
            
            # N06: 主力净流入+市值+股价
            if rule_id == 'N06':
                return (
                    price_data.main_net_inflow >= 3e8  # 3亿
                    and 50 <= sample.stock_info.float_cap <= 200
                    and price_data.close <= 100
                )
            
            # T08-T11: 换手率区间
            if rule_id == 'T08':  # 3%-7%
                return 3 <= price_data.turnover_rate < 7
            if rule_id == 'T09':  # 7%-10%
                return 7 <= price_data.turnover_rate < 10
            if rule_id == 'T10':  # 10%-15%
                return 10 <= price_data.turnover_rate < 15
            if rule_id == 'T11':  # >15%
                return price_data.turnover_rate > 15
            
            # L02: KDJ低位金叉
            if rule_id == 'L02':
                prev_kdj = None
                for p in sample.prices:
                    if p.date == sample.trigger_date:
                        break
                    prev_kdj = p
                return (
                    price_data.kdj_k < 30
                    and prev_kdj
                    and prev_kdj.kdj_k < prev_kdj.kdj_d
                    and price_data.kdj_k > price_data.kdj_d
                )
            
            # L05: RSI<30超卖
            if rule_id == 'L05':
                return price_data.rsi < 30
            
            # T01: 放量滞涨
            if rule_id == 'T01':
                prev_prices = [
                    p for p in sample.prices
                    if p.date < sample.trigger_date
                ]
                if len(prev_prices) >= 2:
                    prev_volume = prev_prices[-1].volume
                    change = (price_data.close - prev_prices[-1].close) / prev_prices[-1].close
                    return change < 0.03 and price_data.volume > prev_volume * 2
            
            # T02: 天量见天价
            if rule_id == 'T02':
                if len(sample.prices) >= 90:
                    recent_volumes = [p.volume for p in sample.prices[-90:]]
                    avg_volume = sum(recent_volumes) / len(recent_volumes)
                    return price_data.volume > avg_volume * 1.5
            
            return False
            
        except Exception as e:
            logger.warning(f"检查铁律 {rule.id} 时出错: {e}")
            return False
    
    def _validate_sample(self, sample: ValidationSample) -> Optional[SampleTestResult]:
        """验证单个样本"""
        try:
            # 计算收益率
            sample.calculate_returns()
            
            if sample.status == SampleStatus.EXCLUDED:
                return None
            
            # 判断是否成功（收益率>0）
            success = sample.return_rate > 0
            
            # 创建测试结果
            result = SampleTestResult(
                sample_id=sample.sample_id,
                stock_code=sample.stock_info.code,
                stock_name=sample.stock_info.name,
                trigger_date=str(sample.trigger_date),
                trigger_price=sample.trigger_price,
                exit_price=sample.exit_price,
                holding_days=sample.holding_days,
                triggered_rules=sample.triggered_rules,
                test_results=[],
                overall_success=success,
                return_rate=sample.return_rate,
                max_drawdown=sample.max_drawdown,
            )
            
            # 为每条触发的铁律创建测试结果
            for rule_id in sample.triggered_rules:
                rule = self.rule_parser.get_rule(rule_id)
                if rule:
                    test_result = RuleTestResult(
                        rule_id=rule_id,
                        rule_name=rule.name,
                        success=success,
                        prediction=self._get_prediction(rule),
                        actual_result='上涨' if success else '下跌',
                        return_rate=sample.return_rate,
                        holding_days=sample.holding_days,
                    )
                    result.test_results.append(test_result)
            
            return result
            
        except Exception as e:
            logger.error(f"验证样本失败: {e}")
            return None
    
    def _get_prediction(self, rule: Rule) -> str:
        """获取铁律预测"""
        if rule.rule_type == RuleType.FORBIDDEN:
            return '禁止买入'
        elif rule.rule_type == RuleType.BUY_SIGNAL:
            return '买入'
        elif rule.rule_type == RuleType.SELL_SIGNAL:
            return '卖出'
        else:
            return '观望'
    
    def _generate_report(
        self,
        term_type: TermType,
        start_date: date,
        end_date: date
    ) -> ValidationReport:
        """生成验证报告"""
        report = ValidationReport(
            report_id=generate_id("RPT"),
            version="v1.0",
            created_at=datetime.now(),
            batch_id=generate_id("BATCH"),
            sample_size=len(self.results),
            validation_period=f"{start_date}至{end_date}",
        )
        
        # 统计结果
        report.total_samples = len(self.results)
        report.success_samples = len([r for r in self.results if r.overall_success])
        report.failure_samples = report.total_samples - report.success_samples
        
        if self.results:
            report.overall_accuracy = report.success_samples / report.total_samples
            report.avg_return = sum(r.return_rate for r in self.results) / len(self.results)
            report.avg_drawdown = sum(r.max_drawdown for r in self.results) / len(self.results)
        
        # 计算盈亏比
        profits = [r.return_rate for r in self.results if r.return_rate > 0]
        losses = [abs(r.return_rate) for r in self.results if r.return_rate < 0]
        if losses and sum(losses) > 0:
            report.profit_ratio = (sum(profits) / len(profits)) / (sum(losses) / len(losses)) if profits else 0
        
        # 收集失败案例
        report.failure_cases = [r for r in self.results if not r.overall_success]
        
        # 生成铁律统计
        rule_stats = {}
        for result in self.results:
            for test_result in result.test_results:
                rule_id = test_result.rule_id
                if rule_id not in rule_stats:
                    rule = self.rule_parser.get_rule(rule_id)
                    rule_stats[rule_id] = RuleStatistics(
                        rule_id=rule_id,
                        rule_name=test_result.rule_name,
                        rule_type=rule.rule_type.value if rule else 'unknown',
                    )
                
                rule_stats[rule_id].total_tests += 1
                if test_result.success:
                    rule_stats[rule_id].success_count += 1
                else:
                    rule_stats[rule_id].failure_count += 1
        
        # 计算准确率和置信区间
        for stat in rule_stats.values():
            if stat.total_tests > 0:
                stat.accuracy = stat.success_count / stat.total_tests
                stat.avg_return = sum(
                    r.return_rate for r in self.results
                    if stat.rule_id in r.triggered_rules
                ) / stat.total_tests
                
                # 置信区间
                ci = calculate_confidence_interval(
                    stat.success_count, stat.total_tests
                )
                stat.confidence_interval = ci
                
                # 状态判断
                if stat.accuracy < 0.50:
                    stat.status = "eliminated"
                elif stat.accuracy < 0.60:
                    stat.status = "dormant"
                elif stat.accuracy < 0.70:
                    stat.status = "demoted"
                else:
                    stat.status = "active"
        
        report.rule_statistics = list(rule_stats.values())
        
        # 计算健康度评分
        report.health_score = self._calculate_health_score(report)
        
        # 存储样本结果
        report.sample_results = self.results
        
        return report
    
    def _calculate_health_score(self, report: ValidationReport) -> float:
        """计算健康度评分"""
        score = 50  # 基础分
        
        # 准确率贡献 (0-30分)
        score += report.overall_accuracy * 30
        
        # 盈亏比贡献 (0-10分)
        if report.profit_ratio > 1:
            score += min(report.profit_ratio, 3) * 3.33  # 最高10分
        
        # 回撤控制贡献 (0-10分)
        if report.avg_drawdown > 0:
            score += max(0, (0.2 - report.avg_drawdown) / 0.2 * 10)
        
        return min(100, max(0, score))
    
    def run_continuous_validation(
        self,
        rounds: int = 10,
        interval: int = 60
    ):
        """
        运行持续验证
        
        Args:
            rounds: 验证轮数
            interval: 每轮间隔（秒）
        """
        import time
        
        logger.info(f"开始持续验证: {rounds} 轮")
        
        reports = []
        for i in range(rounds):
            logger.info(f"=== 第 {i+1}/{rounds} 轮 ===")
            
            report = self.run_validation_round()
            if report:
                reports.append(report)
                
                # 保存报告
                self._save_report(report)
                
                # 打印简要结果
                logger.info(f"准确率: {report.overall_accuracy:.2%}, "
                           f"平均收益: {report.avg_return:.2%}, "
                           f"健康度: {report.health_score:.1f}")
            
            if i < rounds - 1:
                logger.info(f"等待 {interval} 秒...")
                time.sleep(interval)
        
        logger.info(f"持续验证完成: {len(reports)} 轮")
        return reports
    
    def _save_report(self, report: ValidationReport):
        """保存报告"""
        from config import PATH_CONFIG
        import os
        
        results_dir = PATH_CONFIG['results_dir']
        os.makedirs(results_dir, exist_ok=True)
        
        # 保存JSON
        json_path = os.path.join(results_dir, f"{report.report_id}.json")
        report.save(json_path, format='json')
        
        # 保存Markdown
        md_path = os.path.join(results_dir, f"{report.report_id}.md")
        report.save(md_path, format='markdown')
        
        logger.info(f"报告已保存: {json_path}")
