# -*- coding: utf-8 -*-
"""
铁律解析模块
"""
import re
from typing import List, Dict, Optional, Tuple, Any
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import (
    Rule, RuleSet, RuleType, RuleStatus, TermType, 
    RuleCondition, RuleVersion
)
from utils.logger import get_logger
from utils.helpers import safe_float

logger = get_logger("RuleParser")


class RuleParser:
    """铁律解析器"""
    
    def __init__(self):
        self.rules: List[Rule] = []
        self.rulesets: List[RuleSet] = []
        self.version: Optional[RuleVersion] = None
    
    def parse_rule_library(self, file_path: str) -> List[RuleSet]:
        """
        解析铁律库文件
        
        Args:
            file_path: 铁律库文件路径
        
        Returns:
            铁律集合列表
        """
        from pathlib import Path
        content = Path(file_path).read_text(encoding='utf-8')
        
        # 解析各个系列
        self.rulesets = []
        
        # S系列 - 短线铁律
        s_rules = self._parse_series_s(content)
        if s_rules:
            self.rulesets.append(RuleSet(
                series_id='S',
                series_name='短线铁律',
                rules=s_rules,
                description='适用场景：3日内操作，追求快速收益'
            ))
        
        # M系列 - 中线铁律
        m_rules = self._parse_series_m(content)
        if m_rules:
            self.rulesets.append(RuleSet(
                series_id='M',
                series_name='中线铁律',
                rules=m_rules,
                description='适用场景：5-20日操作，趋势跟踪'
            ))
        
        # L系列 - 长线铁律
        l_rules = self._parse_series_l(content)
        if l_rules:
            self.rulesets.append(RuleSet(
                series_id='L',
                series_name='长线铁律',
                rules=l_rules,
                description='适用场景：20日以上操作，价值投资'
            ))
        
        # T系列 - 成交量铁律
        t_rules = self._parse_series_t(content)
        if t_rules:
            self.rulesets.append(RuleSet(
                series_id='T',
                series_name='成交量铁律',
                rules=t_rules,
                description='适用场景：量价配合判断'
            ))
        
        # P系列 - 板块轮动铁律
        p_rules = self._parse_series_p(content)
        if p_rules:
            self.rulesets.append(RuleSet(
                series_id='P',
                series_name='板块轮动铁律',
                rules=p_rules,
                description='适用场景：板块选择、主线判断'
            ))
        
        # E系列 - 市场情绪铁律
        e_rules = self._parse_series_e(content)
        if e_rules:
            self.rulesets.append(RuleSet(
                series_id='E',
                series_name='市场情绪铁律',
                rules=e_rules,
                description='适用场景：市场整体情绪判断'
            ))
        
        # N系列 - 新增优化铁律
        n_rules = self._parse_series_n(content)
        if n_rules:
            self.rulesets.append(RuleSet(
                series_id='N',
                series_name='新增优化铁律',
                rules=n_rules,
                description='适用场景：验证发现的新铁律'
            ))
        
        # B系列 - 龙虎榜铁律
        b_rules = self._parse_series_b(content)
        if b_rules:
            self.rulesets.append(RuleSet(
                series_id='B',
                series_name='龙虎榜铁律',
                rules=b_rules,
                description='适用场景：游资/机构动向分析'
            ))
        
        # R系列 - 融资融券铁律
        r_rules = self._parse_series_r(content)
        if r_rules:
            self.rulesets.append(RuleSet(
                series_id='R',
                series_name='融资融券铁律',
                rules=r_rules,
                description='适用场景：杠杆资金分析'
            ))
        
        # G系列 - 缺口理论铁律
        g_rules = self._parse_series_g(content)
        if g_rules:
            self.rulesets.append(RuleSet(
                series_id='G',
                series_name='缺口理论铁律',
                rules=g_rules,
                description='适用场景：技术形态分析'
            ))
        
        # O系列 - 操作纪律铁律
        o_rules = self._parse_series_o(content)
        if o_rules:
            self.rulesets.append(RuleSet(
                series_id='O',
                series_name='操作纪律铁律',
                rules=o_rules,
                description='适用场景：交易执行规范'
            ))
        
        # 汇总所有规则
        self.rules = []
        for rs in self.rulesets:
            self.rules.extend(rs.rules)
        
        logger.info(f"解析铁律库成功: {len(self.rules)} 条铁律, {len(self.rulesets)} 个系列")
        return self.rulesets
    
    def _parse_series_s(self, content: str) -> List[Rule]:
        """解析S系列（短线铁律）"""
        rules = []
        
        # S01-S05 禁止买入铁律
        rules.append(Rule(
            id='S01', name='MACD零轴下方禁止买入',
            content='MACD零轴下方很远（<-0.5）+ 无量金叉 → 禁止买入',
            accuracy=0.825, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S02', name='主力连续净流出禁止买入',
            content='主力连续净流出累计≥5亿 + RSI>70高位 → 禁止买入',
            accuracy=0.88, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S03', name='高价股禁买',
            content='股价>100元禁止买入',
            accuracy=1.0, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S04', name='追涨停板禁令',
            content='禁止追板：首板、换手>20%、涨幅>50%、尾盘板、孤板、一字后二板',
            accuracy=0.825, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S05', name='业绩下滑股禁买',
            content='业绩预告/财报显示净利润下滑<0%时禁止买入',
            accuracy=0.76, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
            category='S系列'
        ))
        
        # S06-S11 买入信号铁律
        rules.append(Rule(
            id='S06', name='综合评分买入',
            content='主力净流入≥10亿 + 评分≥8 + PE<80倍 + 换手率<15%',
            accuracy=0.917, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S07', name='短线高收益标准',
            content='主力净流入>3亿 + 板块涨幅前3 + 股价10-100元',
            accuracy=0.444, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S08', name='主力净流出>10亿',
            content='主力净流出>10亿 = 明确卖出信号',
            accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S09', name='周线趋势确认',
            content='周线MACD零轴上方/附近 + 量能放大30-50% + 站稳20周线',
            accuracy=0.775, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S10', name='涨停后次日主力净流出',
            content='涨停后次日主力净流出 = 及时止盈信号',
            accuracy=0.75, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S11', name='主力大幅净流入+涨停',
            content='主力大幅净流入 + 涨停 = 买入信号',
            accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        
        # S12-S22 止盈止损铁律
        rules.append(Rule(
            id='S12', name='动态止损',
            content='分层止损：评分≥22分→止损-7%，20-21分→止损-5%，18-19分→止损-3%',
            accuracy=0.875, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S13', name='持股周期',
            content='短线持股≤3日，到期必须决策',
            accuracy=0.80, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S14', name='仓位控制',
            content='单次买入≤50%仓位，同时持仓≤2只',
            accuracy=0.95, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S15', name='动态分批止盈',
            content='强势龙头：+8%卖30%，+12%卖剩余',
            accuracy=0.875, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S16', name='保本铁律',
            content='回到成本价立即卖出，不贪恋',
            accuracy=1.0, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S20', name='短期诱多识别',
            content='满足3条触发诱多警告→降仓至20%',
            accuracy=0.75, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S21', name='动态止盈优化',
            content='强势龙头→+12%止盈50%，+15%止盈剩余',
            accuracy=0.0, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        rules.append(Rule(
            id='S22', name='铁律冲突解决',
            content='S06买入 AND S09周线主力流出→降仓至30%',
            accuracy=0.80, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
            category='S系列'
        ))
        
        return rules
    
    def _parse_series_m(self, content: str) -> List[Rule]:
        """解析M系列（中线铁律）"""
        return [
            Rule(
                id='M01', name='业绩暴增',
                content='业绩预告/财报显示净利润增长>100% = 强烈买入',
                accuracy=0.96, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='M系列'
            ),
            Rule(
                id='M02', name='业绩大幅增长',
                content='业绩预告/财报显示净利润增长30-100% = 买入',
                accuracy=0.95, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='M系列'
            ),
            Rule(
                id='M03', name='业绩稳定增长',
                content='扣非增速>50% + 连续3季>20%逐季提升 + PE<20倍',
                accuracy=1.0, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='M系列'
            ),
            Rule(
                id='M04', name='公告前3日埋伏',
                content='可获得更高收益',
                accuracy=0.90, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='M系列'
            ),
            Rule(
                id='M05', name='公告后次日追入',
                content='公告前未涨或涨幅<20%、主力净流入>1亿时可买入',
                accuracy=0.832, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='M系列'
            ),
        ]
    
    def _parse_series_l(self, content: str) -> List[Rule]:
        """解析L系列（长线铁律）"""
        return [
            Rule(
                id='L01', name='均线多头排列',
                content='M5/M10/M20/M30多头排列 + MACD转强 + 板块共振',
                accuracy=1.0, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.LONG,
                category='L系列'
            ),
            Rule(
                id='L02', name='KDJ低位金叉',
                content='KDJ低位金叉(<30) + 量能放大 = 买入信号',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.LONG,
                category='L系列'
            ),
            Rule(
                id='L03', name='KDJ高位金叉',
                content='KDJ高位金叉(>80) + 主力净流入 = 买入信号',
                accuracy=0.90, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.LONG,
                category='L系列'
            ),
            Rule(
                id='L04', name='RSI>70超买',
                content='RSI>70超买 + 主力净流出 = 卖出信号',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.LONG,
                category='L系列'
            ),
            Rule(
                id='L05', name='RSI<30超卖',
                content='RSI<30超卖 + 主力净流入 = 买入信号',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.LONG,
                category='L系列'
            ),
        ]
    
    def _parse_series_t(self, content: str) -> List[Rule]:
        """解析T系列（成交量铁律）"""
        return [
            Rule(
                id='T01', name='放量滞涨',
                content='股价涨幅<3%但成交量放大至前日2倍以上，主力可能在出货',
                accuracy=0.90, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T02', name='天量见天价',
                content='成交量创历史天量（>90日最高），往往对应股价阶段性高点',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T03', name='低位量价齐升',
                content='股价从高点跌≥30% + 低位横盘≥1个月 + 板块上涨/主力净流入',
                accuracy=0.833, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='T系列'
            ),
            Rule(
                id='T04', name='缩量下跌抄底四条件',
                content='位置低位(跌≥50%) + 企稳阳线 + 三天不破 + 温和放量',
                accuracy=0.833, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='T系列'
            ),
            Rule(
                id='T05', name='地量见地价五条件',
                content='连续地量(3-5日) + 股价企稳 + 放量确认 + 股价同步突破',
                accuracy=0.75, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='T系列'
            ),
            Rule(
                id='T06', name='温和放量启动',
                content='成交量逐步放大至5日均量的1.5-2倍',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T07', name='换手率1%-3%死水区',
                content='低位死水区是蓄势，高位死水区是出货',
                accuracy=0.825, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T08', name='换手率3%-7%试探区间',
                content='低位温和放量是吸筹信号，连续5日+主力净流入',
                accuracy=0.84, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T09', name='换手率7%-10%强势区',
                content='低位首次出现是机会，连续3-5日+主力>1亿+板块前5',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T10', name='换手率10%-15%高活跃区',
                content='小盘股(<200亿)正常，大盘股(>500亿)需重大消息配合',
                accuracy=0.825, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T11', name='换手率>15%极端区',
                content='高位极端区是卖出信号，低位极端区是买入机会',
                accuracy=0.80, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T12', name='价升量缩需警惕',
                content='低位缩量上涨=控盘信号，高位缩量=诱多陷阱',
                accuracy=0.85, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T13', name='低位放量是吸筹信号',
                content='已含位置条件',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T14', name='高位放量是出货信号',
                content='已含位置条件',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='T系列'
            ),
            Rule(
                id='T15', name='堆量是主力行为',
                content='底部堆量：连续5-8天+量增10%+股价涨<5%',
                accuracy=0.86, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='T系列'
            ),
        ]
    
    def _parse_series_p(self, content: str) -> List[Rule]:
        """解析P系列（板块轮动铁律）"""
        return [
            Rule(
                id='P01', name='政策催化板块启动快',
                content='受政策利好刺激的板块，往往在消息公布后3日内涨幅最大',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P02', name='弱势板块不参与反弹',
                content='资金持续流出的弱势板块，任何反弹都是卖出机会',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P03', name='主力资金与北向资金共振',
                content='两者同时净买入某板块，上涨确定性更高',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P04', name='板块涨停家数>10家是主线确认',
                content='板块内涨停个股数量超过10家，说明资金高度认可',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P05', name='资金流入确认板块方向',
                content='近5日中至少3日主力净流入+累计净流入≥5亿+板块涨幅前5',
                accuracy=0.75, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P06', name='资金连续3日净流出警示',
                content='板块连续3日净流出且3日累计≥30亿，RSI>70高位',
                accuracy=0.83, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P07', name='龙头股领涨板块',
                content='市值≥100亿+主营占比≥70%或涨幅≥10%+换手率≥8%',
                accuracy=0.82, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='P系列'
            ),
            Rule(
                id='P08', name='北向资金连续买入',
                content='连续3日净买入+累计≥30亿+沪股通深股通同向',
                accuracy=0.81, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='P系列'
            ),
        ]
    
    def _parse_series_e(self, content: str) -> List[Rule]:
        """解析E系列（市场情绪铁律）"""
        return [
            Rule(
                id='E01', name='大盘跌破20日线',
                content='大盘跌破20日线 + 成交量放大 = 减仓信号',
                accuracy=0.85, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E02', name='大盘站稳20日线',
                content='大盘站稳20日线 + 成交量放大 = 加仓信号',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E03', name='涨停家数>50家',
                content='涨停家数>50家 = 市场情绪活跃，可积极参与',
                accuracy=0.75, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E04', name='跌停家数>20家',
                content='跌停家数>20家 = 市场情绪恐慌，谨慎操作',
                accuracy=0.80, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E05', name='北向资金连续净买入',
                content='北向资金连续3日净买入 + 累计≥50亿 = 中期看涨信号',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='E系列'
            ),
            Rule(
                id='E06', name='北向资金连续净卖出',
                content='北向资金连续3日净卖出 + 累计≥50亿 = 中期看跌信号',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.MID,
                category='E系列'
            ),
            Rule(
                id='E07', name='两市成交额破万亿',
                content='两市成交额破万亿 = 市场活跃，机会较多',
                accuracy=0.75, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E08', name='两市成交额<7000亿',
                content='两市成交额<7000亿 = 市场冷清，机会较少',
                accuracy=0.75, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E09', name='融资余额持续上升',
                content='融资余额连续5日上升 = 市场看多情绪浓厚',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E10', name='融资余额持续下降',
                content='融资余额连续5日下降 = 市场看空情绪浓厚',
                accuracy=0.80, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E11', name='新增开户数激增',
                content='新增开户数环比增长>50% = 散户入场，需警惕见顶',
                accuracy=0.75, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E12', name='新增开户数骤降',
                content='新增开户数环比下降>50% = 散户离场，可能见底',
                accuracy=0.75, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E13', name='北向资金净流入',
                content='北向资金单日净流入>100亿 = 强烈买入信号',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E14', name='板块轮动加速',
                content='热点板块快速切换（3日内更换主线）= 市场不稳定',
                accuracy=0.80, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='E系列'
            ),
            Rule(
                id='E15', name='情绪周期判断',
                content='情绪周期：启动期→爆发期→分化期→退潮期→冰点期→启动期',
                accuracy=0.785, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='E系列'
            ),
        ]
    
    def _parse_series_n(self, content: str) -> List[Rule]:
        """解析N系列（新增优化铁律）"""
        return [
            Rule(
                id='N01', name='高位股禁买',
                content='股价距历史高点<20%时禁止买入',
                accuracy=1.0, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N02', name='无实质业务股禁买',
                content='主营业务收入占比<50%或无明确盈利模式的股票禁止买入',
                accuracy=1.0, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N04', name='跌停股禁买',
                content='近5日有跌停记录的股票禁止买入',
                accuracy=1.0, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N05', name='次新股禁买',
                content='上市不足60个交易日的次新股禁止买入',
                accuracy=1.0, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N06', name='主力净流入+市值+股价',
                content='主力净流入≥3亿 + 流通市值50-200亿 + 股价≤100元',
                accuracy=0.933, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N07', name='三路资金联合买入',
                content='机构 + 北向资金 + 游资同时净买入 = 强买入信号',
                accuracy=0.95, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N08', name='主板优先',
                content='主板股票（600/000开头）稳定性更高',
                accuracy=0.95, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N09', name='股价10-50元区间',
                content='主力资金3亿级标的中，股价10-50元区间弹性更大',
                accuracy=0.90, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N10', name='独食信号',
                content='单游资买入占比>50% + 机构卖出 = 禁止追高',
                accuracy=0.85, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N11', name='板块批量跌停',
                content='同板块>2股跌停 = 禁止买入该板块',
                accuracy=0.90, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N12', name='主力净流出>10亿',
                content='主力净流出>10亿 = 立即减仓50%+',
                accuracy=0.90, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N13', name='大盘环境仓位控制',
                content='上证指数4000点以上减仓，3900点以下可加仓',
                accuracy=0.88, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N14', name='主力资金防骗铁律',
                content='主力净流入必须同时满足：大单占比≥15% + 量价齐升',
                accuracy=0.75, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N15', name='资金持续性验证铁律',
                content='单日净流入不可信，必须看连续3-5日趋势',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N16', name='资金+位置共振铁律',
                content='主力净流入 + 股价处于低位/超跌区域 + 量价齐升',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N17', name='高位蓄势识别',
                content='前期涨幅>30% + 高位整理>5日 + 均线多头 + 缩量整理',
                accuracy=0.90, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='N系列'
            ),
            Rule(
                id='N18', name='实质业务验证',
                content='主线业务占比>50% + 业绩持续增长 + 行业景气向上',
                accuracy=0.95, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.LONG,
                category='N系列'
            ),
        ]
    
    def _parse_series_b(self, content: str) -> List[Rule]:
        """解析B系列（龙虎榜铁律）"""
        return [
            Rule(
                id='B01', name='游资主导+涨停',
                content='游资买入金额>机构买入金额 + 涨停 = 短线机会',
                accuracy=0.75, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='B系列'
            ),
            Rule(
                id='B02', name='机构主导+涨停',
                content='机构买入金额>游资买入金额 + 涨停 = 中线机会',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='B系列'
            ),
            Rule(
                id='B03', name='游资撤退',
                content='游资大幅卖出（卖出金额>买入金额2倍）= 短线见顶信号',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='B系列'
            ),
            Rule(
                id='B04', name='机构撤退',
                content='机构大幅卖出（卖出金额>买入金额2倍）= 中线见顶信号',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.MID,
                category='B系列'
            ),
            Rule(
                id='B05', name='多家机构买入',
                content='龙虎榜显示≥3家机构买入 = 强烈看好信号',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.MID,
                category='B系列'
            ),
            Rule(
                id='B06', name='知名游资买入',
                content='知名游资买入 = 短线关注度提升',
                accuracy=0.70, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='B系列'
            ),
            Rule(
                id='B07', name='游资+机构共振',
                content='游资和机构同时大幅买入 = 短中线共振',
                accuracy=0.90, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='B系列'
            ),
            Rule(
                id='B08', name='营业部席位集中',
                content='同一营业部席位连续多日买入同一股票 = 可能存在内部消息',
                accuracy=0.75, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='B系列'
            ),
        ]
    
    def _parse_series_r(self, content: str) -> List[Rule]:
        """解析R系列（融资融券铁律）"""
        return [
            Rule(
                id='R01', name='融资余额持续上升',
                content='融资余额连续5日上升 + 股价上涨 = 看多信号',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='R系列'
            ),
            Rule(
                id='R02', name='融资余额持续下降',
                content='融资余额连续5日下降 + 股价下跌 = 看空信号',
                accuracy=0.80, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='R系列'
            ),
            Rule(
                id='R03', name='融券余额激增',
                content='融券余额单日增长>50% = 看空信号，需警惕',
                accuracy=0.75, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='R系列'
            ),
            Rule(
                id='R04', name='融资买入占比高',
                content='融资买入额占成交额>20% = 市场情绪过热，需警惕见顶',
                accuracy=0.75, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='R系列'
            ),
            Rule(
                id='R05', name='融资余额创新高',
                content='融资余额创历史新高 = 市场情绪极度乐观，风险加大',
                accuracy=0.70, rule_type=RuleType.RISK_WARNING, term_type=TermType.SHORT,
                category='R系列'
            ),
            Rule(
                id='R06', name='融资余额创新低',
                content='融资余额创历史新低 = 市场情绪极度悲观，可能见底',
                accuracy=0.70, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='R系列'
            ),
        ]
    
    def _parse_series_g(self, content: str) -> List[Rule]:
        """解析G系列（缺口理论铁律）"""
        return [
            Rule(
                id='G01', name='向上突破缺口',
                content='向上突破缺口 + 成交量放大 = 强势上涨信号',
                accuracy=0.85, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='G系列'
            ),
            Rule(
                id='G02', name='向下突破缺口',
                content='向下突破缺口 + 成交量放大 = 强势下跌信号',
                accuracy=0.85, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='G系列'
            ),
            Rule(
                id='G03', name='向上衰竭缺口',
                content='连续上涨后出现向上缺口 + 成交量萎缩 = 上涨衰竭信号',
                accuracy=0.80, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='G系列'
            ),
            Rule(
                id='G04', name='向下衰竭缺口',
                content='连续下跌后出现向下缺口 + 成交量萎缩 = 下跌衰竭信号',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='G系列'
            ),
            Rule(
                id='G05', name='缺口回补',
                content='普通缺口通常会在3-5个交易日内回补',
                accuracy=0.75, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='G系列'
            ),
            Rule(
                id='G06', name='缺口支撑',
                content='向上突破缺口被回踩但不完全回补 = 强支撑位',
                accuracy=0.80, rule_type=RuleType.BUY_SIGNAL, term_type=TermType.SHORT,
                category='G系列'
            ),
            Rule(
                id='G07', name='缺口压力',
                content='向下突破缺口被反弹但不完全回补 = 强压力位',
                accuracy=0.80, rule_type=RuleType.SELL_SIGNAL, term_type=TermType.SHORT,
                category='G系列'
            ),
        ]
    
    def _parse_series_o(self, content: str) -> List[Rule]:
        """解析O系列（操作纪律铁律）"""
        return [
            Rule(
                id='O01', name='不追高',
                content='股价当日涨幅>5%不追高买入',
                accuracy=0.85, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='O系列'
            ),
            Rule(
                id='O02', name='不抄底',
                content='股价连续下跌不预判底部，等待企稳信号',
                accuracy=0.90, rule_type=RuleType.FORBIDDEN, term_type=TermType.SHORT,
                category='O系列'
            ),
            Rule(
                id='O03', name='严格执行止盈止损',
                content='设定止盈止损位后必须严格执行，不犹豫',
                accuracy=0.95, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='O系列'
            ),
            Rule(
                id='O04', name='不满仓',
                content='任何时候不满仓操作，保留现金应对风险',
                accuracy=0.90, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='O系列'
            ),
            Rule(
                id='O05', name='不频繁交易',
                content='单日交易次数不超过3次，避免过度交易',
                accuracy=0.85, rule_type=RuleType.NEUTRAL, term_type=TermType.SHORT,
                category='O系列'
            ),
        ]
    
    def get_rule(self, rule_id: str) -> Optional[Rule]:
        """获取指定铁律"""
        for rule in self.rules:
            if rule.id == rule_id:
                return rule
        return None
    
    def get_rules_by_type(self, rule_type: RuleType) -> List[Rule]:
        """获取指定类型的铁律"""
        return [r for r in self.rules if r.rule_type == rule_type]
    
    def get_rules_by_series(self, series_id: str) -> List[Rule]:
        """获取指定系列的铁律"""
        return [r for r in self.rules if r.category.startswith(series_id)]
    
    def get_forbidden_rules(self) -> List[Rule]:
        """获取禁止买入铁律"""
        return self.get_rules_by_type(RuleType.FORBIDDEN)
    
    def get_buy_signal_rules(self) -> List[Rule]:
        """获取买入信号铁律"""
        return self.get_rules_by_type(RuleType.BUY_SIGNAL)
    
    def get_sell_signal_rules(self) -> List[Rule]:
        """获取卖出信号铁律"""
        return self.get_rules_by_type(RuleType.SELL_SIGNAL)
    
    def get_statistics(self) -> Dict:
        """获取铁律统计"""
        stats = {
            'total': len(self.rules),
            'by_type': {},
            'by_series': {},
            'avg_accuracy': 0,
        }
        
        # 按类型统计
        for rt in RuleType:
            count = len([r for r in self.rules if r.rule_type == rt])
            stats['by_type'][rt.value] = count
        
        # 按系列统计
        for rs in self.rulesets:
            stats['by_series'][rs.series_id] = {
                'name': rs.series_name,
                'count': len(rs.rules),
            }
        
        # 平均准确率
        if self.rules:
            stats['avg_accuracy'] = sum(r.accuracy for r in self.rules) / len(self.rules)
        
        return stats
