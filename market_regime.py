# -*- coding: utf-8 -*-
"""
市场状态识别模块
"""
from typing import Dict, Optional
from datetime import date, timedelta
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import MarketRegime
from config import MARKET_REGIME_CONFIG
from utils.logger import get_logger

logger = get_logger("MarketRegime")


class MarketRegimeIdentifier:
    """市场状态识别器"""
    
    def __init__(self, data_fetcher):
        self.data_fetcher = data_fetcher
        self.current_regime: Optional[MarketRegime] = None
        self.last_update: Optional[date] = None
    
    def identify_regime(self, current_date: date = None) -> MarketRegime:
        """
        识别市场状态
        
        Args:
            current_date: 当前日期
        
        Returns:
            市场状态
        """
        if current_date is None:
            from datetime import date as date_type
            current_date = date_type.today()
        
        # 检查缓存
        if (self.current_regime is not None 
            and self.last_update == current_date):
            return self.current_regime
        
        try:
            # 获取市场指数数据
            index_data = self.data_fetcher.get_market_index("000001", days=60)
            
            if not index_data:
                self.current_regime = MarketRegime.VOLATILE
                return self.current_regime
            
            # 计算各种指标
            current_close = index_data[-1]['close'] if index_data else 0
            
            # 计算均线
            ma20 = self._calculate_ma([d['close'] for d in index_data[-20:]], 20)
            ma60 = self._calculate_ma([d['close'] for d in index_data], 60)
            
            # 计算波动率
            volatility = self._calculate_volatility([d['close'] for d in index_data[-20:]])
            
            # 计算趋势
            trend = self._calculate_trend([d['close'] for d in index_data[-20:]])
            
            # 综合判断市场状态
            self.current_regime = self._determine_regime(
                current_close, ma20, ma60, volatility, trend
            )
            
            self.last_update = current_date
            
            logger.info(f"市场状态识别: {self.current_regime.value}, "
                       f"点位={current_close:.2f}, 趋势={trend:.2%}")
            
            return self.current_regime
            
        except Exception as e:
            logger.error(f"市场状态识别失败: {e}")
            self.current_regime = MarketRegime.VOLATILE
            return self.current_regime
    
    def _determine_regime(
        self,
        current_close: float,
        ma20: float,
        ma60: float,
        volatility: float,
        trend: float
    ) -> MarketRegime:
        """综合判断市场状态"""
        bull_threshold = MARKET_REGIME_CONFIG['bull_threshold']
        bear_threshold = MARKET_REGIME_CONFIG['bear_threshold']
        high_vol_threshold = MARKET_REGIME_CONFIG['high_volatility_threshold']
        
        # 危机状态判断（高波动+大幅下跌）
        if volatility > high_vol_threshold * 2 and trend < -0.1:
            return MarketRegime.CRISIS
        
        # 牛市判断
        if current_close > bull_threshold and ma20 > ma60 and trend > 0.05:
            return MarketRegime.BULL
        
        # 熊市判断
        if current_close < bear_threshold and ma20 < ma60 and trend < -0.05:
            return MarketRegime.BEAR
        
        # 复苏判断
        if trend > 0.02 and volatility > high_vol_threshold:
            return MarketRegime.RECOVERY
        
        # 默认震荡市
        return MarketRegime.VOLATILE
    
    def _calculate_ma(self, values: list, period: int) -> float:
        """计算移动平均"""
        if len(values) < period:
            return sum(values) / len(values) if values else 0
        return sum(values[-period:]) / period
    
    def _calculate_volatility(self, values: list) -> float:
        """计算波动率（日收益率标准差）"""
        if len(values) < 2:
            return 0
        
        returns = []
        for i in range(1, len(values)):
            ret = (values[i] - values[i-1]) / values[i-1]
            returns.append(ret)
        
        if not returns:
            return 0
        
        mean = sum(returns) / len(returns)
        variance = sum((r - mean) ** 2 for r in returns) / len(returns)
        return variance ** 0.5
    
    def _calculate_trend(self, values: list) -> float:
        """计算趋势（涨跌幅）"""
        if len(values) < 2:
            return 0
        return (values[-1] - values[0]) / values[0]
    
    def get_regime_description(self, regime: MarketRegime) -> str:
        """获取市场状态描述"""
        descriptions = {
            MarketRegime.BULL: "牛市 - 上涨趋势确立，适合积极做多",
            MarketRegime.BEAR: "熊市 - 下跌趋势确立，建议减仓或空仓",
            MarketRegime.VOLATILE: "震荡市 - 方向不明，建议高抛低吸",
            MarketRegime.CRISIS: "危机 - 市场剧烈波动，建议观望",
            MarketRegime.RECOVERY: "复苏 - 趋势好转，关注机会",
        }
        return descriptions.get(regime, "未知状态")
    
    def get_regime_adjustments(self, regime: MarketRegime) -> Dict:
        """获取市场状态调整参数"""
        adjustments = {
            MarketRegime.BULL: {
                'position_ratio': 0.8,        # 持仓比例
                'stop_loss': 0.05,            # 止损幅度
                'take_profit': 0.15,          # 止盈幅度
                'leverage': 1.0,              # 杠杆
            },
            MarketRegime.BEAR: {
                'position_ratio': 0.2,
                'stop_loss': 0.03,
                'take_profit': 0.05,
                'leverage': 0.5,
            },
            MarketRegime.VOLATILE: {
                'position_ratio': 0.5,
                'stop_loss': 0.04,
                'take_profit': 0.08,
                'leverage': 0.8,
            },
            MarketRegime.CRISIS: {
                'position_ratio': 0.1,
                'stop_loss': 0.02,
                'take_profit': 0.03,
                'leverage': 0.3,
            },
            MarketRegime.RECOVERY: {
                'position_ratio': 0.6,
                'stop_loss': 0.05,
                'take_profit': 0.10,
                'leverage': 0.8,
            },
        }
        return adjustments.get(regime, adjustments[MarketRegime.VOLATILE])
    
    def get_applicable_rules(self, regime: MarketRegime) -> Dict[str, float]:
        """
        获取适合当前市场状态的铁律权重调整
        
        Returns:
            铁律ID -> 权重调整因子
        """
        adjustments = {}
        
        # 牛市：强化趋势跟随类铁律
        if regime == MarketRegime.BULL:
            adjustments.update({
                'S06': 1.2,   # 综合评分买入
                'S11': 1.2,   # 主力大幅净流入+涨停
                'L01': 1.2,   # 均线多头排列
                'P01': 1.2,   # 政策催化板块启动
                'S04': 0.8,   # 追涨停板禁令（可适当放宽）
            })
        
        # 熊市：强化防御类铁律
        elif regime == MarketRegime.BEAR:
            adjustments.update({
                'S01': 1.3,   # MACD零轴下方禁止买入
                'S02': 1.3,   # 主力连续净流出禁止买入
                'S03': 1.3,   # 高价股禁买
                'N01': 1.3,   # 高位股禁买
                'S08': 1.3,   # 主力净流出>10亿
                'E01': 1.3,   # 大盘跌破20日线
                'S12': 1.3,   # 动态止损
            })
        
        # 震荡市：强化区间震荡类铁律
        elif regime == MarketRegime.VOLATILE:
            adjustments.update({
                'T05': 1.2,   # 地量见地价五条件
                'T04': 1.2,   # 缩量下跌抄底四条件
                'G05': 1.2,   # 缺口回补
                'E14': 1.2,   # 板块轮动加速
            })
        
        # 危机：极度保守
        elif regime == MarketRegime.CRISIS:
            adjustments.update({
                'S01': 1.5,
                'S02': 1.5,
                'S03': 1.5,
                'S04': 1.5,
                'S05': 1.5,
                'N01': 1.5,
                'N04': 1.5,
                'N05': 1.5,
                'O02': 1.5,   # 不抄底
                'O04': 1.5,   # 不满仓
            })
        
        # 复苏：适度积极
        elif regime == MarketRegime.RECOVERY:
            adjustments.update({
                'S06': 1.1,
                'S11': 1.1,
                'M01': 1.2,   # 业绩暴增
                'M02': 1.2,   # 业绩大幅增长
                'E02': 1.2,   # 大盘站稳20日线
            })
        
        return adjustments
