# -*- coding: utf-8 -*-
"""
铁律外部验证机制
包括纸上交易模拟器、小仓位实盘验证接口、模拟账户管理
"""
from pathlib import Path
import json
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from models.rule import Rule, RuleSet, RuleType
from utils.logger import get_logger

logger = get_logger("ExternalValidation")


class OrderType(Enum):
    """订单类型"""
    BUY = "buy"
    SELL = "sell"


class OrderStatus(Enum):
    """订单状态"""
    PENDING = "pending"           # 待成交
    FILLED = "filled"            # 已成交
    CANCELLED = "cancelled"      # 已取消
    REJECTED = "rejected"        # 已拒绝


class PositionStatus(Enum):
    """持仓状态"""
    OPEN = "open"                 # 持仓中
    CLOSED = "closed"            # 已平仓
    STOPPED = "stopped"          # 止损出局
    PROFITED = "profited"        # 止盈出局


@dataclass
class SimulatedOrder:
    """模拟订单"""
    order_id: str
    stock_code: str
    stock_name: str
    order_type: OrderType
    price: float
    quantity: int
    timestamp: str
    status: OrderStatus = OrderStatus.PENDING
    filled_price: float = 0
    filled_time: str = ""
    commission: float = 0        # 手续费
    slippage: float = 0          # 滑点
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'order_id': self.order_id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'order_type': self.order_type.value,
            'price': self.price,
            'quantity': self.quantity,
            'timestamp': self.timestamp,
            'status': self.status.value,
            'filled_price': self.filled_price,
            'filled_time': self.filled_time,
            'commission': self.commission,
            'slippage': self.slippage,
        }


@dataclass
class Position:
    """持仓"""
    position_id: str
    stock_code: str
    stock_name: str
    entry_price: float
    quantity: int
    entry_date: str
    status: PositionStatus = PositionStatus.OPEN
    
    # 盈亏计算
    current_price: float = 0
    unrealized_pnl: float = 0
    unrealized_pnl_pct: float = 0
    
    # 止损止盈
    stop_loss_price: float = 0
    take_profit_price: float = 0
    
    def update_pnl(self, current_price: float):
        """更新盈亏"""
        self.current_price = current_price
        cost = self.entry_price * self.quantity
        current_value = current_price * self.quantity
        self.unrealized_pnl = current_value - cost
        self.unrealized_pnl_pct = (current_price - self.entry_price) / self.entry_price
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'position_id': self.position_id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'entry_price': self.entry_price,
            'quantity': self.quantity,
            'entry_date': self.entry_date,
            'status': self.status.value,
            'current_price': self.current_price,
            'unrealized_pnl': f"{self.unrealized_pnl:.2f}",
            'unrealized_pnl_pct': f"{self.unrealized_pnl_pct:.2%}",
            'stop_loss_price': self.stop_loss_price,
            'take_profit_price': self.take_profit_price,
        }


@dataclass
class TradeRecord:
    """交易记录"""
    trade_id: str
    position_id: str
    stock_code: str
    stock_name: str
    entry_time: str
    exit_time: str
    entry_price: float
    exit_price: float
    quantity: int
    pnl: float
    pnl_pct: float
    holding_days: int
    exit_reason: str              # stop_loss/take_profit/manual/time_limit
    rules_triggered: List[str]    # 触发的铁律
    
    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'trade_id': self.trade_id,
            'position_id': self.position_id,
            'stock_code': self.stock_code,
            'stock_name': self.stock_name,
            'entry_time': self.entry_time,
            'exit_time': self.exit_time,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'quantity': self.quantity,
            'pnl': f"{self.pnl:.2f}",
            'pnl_pct': f"{self.pnl_pct:.2%}",
            'holding_days': self.holding_days,
            'exit_reason': self.exit_reason,
            'rules_triggered': self.rules_triggered,
        }


@dataclass
class SimulatedAccount:
    """模拟账户"""
    account_id: str
    name: str
    initial_capital: float
    current_capital: float
    created_at: str
    
    # 统计
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    
    # 持仓
    positions: List[Position] = field(default_factory=list)
    
    # 历史交易
    trade_history: List[TradeRecord] = field(default_factory=list)
    
    # 每日净值
    daily_nav: List[Dict] = field(default_factory=list)
    
    def get_win_rate(self) -> float:
        """获取胜率"""
        if self.total_trades == 0:
            return 0
        return self.winning_trades / self.total_trades
    
    def get_total_pnl(self) -> float:
        """获取总盈亏"""
        return sum(t.pnl for t in self.trade_history)
    
    def get_avg_profit(self) -> float:
        """获取平均盈利"""
        winning = [t.pnl for t in self.trade_history if t.pnl > 0]
        return sum(winning) / len(winning) if winning else 0
    
    def get_avg_loss(self) -> float:
        """获取平均亏损"""
        losing = [t.pnl for t in self.trade_history if t.pnl < 0]
        return sum(losing) / len(losing) if losing else 0
    
    def get_profit_factor(self) -> float:
        """获取盈利因子"""
        total_profit = sum(t.pnl for t in self.trade_history if t.pnl > 0)
        total_loss = abs(sum(t.pnl for t in self.trade_history if t.pnl < 0))
        return total_profit / total_loss if total_loss > 0 else 0
    
    def get_max_drawdown(self) -> float:
        """获取最大回撤"""
        if not self.daily_nav:
            return 0
        
        nav_values = [d['nav'] for d in self.daily_nav]
        peak = nav_values[0]
        max_dd = 0
        
        for nav in nav_values:
            if nav > peak:
                peak = nav
            dd = (peak - nav) / peak
            if dd > max_dd:
                max_dd = dd
        
        return max_dd
    
    def to_summary(self) -> Dict:
        """获取账户摘要"""
        return {
            'account_id': self.account_id,
            'name': self.name,
            'initial_capital': f"{self.initial_capital:.2f}",
            'current_capital': f"{self.current_capital:.2f}",
            'total_pnl': f"{self.get_total_pnl():.2f}",
            'total_pnl_pct': f"{self.get_total_pnl() / self.initial_capital:.2%}",
            'total_trades': self.total_trades,
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': f"{self.get_win_rate():.1%}",
            'profit_factor': f"{self.get_profit_factor():.2f}",
            'max_drawdown': f"{self.get_max_drawdown():.2%}",
            'open_positions': len(self.positions),
        }


class PaperTradingSimulator:
    """纸上交易模拟器"""
    
    def __init__(self, config: Dict = None):
        """
        初始化纸上交易模拟器
        
        Args:
            config: 配置参数
        """
        self.config = config or {}
        
        # 手续费配置
        self.commission_rate = self.config.get('commission_rate', 0.0003)     # 万三
        self.min_commission = self.config.get('min_commission', 5)             # 最低5元
        self.stamp_tax = self.config.get('stamp_tax', 0.001)                  # 印花税千分之一
        
        # 滑点配置
        self.slippage_rate = self.config.get('slippage_rate', 0.001)          # 千分之一
        
        # 止损止盈配置
        self.default_stop_loss = self.config.get('default_stop_loss', -0.05)  # 默认止损5%
        self.default_take_profit = self.config.get('default_take_profit', 0.10)  # 默认止盈10%
        
        # 账户
        self.account: SimulatedAccount = None
        
        # 存储目录
        from config import PATH_CONFIG
        self.storage_dir = Path(PATH_CONFIG['project_root']) / 'data' / 'paper_trading'
        self.storage_dir.mkdir(parents=True, exist_ok=True)
    
    def create_account(
        self,
        name: str,
        initial_capital: float = 100000,
    ) -> SimulatedAccount:
        """
        创建模拟账户
        
        Args:
            name: 账户名称
            initial_capital: 初始资金
        
        Returns:
            模拟账户
        """
        self.account = SimulatedAccount(
            account_id=f"paper_{datetime.now().strftime('%Y%m%d%H%M%S')}",
            name=name,
            initial_capital=initial_capital,
            current_capital=initial_capital,
            created_at=datetime.now().isoformat(),
        )
        
        self._save_account()
        logger.info(f"创建模拟账户: {name}, 初始资金: {initial_capital}")
        return self.account
    
    def load_account(self, account_id: str) -> Optional[SimulatedAccount]:
        """加载账户"""
        account_file = self.storage_dir / f"{account_id}.json"
        
        if not account_file.exists():
            logger.error(f"账户不存在: {account_id}")
            return None
        
        try:
            with open(account_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            self.account = SimulatedAccount(
                account_id=data['account_id'],
                name=data['name'],
                initial_capital=data['initial_capital'],
                current_capital=data['current_capital'],
                created_at=data['created_at'],
                total_trades=data.get('total_trades', 0),
                winning_trades=data.get('winning_trades', 0),
                losing_trades=data.get('losing_trades', 0),
                trade_history=[TradeRecord(**t) for t in data.get('trade_history', [])],
                daily_nav=data.get('daily_nav', []),
            )
            
            # 重建持仓
            self.account.positions = [Position(**p) for p in data.get('positions', [])]
            
            return self.account
        except Exception as e:
            logger.error(f"加载账户失败: {e}")
            return None
    
    def _save_account(self):
        """保存账户"""
        if not self.account:
            return
        
        account_file = self.storage_dir / f"{self.account.account_id}.json"
        
        data = {
            'account_id': self.account.account_id,
            'name': self.account.name,
            'initial_capital': self.account.initial_capital,
            'current_capital': self.account.current_capital,
            'created_at': self.account.created_at,
            'total_trades': self.account.total_trades,
            'winning_trades': self.account.winning_trades,
            'losing_trades': self.account.losing_trades,
            'positions': [p.to_dict() for p in self.account.positions],
            'trade_history': [t.to_dict() for t in self.account.trade_history],
            'daily_nav': self.account.daily_nav,
        }
        
        with open(account_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    
    def _generate_order_id(self) -> str:
        """生成订单ID"""
        return f"order_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    
    def _generate_position_id(self) -> str:
        """生成持仓ID"""
        return f"pos_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
    
    def _calculate_commission(self, amount: float, is_sell: bool = False) -> float:
        """计算手续费"""
        commission = amount * self.commission_rate
        commission = max(commission, self.min_commission)
        
        # 卖出时收取印花税
        if is_sell:
            commission += amount * self.stamp_tax
        
        return commission
    
    def _calculate_slippage(self, price: float) -> float:
        """计算滑点"""
        return price * self.slippage_rate
    
    def place_order(
        self,
        stock_code: str,
        stock_name: str,
        order_type: OrderType,
        price: float,
        quantity: int,
    ) -> SimulatedOrder:
        """
        下单
        
        Args:
            stock_code: 股票代码
            stock_name: 股票名称
            order_type: 订单类型
            price: 价格
            quantity: 数量
        
        Returns:
            订单
        """
        if not self.account:
            raise ValueError("账户未初始化")
        
        order = SimulatedOrder(
            order_id=self._generate_order_id(),
            stock_code=stock_code,
            stock_name=stock_name,
            order_type=order_type,
            price=price,
            quantity=quantity,
            timestamp=datetime.now().isoformat(),
        )
        
        # 计算滑点
        order.slippage = self._calculate_slippage(price)
        
        if order_type == OrderType.BUY:
            # 买入成本
            cost = price * quantity
            commission = self._calculate_commission(cost, is_sell=False)
            total_cost = cost + commission
            
            # 检查资金
            if total_cost > self.account.current_capital:
                order.status = OrderStatus.REJECTED
                logger.warning(f"资金不足: 需要{total_cost:.2f}, 账户余额{self.account.current_capital:.2f}")
                return order
            
            # 成交
            order.status = OrderStatus.FILLED
            order.filled_price = price + order.slippage  # 滑点
            order.filled_time = datetime.now().isoformat()
            order.commission = commission
            
            # 扣除资金
            self.account.current_capital -= (order.filled_price * quantity + commission)
            
            # 创建持仓
            position = Position(
                position_id=self._generate_position_id(),
                stock_code=stock_code,
                stock_name=stock_name,
                entry_price=order.filled_price,
                quantity=quantity,
                entry_date=date.today().isoformat(),
                stop_loss_price=order.filled_price * (1 + self.default_stop_loss),
                take_profit_price=order.filled_price * (1 + self.default_take_profit),
            )
            self.account.positions.append(position)
            
        else:  # SELL
            # 查找持仓
            position = None
            for pos in self.account.positions:
                if pos.stock_code == stock_code and pos.status == PositionStatus.OPEN:
                    position = pos
                    break
            
            if not position:
                order.status = OrderStatus.REJECTED
                logger.warning(f"没有可卖出的持仓: {stock_code}")
                return order
            
            # 成交
            order.status = OrderStatus.FILLED
            order.filled_price = price - order.slippage  # 滑点
            order.filled_time = datetime.now().isoformat()
            commission = self._calculate_commission(order.filled_price * quantity, is_sell=True)
            order.commission = commission
            
            # 收入
            revenue = order.filled_price * quantity - commission
            self.account.current_capital += revenue
            
            # 平仓
            position.status = PositionStatus.CLOSED
            
            # 记录交易
            holding_days = (date.today() - date.fromisoformat(position.entry_date)).days
            pnl = (order.filled_price - position.entry_price) * quantity - commission
            pnl_pct = (order.filled_price - position.entry_price) / position.entry_price
            
            trade = TradeRecord(
                trade_id=f"trade_{datetime.now().strftime('%Y%m%d%H%M%S')}",
                position_id=position.position_id,
                stock_code=stock_code,
                stock_name=stock_name,
                entry_time=position.entry_date,
                exit_time=date.today().isoformat(),
                entry_price=position.entry_price,
                exit_price=order.filled_price,
                quantity=quantity,
                pnl=pnl,
                pnl_pct=pnl_pct,
                holding_days=holding_days,
                exit_reason="manual",
                rules_triggered=[],
            )
            
            self.account.trade_history.append(trade)
            self.account.total_trades += 1
            
            if pnl > 0:
                self.account.winning_trades += 1
            else:
                self.account.losing_trades += 1
            
            # 移除持仓
            self.account.positions.remove(position)
        
        self._save_account()
        logger.info(f"订单成交: {order.order_type.value} {stock_code} {quantity}股 @{order.filled_price:.2f}")
        return order
    
    def check_and_close_positions(
        self,
        current_prices: Dict[str, float],
    ) -> List[str]:
        """
        检查并自动平仓
        
        Args:
            current_prices: 当前价格字典 {stock_code: price}
        
        Returns:
            平仓原因列表
        """
        if not self.account:
            return []
        
        closed_reasons = []
        
        for position in self.account.positions[:]:  # 复制列表避免迭代问题
            if position.stock_code not in current_prices:
                continue
            
            current_price = current_prices[position.stock_code]
            position.update_pnl(current_price)
            
            # 检查止损
            if current_price <= position.stop_loss_price:
                self.place_order(
                    stock_code=position.stock_code,
                    stock_name=position.stock_name,
                    order_type=OrderType.SELL,
                    price=current_price,
                    quantity=position.quantity,
                )
                position.status = PositionStatus.STOPPED
                closed_reasons.append(f"{position.stock_code}: 止损出局")
            
            # 检查止盈
            elif current_price >= position.take_profit_price:
                self.place_order(
                    stock_code=position.stock_code,
                    stock_name=position.stock_name,
                    order_type=OrderType.SELL,
                    price=current_price,
                    quantity=position.quantity,
                )
                position.status = PositionStatus.PROFITED
                closed_reasons.append(f"{position.stock_code}: 止盈出局")
        
        return closed_reasons
    
    def update_daily_nav(self, index_value: float = None):
        """更新每日净值"""
        if not self.account:
            return
        
        total_assets = self.account.current_capital
        for position in self.account.positions:
            total_assets += position.current_price * position.quantity if position.current_price > 0 else 0
        
        nav = total_assets / self.account.initial_capital
        
        self.account.daily_nav.append({
            'date': date.today().isoformat(),
            'nav': nav,
            'capital': self.account.current_capital,
            'index': index_value,
        })
        
        self._save_account()
    
    def get_account_summary(self) -> Dict:
        """获取账户摘要"""
        if not self.account:
            return {}
        return self.account.to_summary()
    
    def get_trade_history(
        self,
        limit: int = 50,
        stock_code: str = None,
    ) -> List[Dict]:
        """获取交易历史"""
        if not self.account:
            return []
        
        history = self.account.trade_history
        
        if stock_code:
            history = [t for t in history if t.stock_code == stock_code]
        
        return [t.to_dict() for t in history[-limit:]]
    
    def get_open_positions(self) -> List[Dict]:
        """获取持仓"""
        if not self.account:
            return []
        return [p.to_dict() for p in self.account.positions]
    
    def generate_performance_report(self) -> str:
        """生成绩效报告"""
        if not self.account:
            return "账户未初始化"
        
        summary = self.account.to_summary()
        nav = self.account.daily_nav
        
        lines = [
            "# 纸上交易绩效报告",
            f"\n**账户**: {self.account.name}",
            f"**时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "\n---\n",
            "\n## 账户概览\n",
            f"- 初始资金: {summary['initial_capital']}",
            f"- 当前资金: {summary['current_capital']}",
            f"- 总盈亏: {summary['total_pnl']} ({summary['total_pnl_pct']})",
            f"- 最大回撤: {summary['max_drawdown']}",
            "\n---\n",
            "\n## 交易统计\n",
            f"- 总交易次数: {summary['total_trades']}",
            f"- 盈利次数: {summary['winning_trades']}",
            f"- 亏损次数: {summary['losing_trades']}",
            f"- 胜率: {summary['win_rate']}",
            f"- 盈利因子: {summary['profit_factor']}",
            "\n---\n",
        ]
        
        # 最近交易
        recent = self.account.trade_history[-10:]
        if recent:
            lines.append("\n## 最近交易\n")
            lines.append("\n| 股票 | 买入 | 卖出 | 盈亏 | 盈亏% | 持仓天数 | 出局原因 |")
            lines.append("|------|------|------|------|-------|----------|----------|")
            
            for t in reversed(recent):
                pnl_color = "🟢" if t.pnl > 0 else "🔴"
                lines.append(
                    f"| {t.stock_name}({t.stock_code}) | {t.entry_price:.2f} | {t.exit_price:.2f} | "
                    f"{pnl_color}{t.pnl:.2f} | {t.pnl_pct:.2%} | {t.holding_days} | {t.exit_reason} |"
                )
        
        # 持仓
        positions = self.account.positions
        if positions:
            lines.append("\n## 当前持仓\n")
            lines.append("\n| 股票 | 买入价 | 当前价 | 数量 | 盈亏 | 盈亏% | 止损价 | 止盈价 |")
            lines.append("|------|--------|--------|------|------|-------|--------|--------|")
            
            for p in positions:
                pnl_color = "🟢" if p.unrealized_pnl > 0 else "🔴"
                lines.append(
                    f"| {p.stock_name}({p.stock_code}) | {p.entry_price:.2f} | {p.current_price:.2f} | "
                    f"{p.quantity} | {pnl_color}{p.unrealized_pnl:.2f} | {p.unrealized_pnl_pct:.2%} | "
                    f"{p.stop_loss_price:.2f} | {p.take_profit_price:.2f} |"
                )
        
        return "\n".join(lines)


def integrate_with_validator(validator):
    """
    将外部验证模块集成到验证器
    
    Args:
        validator: RuleValidator实例
    """
    # 创建模拟器
    simulator = PaperTradingSimulator()
    
    validator.paper_simulator = simulator
    validator.create_paper_account = simulator.create_account
    validator.load_paper_account = simulator.load_account
    validator.place_order = simulator.place_order
    validator.get_paper_summary = simulator.get_account_summary
    validator.get_paper_trades = simulator.get_trade_history
    validator.get_paper_positions = simulator.get_open_positions
    validator.generate_paper_report = simulator.generate_performance_report
    
    return validator
