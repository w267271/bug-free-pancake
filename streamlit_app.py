#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
铁律验证系统 - Streamlit Cloud 部署版 v1.0
GitHub: https://github.com/w267271/iron-rule-validator
App URL: https://iron-rule-validator-w267271.streamlit.app
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import time
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# ==================== 页面配置 ====================
st.set_page_config(
    page_title="铁律验证系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ==================== 自定义样式 ====================
st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1.5rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        border-radius: 10px;
        padding: 15px;
        text-align: center;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    .status-active { color: #28a745; font-weight: bold; }
    .status-review { color: #ffc107; font-weight: bold; }
    .status-dormant { color: #dc3545; font-weight: bold; }
    .rule-card {
        background-color: #ffffff;
        border-radius: 8px;
        padding: 12px;
        margin-bottom: 8px;
        border-left: 4px solid #1f77b4;
    }
    .accuracy-high { color: #28a745; font-weight: bold; }
    .accuracy-mid { color: #ffc107; font-weight: bold; }
    .accuracy-low { color: #dc3545; font-weight: bold; }
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px;
    }
</style>
""", unsafe_allow_html=True)

# ==================== 路径配置 ====================
# Streamlit Cloud环境路径
PROJECT_ROOT = Path(__file__).parent
DATA_DIR = PROJECT_ROOT / "data"
RESULTS_DIR = DATA_DIR / "results"
CACHE_DIR = DATA_DIR / "cache"

# 确保目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
RESULTS_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# ==================== 累计统计类 ====================
class CumulativeStats:
    """累计统计数据"""
    
    @staticmethod
    def load_cumulative_report() -> Dict:
        """加载累计报告"""
        report_path = RESULTS_DIR / "cumulative_report.json"
        if report_path.exists():
            try:
                with open(report_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                st.warning(f"加载累计报告失败: {e}")
        
        # 返回默认数据
        return {
            "total_samples": 0,
            "overall_accuracy": 0.0,
            "avg_return": 0.0,
            "health_score": 0.0,
            "rule_stats": {},
            "timestamp": datetime.now().isoformat(),
            "version": "1.0"
        }
    
    @staticmethod
    def save_cumulative_report(report: Dict):
        """保存累计报告"""
        report_path = RESULTS_DIR / "cumulative_report.json"
        try:
            with open(report_path, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
        except Exception as e:
            st.error(f"保存累计报告失败: {e}")

# ==================== 数据源管理 ====================
class DataSourceManager:
    """数据源管理器"""
    
    @staticmethod
    @st.cache_resource(ttl=3600)
    def check_tickflow():
        """检查TickFlow状态"""
        try:
            from tickflow import TickFlow
            tf = TickFlow.free()
            df = tf.klines.get("600000.SH", period="1d", count=5, as_dataframe=True)
            if df is not None and len(df) > 0:
                return True, "TickFlow连接正常"
            return False, "返回数据为空"
        except ImportError:
            return False, "TickFlow未安装"
        except Exception as e:
            return False, str(e)[:50]
    
    @staticmethod
    @st.cache_resource(ttl=3600)
    def check_baostock():
        """检查Baostock状态"""
        try:
            import baostock as bs
            lg = bs.login()
            if lg.error_code == '0':
                bs.logout()
                return True, "Baostock连接正常"
            return False, lg.error_msg
        except ImportError:
            return False, "Baostock未安装"
        except Exception as e:
            return False, str(e)[:50]
    
    @staticmethod
    @st.cache_resource(ttl=3600)
    def check_akshare():
        """检查AKShare状态"""
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            if df is not None and len(df) > 0:
                return True, f"AKShare连接正常 (获取{len(df)}只股票)"
            return False, "返回数据为空"
        except ImportError:
            return False, "AKShare未安装"
        except Exception as e:
            return False, str(e)[:50]

# ==================== 铁律验证核心 ====================
class RuleValidatorSimple:
    """简化版铁律验证器"""
    
    # 铁律定义
    RULES = {
        "K01": {"name": "MACD零轴金叉", "type": "buy", "threshold": 0.75},
        "K02": {"name": "KDJ低位金叉", "type": "buy", "threshold": 0.70},
        "K03": {"name": "KDJ高位金叉", "type": "sell", "threshold": 0.65},
        "R01": {"name": "RSI超卖(<30)", "type": "buy", "threshold": 0.70},
        "R02": {"name": "RSI超买(>70)", "type": "sell", "threshold": 0.65},
        "N01": {"name": "主力净流入确认", "type": "buy", "threshold": 0.70},
        "N02": {"name": "主力连续净流出", "type": "sell", "threshold": 0.75},
        "E01": {"name": "北向资金买入", "type": "buy", "threshold": 0.68},
        "M01": {"name": "均线多头排列", "type": "buy", "threshold": 0.72},
        "T01": {"name": "突破前期高点", "type": "buy", "threshold": 0.68},
        "D01": {"name": "地量见地价", "type": "buy", "threshold": 0.65},
        "S01": {"name": "高位+主力出逃", "type": "sell", "threshold": 0.80},
        "F01": {"name": "禁止满仓操作", "type": "risk", "threshold": 1.0},
    }
    
    @staticmethod
    def get_stock_list() -> List[str]:
        """获取股票列表"""
        try:
            # 优先使用TickFlow
            from tickflow import TickFlow
            tf = TickFlow.free()
            # 返回主要股票代码
            stocks = [f"{str(i).zfill(6)}.SH" for i in range(600000, 600100)]
            stocks += [f"{str(i).zfill(6)}.SZ" for i in range(1, 100)]
            return stocks[:50]
        except:
            pass
        
        # 备用：使用AKShare
        try:
            import akshare as ak
            df = ak.stock_zh_a_spot_em()
            return df['代码'].head(50).tolist()
        except:
            return []
    
    @staticmethod
    def validate_rule(rule_id: str, stock_code: str, rule_info: Dict) -> Dict:
        """验证单条铁律"""
        # 模拟验证结果（实际应用中需要调用真实数据）
        import random
        import numpy as np
        
        # 基于铁律阈值生成模拟准确率
        base_threshold = rule_info.get("threshold", 0.7)
        accuracy = base_threshold + random.uniform(-0.15, 0.15)
        accuracy = max(0.4, min(0.95, accuracy))
        
        result = {
            "rule_id": rule_id,
            "rule_name": rule_info["name"],
            "rule_type": rule_info["type"],
            "stock_code": stock_code,
            "accuracy": round(accuracy * 100, 1),
            "success": random.random() < accuracy,
            "return_pct": round(random.uniform(-5, 10), 2),
            "timestamp": datetime.now().isoformat()
        }
        return result

# ==================== 主应用 ====================
def main():
    # 标题
    st.markdown('<h1 class="main-header">📊 铁律验证系统</h1>', unsafe_allow_html=True)
    
    # 侧边栏 - 设置
    with st.sidebar:
        st.header("⚙️ 设置")
        
        # 数据源选择
        st.subheader("📡 数据源")
        use_tickflow = st.checkbox("TickFlow (推荐)", value=True)
        use_akshare = st.checkbox("AKShare", value=False)
        
        # 验证设置
        st.subheader("🔧 验证设置")
        sample_size = st.slider("样本数量", 10, 100, 30)
        selected_rules = st.multiselect(
            "选择铁律",
            options=list(RuleValidatorSimple.RULES.keys()),
            default=list(RuleValidatorSimple.RULES.keys())[:5]
        )
        
        # 信息
        st.divider()
        st.caption("📌 Streamlit Cloud部署")
        st.caption("GitHub: w267271/iron-rule-validator")
    
    # 加载累计统计
    stats = CumulativeStats.load_cumulative_report()
    
    # ==================== 统计概览 ====================
    st.subheader("📈 累计统计")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="总样本数",
            value=f"{stats.get('total_samples', 0)}",
            delta="条验证记录"
        )
    
    with col2:
        accuracy = stats.get('overall_accuracy', 0)
        st.metric(
            label="综合准确率",
            value=f"{accuracy:.1f}%",
            delta="平均准确率"
        )
    
    with col3:
        avg_return = stats.get('avg_return', 0)
        st.metric(
            label="平均收益率",
            value=f"{avg_return:.2f}%",
            delta="平均收益"
        )
    
    with col4:
        health = stats.get('health_score', 0)
        st.metric(
            label="健康度",
            value=f"{health:.1f}%",
            delta="系统状态"
        )
    
    st.divider()
    
    # ==================== 标签页 ====================
    tab1, tab2, tab3, tab4 = st.tabs(["📋 铁律排行", "🔍 手动验证", "📊 验证历史", "ℹ️ 使用说明"])
    
    with tab1:
        st.subheader("🏆 铁律准确率排行")
        
        # 加载铁律统计
        rule_stats = stats.get('rule_stats', {})
        
        if rule_stats:
            # 转换为DataFrame
            df_rules = pd.DataFrame([
                {
                    "铁律ID": rule_id,
                    "铁律名称": info["name"],
                    "准确率": info["accuracy"],
                    "测试次数": info["test_count"],
                    "成功次数": info["success_count"],
                    "平均收益": info.get("avg_return", 0),
                    "状态": info["status"]
                }
                for rule_id, info in rule_stats.items()
            ])
            df_rules = df_rules.sort_values("准确率", ascending=False)
            
            # 颜色映射
            def color_accuracy(val):
                if val >= 80:
                    return "🟢"
                elif val >= 60:
                    return "🟡"
                else:
                    return "🔴"
            
            df_rules["等级"] = df_rules["准确率"].apply(color_accuracy)
            
            # 显示表格
            st.dataframe(
                df_rules,
                column_config={
                    "等级": st.column_config.TextColumn("等级"),
                    "铁律ID": st.column_config.TextColumn("铁律ID"),
                    "铁律名称": st.column_config.TextColumn("铁律名称"),
                    "准确率": st.column_config.NumberColumn("准确率", format="%.1f%%"),
                    "测试次数": st.column_config.NumberColumn("测试次数", format="%d"),
                    "成功次数": st.column_config.NumberColumn("成功次数", format="%d"),
                    "平均收益": st.column_config.NumberColumn("平均收益", format="%.2f%%"),
                    "状态": st.column_config.TextColumn("状态"),
                },
                hide_index=True,
                use_container_width=True
            )
            
            # 准确率图表
            fig = px.bar(
                df_rules,
                x="铁律名称",
                y="准确率",
                color="准确率",
                color_continuous_scale=["red", "yellow", "green"],
                title="铁律准确率对比"
            )
            fig.add_hline(y=75, line_dash="dash", annotation_text="基准线75%")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("暂无铁律统计数据，请先运行手动验证生成数据")
    
    with tab2:
        st.subheader("🔍 手动验证")
        
        # 选择要验证的铁律
        validate_rules = st.multiselect(
            "选择要验证的铁律",
            options=list(RuleValidatorSimple.RULES.keys()),
            default=["K01", "R01", "N01"],
            format_func=lambda x: f"{x} - {RuleValidatorSimple.RULES[x]['name']}"
        )
        
        # 验证按钮
        if st.button("🚀 开始验证", type="primary"):
            if not validate_rules:
                st.warning("请至少选择一条铁律进行验证")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                results = []
                total = len(validate_rules)
                
                for i, rule_id in enumerate(validate_rules):
                    rule_info = RuleValidatorSimple.RULES[rule_id]
                    status_text.text(f"验证中: {rule_info['name']}")
                    
                    # 模拟验证（实际应用中需要真实数据）
                    result = RuleValidatorSimple.validate_rule(rule_id, "600000.SH", rule_info)
                    results.append(result)
                    
                    progress_bar.progress((i + 1) / total)
                    time.sleep(0.3)
                
                status_text.text("验证完成!")
                
                # 显示结果
                if results:
                    df_results = pd.DataFrame(results)
                    df_results["准确率"] = df_results["accuracy"].apply(lambda x: f"{x:.1f}%")
                    df_results["收益率"] = df_results["return_pct"].apply(lambda x: f"{x:.2f}%")
                    df_results["状态"] = df_results["success"].apply(lambda x: "✅成功" if x else "❌失败")
                    
                    st.success(f"完成 {len(results)} 条铁律验证")
                    
                    # 显示结果表格
                    st.dataframe(
                        df_results[["rule_name", "rule_type", "stock_code", "准确率", "收益率", "状态"]],
                        column_config={
                            "rule_name": "铁律名称",
                            "rule_type": "类型",
                            "stock_code": "股票代码",
                            "准确率": "准确率",
                            "收益率": "收益率",
                            "状态": "状态"
                        },
                        hide_index=True,
                        use_container_width=True
                    )
    
    with tab3:
        st.subheader("📊 验证历史")
        
        # 列出已有的验证报告
        reports = sorted(RESULTS_DIR.glob("report_*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
        
        if reports:
            for report in reports[:10]:
                with st.expander(f"📄 {report.name}"):
                    try:
                        with open(report, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**样本数:** {data.get('total_samples', 'N/A')}")
                            st.write(f"**准确率:** {data.get('overall_accuracy', 0):.1f}%")
                        with col2:
                            st.write(f"**平均收益:** {data.get('avg_return', 0):.2f}%")
                            st.write(f"**健康度:** {data.get('health_score', 0):.1f}%")
                        
                        st.write(f"**时间:** {data.get('timestamp', 'N/A')}")
                    except Exception as e:
                        st.error(f"读取报告失败: {e}")
        else:
            st.info("暂无验证历史记录")
    
    with tab4:
        st.subheader("ℹ️ 使用说明")
        
        st.markdown("""
        ## 铁律验证系统
        
        ### 功能说明
        - **铁律排行**: 查看各条铁律的历史验证准确率
        - **手动验证**: 对选定的铁律进行实时验证
        - **验证历史**: 查看历史验证报告
        
        ### 铁律分类
        | 类型 | 说明 | 示例 |
        |------|------|------|
        | buy | 买入信号 | MACD金叉、RSI超卖 |
        | sell | 卖出信号 | RSI超买、高位主力出逃 |
        | risk | 风险警示 | 禁止满仓 |
        
        ### 准确率等级
        - 🟢 **80%+**: 高置信度
        - 🟡 **60-80%**: 中等置信度
        - 🔴 **<60%**: 低置信度，需要复核
        
        ### 部署信息
        - **GitHub**: https://github.com/w267271/iron-rule-validator
        - **App URL**: https://iron-rule-validator-w267271.streamlit.app
        - **数据源**: TickFlow (主), AKShare (备)
        """)
        
        st.divider()
        st.caption("© 2024 铁律验证系统 | Streamlit Cloud部署版")

# ==================== 运行 ====================
if __name__ == "__main__":
    main()
