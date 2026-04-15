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

# ==================== 密码保护 ====================
def check_password():
    """密码验证 - 保护应用不被未授权访问"""
    def password_entered():
        if st.session_state["password"] == "tie_lu_2024":
            st.session_state["password_correct"] = True
            del st.session_state["password"]
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        st.markdown("""
        <div style='text-align: center; padding: 50px;'>
            <h1>🔐 铁律验证系统</h1>
            <p style='color: gray;'>请输入访问密码</p>
        </div>
        """, unsafe_allow_html=True)
        st.text_input("密码", type="password", on_change=password_entered, key="password")
        return False
    elif not st.session_state["password_correct"]:
        st.markdown("""
        <div style='text-align: center; padding: 50px;'>
            <h1>🔐 铁律验证系统</h1>
            <p style='color: gray;'>请输入访问密码</p>
        </div>
        """, unsafe_allow_html=True)
        st.text_input("密码", type="password", on_change=password_entered, key="password")
        st.error("❌ 密码错误，请重试")
        return False
    else:
        return True

if not check_password():
    st.stop()
