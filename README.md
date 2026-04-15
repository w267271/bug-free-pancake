# 铁律验证系统 (Iron Rule Validator)

📊 A股铁律验证系统，基于历史数据验证交易铁律的准确率。

## 功能特性

- ✅ **铁律排行**: 查看各条铁律的历史验证准确率
- ✅ **手动验证**: 对选定的铁律进行实时验证
- ✅ **验证历史**: 查看历史验证报告
- ✅ **数据源支持**: TickFlow (主), AKShare (备)

## 在线访问

🌐 **App URL**: https://iron-rule-validator-w267271.streamlit.app

## 部署

### Streamlit Cloud 部署

1. **Fork 本仓库到 GitHub**
   ```
   https://github.com/w267271/iron-rule-validator
   ```

2. **在 Streamlit Cloud 中部署**
   - 访问 https://streamlit.io/cloud
   - 点击 "New app"
   - 选择 `w267271/iron-rule-validator` 仓库
   - 设置 Main file path 为 `铁律验证系统/streamlit_app.py`
   - 点击 "Deploy!"

3. **预期 URL**
   ```
   https://iron-rule-validator-w267271.streamlit.app
   ```

### 本地运行

```bash
cd 铁律验证系统
pip install -r requirements.txt
streamlit run streamlit_app.py
```

## 目录结构

```
铁律验证系统/
├── streamlit_app.py      # Streamlit Cloud 主应用
├── app.py                 # 本地完整版应用
├── main.py                # 核心验证逻辑
├── requirements.txt       # Python依赖
├── .streamlit/
│   ├── config.toml        # Streamlit配置
│   └── secrets.toml.example  # Secrets示例
├── data/
│   ├── results/           # 验证报告
│   ├── cache/             # 数据缓存
│   └── rule_library/      # 铁律库
└── core/                  # 核心模块
```

## 铁律准确率等级

| 等级 | 准确率 | 说明 |
|------|--------|------|
| 🟢 高置信度 | 80%+ | 推荐使用 |
| 🟡 中等置信度 | 60-80% | 参考使用 |
| 🔴 低置信度 | <60% | 需复核 |

## 数据源

- **TickFlow** (推荐): 免费股票数据API
- **AKShare** (备用): 开源财经数据接口

## 许可证

MIT License
