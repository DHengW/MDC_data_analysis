# 数据集分类分析工具

这是一个基于智谱AI API的多线程数据集分类分析工具，用于分析parquet格式的数据集分类标签准确性和归纳分类规律。

## 功能特性

- ✅ **多线程并发处理**: 支持多线程并发调用API，提高处理效率
- 🔄 **智能重试机制**: 内置5次重试机制，确保API调用的稳定性
- 💾 **中间结果保存**: 实时保存中间结果到临时文件，防止数据丢失
- ⏯️ **断点恢复**: 支持从中断处恢复分析任务
- 📊 **详细统计分析**: 生成分类分布、关键词频率、准确性等统计信息
- 📝 **完整日志记录**: 详细的处理日志，便于问题排查

## 安装要求

在运行之前，请确保安装以下Python依赖：

```bash
pip install pandas zai
```

## 文件结构

```
.
├── dataset_analysis_multithreaded.py  # 主分析模块
├── config_example.py                  # 配置文件示例
├── run_analysis_example.py           # 运行示例脚本
├── README.md                          # 使用说明
└── temp_results/                      # 临时结果目录（自动创建）
    ├── batch_0_results.json          # 批次结果文件
    ├── batch_0_item_0.json           # 单项结果文件
    └── checkpoint.json                # 检查点文件
```

## 数据格式要求

输入的parquet文件必须包含以下四列：

- `target_dataset_id`: 目标数据集ID
- `article_id`: 文章唯一识别码  
- `aggregated_text`: 数据集ID所在的聚合文本chunk
- `type`: 数据集类型标签 (Primary/Secondary/None)

### 分类标准

**A) Primary - 专门为本研究生成的数据**
- 作者为此研究创建的原始实验数据、测量或观察
- 作者产生并存放的新数据集
- 专门收集来回答本文研究问题的数据

**B) Secondary - 重用或源自现有来源的数据**
- 之前发布的数据集被下载并重新分析
- 检索的公共数据库记录用于比较分析
- 现有数据被重新用于新的研究问题

**C) None - 不是数据集引用或不相关**
- 对其他论文的引用（非数据集）
- 对方法、软件或工具的引用
- 在与数据使用无关的上下文中提及
- 提及的数据库标识符但没有实际数据使用


## 使用方法

### 1. 配置设置

编辑 `config_example.py` 文件，设置必要的参数：

```python
# API配置
API_KEY = "your_zhipu_ai_api_key"  # 必须：您的智谱AI API密钥

# 文件路径配置
PARQUET_FILE_PATH = "your_dataset.parquet"  # 必须：您的数据集文件路径

# 并发配置
MAX_WORKERS = 5      # 并发线程数，建议根据API配额调整
BATCH_SIZE = 50      # 每批处理数据量
MAX_RETRIES = 5      # 最大重试次数

# 输出配置
TEMP_DIR = "temp_results"  # 临时结果存储目录
FINAL_RESULTS_FILE = "final_analysis_results.json"  # 最终结果文件
LOG_FILE = "dataset_analysis.log"  # 日志文件

# 模型配置
MODEL_NAME = "glm-4.5"  # 使用的智谱AI模型版本
MAX_TOKENS = 10000     # 最大输出token数
TEMPERATURE = 0.1      # 推理温度，较低值获得更一致的结果

# 分析选项
ENABLE_MISLABEL_ANALYSIS = False  # 是否让模型判断是否误标注；False仅做类型分析与规律总结

# 恢复配置
START_FROM_BATCH = 0   # 如果需要从中断处恢复，设置为具体的批次编号
```

### 2. 运行分析

#### 方法一：使用运行脚本（推荐）

```bash
# 开始新的分析
python run_analysis_example.py

# 从断点恢复分析
python run_analysis_example.py --resume
```

#### 方法二：直接调用API

```python
from dataset_analysis_multithreaded import DatasetAnalyzer
from config_example import *

# 创建分析器
analyzer = DatasetAnalyzer(
    api_key=API_KEY,
    max_workers=MAX_WORKERS,
    max_retries=MAX_RETRIES,
    temperature=TEMPERATURE,
    max_len=MAX_TOKENS,
    output=TEMP_DIR,
    enable_mislabel_analysis=ENABLE_MISLABEL_ANALYSIS
)

# 开始分析
results = analyzer.analyze_dataset(
    parquet_file=PARQUET_FILE_PATH,
    batch_size=BATCH_SIZE,
    start_from_batch=0  # 设置为具体批次号可从断点恢复
)
```

### 3. 查看结果

分析完成后会生成以下文件：

- `final_analysis_results.json`: 最终分析结果
- `temp_results/`: 中间结果目录
- `dataset_analysis.log`: 详细日志文件

## 输出结果格式

### 最终结果文件结构

```json
{
  "summary": {
    "classification_distribution": {
      "Primary": 150,
      "Secondary": 200,
      "None": 100
    },
    "top_keywords": {
      "dataset": 45,
      "data": 38,
      "analysis": 25
    },
    "context_patterns_by_type": {
      "Primary": ["原始实验数据模式", "新数据集创建模式"],
      "Secondary": ["重用现有数据模式", "公开数据库分析模式"]
    },
    "accuracy_analysis": {
      "total_analyzed": 450,
      "correct_classifications": 420,
      "incorrect_classifications": 30
    }
  },
  "detailed_results": [
    {
      "target_dataset_id": "dataset_001",
      "article_id": "article_001", 
      "original_classification": "Primary",
      "analysis_reason": "详细分析原因...",
      "supporting_keywords": ["experimental", "original", "generated"],
      "context_pattern": "归纳的上下文规律",
      "is_correct_classification": true,
      "suggested_classification": "Primary",
      "confidence_score": 0.95
    }
  ],
  "failed_items": [],
  "metadata": {
    "total_processed": 450,
    "total_failed": 0,
    "processing_time": "2024-01-01T12:00:00",
    "batch_size": 50
  }
}
```

## 性能优化建议

### API调用优化

1. **并发数设置**: 根据API配额调整`MAX_WORKERS`，通常建议3-5个
2. **批次大小**: 内存充足时可适当增加`BATCH_SIZE`
3. **重试策略**: 网络不稳定时可增加`MAX_RETRIES`

### 内存优化

1. **大数据集处理**: 超过10万条记录时建议将`BATCH_SIZE`设为20-30
2. **文本长度**: 如果`aggregated_text`很长，考虑预处理截断

### 错误处理

1. **API限流**: 如遇到API限流，会自动重试并增加延迟
2. **网络错误**: 内置指数退避策略处理网络问题
3. **JSON解析错误**: 自动尝试提取JSON内容

## 故障恢复

### 从断点恢复

如果分析过程中断，可以通过以下方式恢复：

```bash
# 自动检测并恢复
python run_analysis_example.py --resume

# 或手动设置起始批次
# 在config_example.py中设置: START_FROM_BATCH = 10
python run_analysis_example.py
```

### 检查中间结果

```python
import json
import os

# 查看检查点信息
with open('temp_results/checkpoint.json', 'r') as f:
    checkpoint = json.load(f)
    print(f"上次完成批次: {checkpoint['last_completed_batch']}")
    print(f"已完成项目: {checkpoint['total_completed']}")

# 查看特定批次结果
with open('temp_results/batch_0_results.json', 'r') as f:
    batch_results = json.load(f)
    print(f"批次0包含 {len(batch_results)} 个结果")
```

## 常见问题

### Q1: API调用失败

**A**: 检查API密钥是否正确，网络是否稳定。工具会自动重试，通常能自行恢复。

### Q2: 内存不足

**A**: 减少`BATCH_SIZE`和`MAX_WORKERS`参数。

### Q3: 结果不准确

**A**: 可以调整prompt模板或降低`temperature`参数以获得更一致的结果。

### Q4: 处理速度慢

**A**: 在API配额允许的情况下增加`MAX_WORKERS`，或检查网络连接。

## 高级用法

### 自定义prompt模板

可以在`DatasetAnalyzer`类中修改`create_analysis_prompt`方法来自定义分析提示词。

### 添加新的分析维度

可以扩展结果JSON结构，添加更多分析字段。

### 集成其他大模型

可以替换`ZhipuAiClient`为其他大模型的客户端。

## 注意事项

1. **API配额**: 注意智谱AI的API调用配额限制
2. **文本长度**: 确保聚合文本不超过模型的上下文限制
3. **磁盘空间**: 大数据集会产生大量临时文件，确保有足够磁盘空间
4. **网络稳定性**: 长时间运行需要稳定的网络连接

## 技术支持

如遇问题，请检查：
1. 日志文件 `dataset_analysis.log`
2. 检查点文件 `temp_results/checkpoint.json`  
3. 失败项目信息在最终结果的`failed_items`字段中
