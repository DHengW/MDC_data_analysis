"""
配置文件示例
"""

# API配置
API_KEY = ""  # 请填写您的智谱AI API密钥

# 文件路径配置
PARQUET_FILE_PATH = "mdc_test_v1.parquet"  # https://drive.google.com/file/d/1_A_pOf3xgZkQGi1CPAQcXIwIViyzedC0/view?usp=drive_link

# 并发配置
MAX_WORKERS = 5  # 最大并发线程数，建议根据API配额调整
BATCH_SIZE = 50  # 每批处理的数据量，可根据内存情况调整
MAX_RETRIES = 5  # 最大重试次数

# 输出配置
TEMP_DIR = "temp_results"  # 临时结果存储目录
FINAL_RESULTS_FILE = "final_analysis_results.json"  # 最终结果文件
LOG_FILE = "dataset_analysis.log"  # 日志文件

# 模型配置
MODEL_NAME = "glm-4.5"
MAX_TOKENS = 10000
TEMPERATURE = 0.3  # 较低的温度值以获得更一致的结果

# 恢复配置
START_FROM_BATCH = 0  # 如果需要从中断处恢复，设置为具体的批次编号
