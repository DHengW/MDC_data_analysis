import pandas as pd
import json
import os
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime
import pickle
from zai import ZhipuAiClient

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dataset_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatasetAnalyzer:
    def __init__(self, api_key: str, max_workers: int = 5, max_retries: int = 5, temperature: float = 0.1, max_len: int = 10000, output: str = "temp_results", enable_mislabel_analysis: bool = True):
        """
        初始化数据集分析器
        
        Args:
            api_key: ZhipuAI API密钥
            max_workers: 最大并发线程数
            max_retries: 最大重试次数
        """
        self.api_key = api_key
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.client = ZhipuAiClient(api_key=api_key)
        self.temperature = temperature
        self.max_len = max_len
        self.enable_mislabel_analysis = enable_mislabel_analysis
        
        # 创建临时文件目录
        self.temp_dir = output
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # 线程锁
        self.lock = threading.Lock()
        
        # 结果存储
        self.completed_count = 0
        self.failed_items = []
        
    def create_analysis_prompt(self, target_dataset_id: str, article_id: str, 
                             aggregated_text: str, type_label: str) -> str:
        """
        创建分析提示词
        """
        if self.enable_mislabel_analysis:
            prompt = f"""
作为数据集分类专家，请分析以下数据集引用的分类原因。

数据集信息：
- 目标数据集ID: {target_dataset_id}
- 文章ID: {article_id}
- 分类类型: {type_label}

聚合文本内容：
{aggregated_text}

分类标准(粗略版)：
A) Primary - 专门为本研究生成的数据
   - 作者为此研究创建的原始实验数据、测量或观察
   - 作者产生并存放的新数据集
   - 专门收集来回答本文研究问题的数据

B) Secondary - 重用或源自现有来源的数据
   - 之前发布的数据集被下载并重新分析
   - 检索的公共数据库记录用于比较分析
   - 现有数据被重新用于新的研究问题

C) None - 不是数据集引用或不相关
   - 对其他论文的引用（非数据集）
   - 对方法、软件或工具的引用
   - 在与数据使用无关的上下文中提及
   - 提及的数据库标识符但没有实际数据使用

请分析：
1. 为什么这个数据集ID被归类为"{type_label}"类型？
2. 在聚合文本中有哪些关键词或短语支持这个分类？
3. 从可复用且不局限某一个数据集ID的角度出发，这个分类的上下文规律是什么？
4. 如果分类错误，正确的分类应该是什么，为什么？

请以JSON格式返回分析结果：
{{
    "target_dataset_id": "{target_dataset_id}",
    "article_id": "{article_id}",
    "original_classification": "{type_label}",
    "analysis_reason": "详细分析分类原因",
    "supporting_keywords": ["关键词1", "关键词2", "..."],
    "context_pattern": "归纳的可复用的上下文规律，最好不针对具体某一个数据集",
    "is_correct_classification": true/false,
    "suggested_classification": "如果原分类错误，建议的正确分类",
    "confidence_score": 0.95
}}
"""
        else:
            prompt = f"""
作为数据集分类专家，请分析以下数据集引用的分类原因。

数据集信息：
- 目标数据集ID: {target_dataset_id}
- 文章ID: {article_id}
- 分类类型: {type_label}

聚合文本内容：
{aggregated_text}

分类标准(粗略版)：
A) Primary - 专门为本研究生成的数据
   - 作者为此研究创建的原始实验数据、测量或观察
   - 作者产生并存放的新数据集
   - 专门收集来回答本文研究问题的数据

B) Secondary - 重用或源自现有来源的数据
   - 之前发布的数据集被下载并重新分析
   - 检索的公共数据库记录用于比较分析
   - 现有数据被重新用于新的研究问题

C) None - 不是数据集引用或不相关
   - 对其他论文的引用（非数据集）
   - 对方法、软件或工具的引用
   - 在与数据使用无关的上下文中提及
   - 提及的数据库标识符但没有实际数据使用

请分析：
1. 为什么这个数据集ID被归类为"{type_label}"类型？
2. 在聚合文本中有哪些关键词或短语支持这个分类？
3. 从可复用且不局限某一个数据集ID的角度出发，这个分类的上下文规律是什么？

请以JSON格式返回分析结果：
{{
    "target_dataset_id": "{target_dataset_id}",
    "article_id": "{article_id}",
    "original_classification": "{type_label}",
    "analysis_reason": "详细分析分类原因",
    "supporting_keywords": ["关键词1", "关键词2", "..."],
    "context_pattern": "归纳的可复用的上下文规律，最好不针对具体某一个数据集"
}}
"""
        return prompt
        
    def call_api_with_retry(self, prompt: str, item_id: str) -> Optional[Dict]:
        """
        带重试机制的API调用
        """
        for attempt in range(self.max_retries):
            try:
                response = self.client.chat.completions.create(
                    model="glm-4.5",
                    messages=[
                        {"role": "user", "content": prompt}
                    ],
                    thinking={
                        "type": "enabled",
                    },
                    stream=False,
                    max_tokens=self.max_len,
                    temperature=self.temperature  # 降低温度以获得更一致的结果
                )
                
                content = response.choices[0].message.content
                
                # 尝试解析JSON响应
                try:
                    # 提取JSON部分（如果响应包含其他文本）
                    if '```json' in content:
                        json_start = content.find('```json') + 7
                        json_end = content.find('```', json_start)
                        json_content = content[json_start:json_end].strip()
                    elif '{' in content and '}' in content:
                        json_start = content.find('{')
                        json_end = content.rfind('}') + 1
                        json_content = content[json_start:json_end]
                    else:
                        json_content = content
                    
                    result = json.loads(json_content)
                    logger.info(f"成功处理项目 {item_id}, 尝试次数: {attempt + 1}")
                    return result
                    
                except json.JSONDecodeError as e:
                    logger.warning(f"JSON解析失败 {item_id}, 尝试次数 {attempt + 1}: {e}")
                    if attempt == self.max_retries - 1:
                        # 如果是最后一次尝试，保存原始响应
                        return {
                            "error": "JSON解析失败",
                            "raw_response": content,
                            "item_id": item_id
                        }
                    
            except Exception as e:
                logger.error(f"API调用失败 {item_id}, 尝试次数 {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(2 ** attempt)  # 指数退避
                else:
                    return {
                        "error": str(e),
                        "item_id": item_id
                    }
        
        return None
    
    def save_intermediate_result(self, result: Dict, batch_id: int, item_index: int):
        """
        保存中间结果到临时文件
        """
        filename = f"{self.temp_dir}/batch_{batch_id}_item_{item_index}.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存中间结果失败 {filename}: {e}")
    
    def process_single_item(self, item: Tuple[int, pd.Series], batch_id: int) -> Optional[Dict]:
        """
        处理单个数据项
        """
        index, row = item
        
        try:
            prompt = self.create_analysis_prompt(
                target_dataset_id=str(row['target_dataset_id']),
                article_id=str(row['article_id']),
                aggregated_text=str(row['aggregated_text']),
                type_label=str(row['type'])
            )
            
            result = self.call_api_with_retry(prompt, f"{batch_id}_{index}")
            
            if result:
                # 添加原始数据信息
                result['original_data'] = {
                    'index': index,
                    'target_dataset_id': str(row['target_dataset_id']),
                    'article_id': str(row['article_id']),
                    'type': str(row['type'])
                }
                
                # 保存中间结果
                self.save_intermediate_result(result, batch_id, index)
                
                with self.lock:
                    self.completed_count += 1
                    logger.info(f"已完成: {self.completed_count} 项")
                
                return result
            
        except Exception as e:
            logger.error(f"处理项目失败 {batch_id}_{index}: {e}")
            with self.lock:
                self.failed_items.append({'batch_id': batch_id, 'index': index, 'error': str(e)})
        
        return None
    
    def process_batch(self, data_batch: pd.DataFrame, batch_id: int) -> List[Dict]:
        """
        处理数据批次
        """
        logger.info(f"开始处理批次 {batch_id}, 包含 {len(data_batch)} 项")
        
        results = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_item = {
                executor.submit(self.process_single_item, item, batch_id): item 
                for item in data_batch.iterrows()
            }
            
            # 收集结果
            for future in as_completed(future_to_item):
                result = future.result()
                if result:
                    results.append(result)
        
        logger.info(f"批次 {batch_id} 完成, 成功处理 {len(results)} 项")
        return results
    
    def save_batch_results(self, results: List[Dict], batch_id: int):
        """
        保存批次结果
        """
        filename = f"{self.temp_dir}/batch_{batch_id}_results.json"
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2)
            logger.info(f"批次 {batch_id} 结果已保存到 {filename}")
        except Exception as e:
            logger.error(f"保存批次结果失败 {filename}: {e}")
    
    def load_existing_results(self) -> List[Dict]:
        """
        加载现有的中间结果
        """
        all_results = []
        
        for filename in os.listdir(self.temp_dir):
            if filename.endswith('_results.json'):
                try:
                    with open(os.path.join(self.temp_dir, filename), 'r', encoding='utf-8') as f:
                        batch_results = json.load(f)
                        all_results.extend(batch_results)
                        logger.info(f"加载现有结果: {filename}, {len(batch_results)} 项")
                except Exception as e:
                    logger.error(f"加载结果文件失败 {filename}: {e}")
        
        return all_results
    
    def analyze_dataset(self, parquet_file: str, batch_size: int = 50, 
                       start_from_batch: int = 0) -> Dict:
        """
        分析整个数据集
        
        Args:
            parquet_file: parquet文件路径
            batch_size: 每批处理的数据量
            start_from_batch: 从哪个批次开始（用于恢复中断的任务）
        """
        logger.info(f"开始分析数据集: {parquet_file}")
        
        # 读取数据
        try:
            df = pd.read_parquet(parquet_file)
            logger.info(f"成功读取数据集，共 {len(df)} 行")
        except Exception as e:
            logger.error(f"读取parquet文件失败: {e}")
            return {"error": f"读取文件失败: {e}"}
        
        # 验证必要的列
        required_columns = ['target_dataset_id', 'article_id', 'aggregated_text', 'type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            error_msg = f"缺少必要的列: {missing_columns}"
            logger.error(error_msg)
            return {"error": error_msg}
        
        # 如果从中间开始，加载现有结果
        if start_from_batch > 0:
            existing_results = self.load_existing_results()
            logger.info(f"加载了 {len(existing_results)} 个现有结果")
        
        all_results = []
        total_batches = (len(df) + batch_size - 1) // batch_size
        
        # 分批处理
        for batch_num in range(start_from_batch, total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(df))
            
            batch_data = df.iloc[start_idx:end_idx].copy()
            
            logger.info(f"处理批次 {batch_num + 1}/{total_batches} (行 {start_idx}-{end_idx})")
            
            batch_results = self.process_batch(batch_data, batch_num)
            
            if batch_results:
                self.save_batch_results(batch_results, batch_num)
                all_results.extend(batch_results)
            
            # 保存检查点
            checkpoint = {
                'last_completed_batch': batch_num,
                'total_completed': len(all_results),
                'failed_items': self.failed_items,
                'timestamp': datetime.now().isoformat()
            }
            
            with open(f"{self.temp_dir}/checkpoint.json", 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, ensure_ascii=False, indent=2)
        
        # 汇总结果
        summary = self.generate_summary(all_results)
        
        # 保存最终结果
        final_results = {
            'summary': summary,
            'detailed_results': all_results,
            'failed_items': self.failed_items,
            'metadata': {
                'total_processed': len(all_results),
                'total_failed': len(self.failed_items),
                'processing_time': datetime.now().isoformat(),
                'batch_size': batch_size
            }
        }
        
        with open('final_analysis_results.json', 'w', encoding='utf-8') as f:
            json.dump(final_results, f, ensure_ascii=False, indent=2)
        
        logger.info(f"分析完成！总计处理 {len(all_results)} 项，失败 {len(self.failed_items)} 项")
        
        return final_results
    
    def generate_summary(self, results: List[Dict]) -> Dict:
        """
        生成分析摘要
        """
        if not results:
            return {"error": "没有有效结果"}
        
        # 统计分类分布
        classification_dist = {}
        pattern_analysis = {}
        keyword_frequency = {}
        
        for result in results:
            if 'error' not in result:
                # 分类分布
                orig_class = result.get('original_classification', 'unknown')
                classification_dist[orig_class] = classification_dist.get(orig_class, 0) + 1
                
                # 上下文规律
                pattern = result.get('context_pattern', '')
                if pattern:
                    pattern_analysis[orig_class] = pattern_analysis.get(orig_class, [])
                    pattern_analysis[orig_class].append(pattern)
                
                # 关键词频率
                keywords = result.get('supporting_keywords', [])
                for keyword in keywords:
                    keyword_frequency[keyword] = keyword_frequency.get(keyword, 0) + 1
        
        # 生成摘要
        summary = {
            'classification_distribution': classification_dist,
            'top_keywords': dict(sorted(keyword_frequency.items(), 
                                      key=lambda x: x[1], reverse=True)[:20]),
            'context_patterns_by_type': pattern_analysis
        }
        if self.enable_mislabel_analysis:
            summary['accuracy_analysis'] = {
                'total_analyzed': len([r for r in results if 'error' not in r]),
                'correct_classifications': len([r for r in results 
                                              if 'error' not in r and 
                                              r.get('is_correct_classification', True)]),
                'incorrect_classifications': len([r for r in results 
                                                if 'error' not in r and 
                                                not r.get('is_correct_classification', True)])
            }
        
        return summary

def main():
    """
    主函数示例
    """
    # 配置参数
    API_KEY = ""  # 请填写您的API密钥
    PARQUET_FILE = "your_dataset.parquet"  # 请填写您的parquet文件路径
    MAX_WORKERS = 5  # 并发线程数
    BATCH_SIZE = 50  # 每批处理的数据量
    
    if not API_KEY:
        print("请先设置API_KEY")
        return
    
    if not os.path.exists(PARQUET_FILE):
        print(f"文件不存在: {PARQUET_FILE}")
        return
    
    # 创建分析器实例
    analyzer = DatasetAnalyzer(
        api_key=API_KEY,
        max_workers=MAX_WORKERS,
        max_retries=5
    )
    
    # 开始分析
    try:
        results = analyzer.analyze_dataset(
            parquet_file=PARQUET_FILE,
            batch_size=BATCH_SIZE,
            start_from_batch=0  # 从头开始，如果需要恢复可以设置为具体批次号
        )
        
        print("分析完成！")
        print(f"成功处理: {results['metadata']['total_processed']} 项")
        print(f"失败项目: {results['metadata']['total_failed']} 项")
        
        # 打印摘要
        summary = results['summary']
        print("\n分类分布:")
        for class_type, count in summary['classification_distribution'].items():
            print(f"  {class_type}: {count}")
        
        print("\n高频关键词:")
        for keyword, freq in list(summary['top_keywords'].items())[:10]:
            print(f"  {keyword}: {freq}")
            
    except Exception as e:
        logger.error(f"分析过程中出现错误: {e}")
        print(f"分析失败: {e}")

if __name__ == "__main__":
    main()
