#!/usr/bin/env python3
"""
数据集分析运行示例
使用方法：
1. 修改config_example.py中的配置参数
2. 运行此脚本开始分析
"""

import os
import sys
from config_example import *
from dataset_analysis_multithreaded import DatasetAnalyzer

def main():
    """
    运行数据集分析的主函数
    """
    print("=== 数据集分类分析工具 ===")
    
    # 检查必要的配置
    if not API_KEY:
        print("❌ 错误: 请在config_example.py中设置API_KEY")
        return False
    
    if not os.path.exists(PARQUET_FILE_PATH):
        print(f"❌ 错误: 数据集文件不存在: {PARQUET_FILE_PATH}")
        print("请检查config_example.py中的PARQUET_FILE_PATH设置")
        return False
    
    print(f"📁 数据集文件: {PARQUET_FILE_PATH}")
    print(f"🔧 配置: {MAX_WORKERS} 个并发线程, 批次大小 {BATCH_SIZE}, 推理温度 {TEMPERATURE}, 最大输出长度 {MAX_TOKENS}, 存储目录 {TEMP_DIR}")
    print(f"🔄 最大重试次数: {MAX_RETRIES}")
    
    # 询问是否继续
    confirm = input("\n是否开始分析? (y/N): ").strip().lower()
    if confirm not in ['y', 'yes']:
        print("已取消分析")
        return False
    
    # 创建分析器实例
    analyzer = DatasetAnalyzer(
        api_key=API_KEY,
        max_workers=MAX_WORKERS,
        max_retries=MAX_RETRIES,
        temperature=TEMPERATURE,
        max_len=MAX_TOKENS,
        output=TEMP_DIR,
        enable_mislabel_analysis=ENABLE_MISLABEL_ANALYSIS
    )
    
    try:
        print("\n🚀 开始分析...")
        
        # 开始分析
        results = analyzer.analyze_dataset(
            parquet_file=PARQUET_FILE_PATH,
            batch_size=BATCH_SIZE,
            start_from_batch=START_FROM_BATCH
        )
        
        # 检查是否有错误
        if 'error' in results:
            print(f"❌ 分析失败: {results['error']}")
            return False
        
        # 打印结果摘要
        print("\n✅ 分析完成!")
        print(f"📊 总计处理: {results['metadata']['total_processed']} 项")
        print(f"❌ 失败项目: {results['metadata']['total_failed']} 项")
        
        if results['metadata']['total_processed'] > 0:
            success_rate = (results['metadata']['total_processed'] / 
                          (results['metadata']['total_processed'] + results['metadata']['total_failed'])) * 100
            print(f"✅ 成功率: {success_rate:.1f}%")
            
            # 显示分类分布
            summary = results['summary']
            print("\n📈 分类分布:")
            for class_type, count in summary['classification_distribution'].items():
                percentage = (count / results['metadata']['total_processed']) * 100
                print(f"  {class_type}: {count} ({percentage:.1f}%)")
            
            # 显示准确性分析（当启用误标注分析时）
            if 'accuracy_analysis' in summary:
                accuracy = summary['accuracy_analysis']
                if accuracy['total_analyzed'] > 0:
                    correct_rate = (accuracy['correct_classifications'] / accuracy['total_analyzed']) * 100
                    print(f"\n🎯 分类准确性: {correct_rate:.1f}% ({accuracy['correct_classifications']}/{accuracy['total_analyzed']})")
            
            # 显示高频关键词
            print("\n🔍 高频关键词 (前10个):")
            for i, (keyword, freq) in enumerate(list(summary['top_keywords'].items())[:10], 1):
                print(f"  {i}. {keyword}: {freq}")
        
        print(f"\n💾 详细结果已保存到: {FINAL_RESULTS_FILE}")
        print(f"📁 中间结果目录: {TEMP_DIR}")
        print(f"📝 日志文件: {LOG_FILE}")
        
        return True
        
    except KeyboardInterrupt:
        print("\n⏹️ 用户中断了分析过程")
        print(f"💡 提示: 可以设置START_FROM_BATCH参数从中断处恢复")
        return False
        
    except Exception as e:
        print(f"\n❌ 分析过程中出现错误: {e}")
        return False

def resume_analysis():
    """
    恢复中断的分析任务
    """
    checkpoint_file = os.path.join(TEMP_DIR, "checkpoint.json")
    
    if not os.path.exists(checkpoint_file):
        print("❌ 没有找到检查点文件，无法恢复")
        return False
    
    try:
        import json
        with open(checkpoint_file, 'r', encoding='utf-8') as f:
            checkpoint = json.load(f)
        
        last_batch = checkpoint.get('last_completed_batch', -1)
        total_completed = checkpoint.get('total_completed', 0)
        
        print(f"🔄 发现检查点:")
        print(f"  上次完成批次: {last_batch}")
        print(f"  已完成项目: {total_completed}")
        print(f"  时间戳: {checkpoint.get('timestamp', 'unknown')}")
        
        confirm = input(f"\n是否从批次 {last_batch + 1} 开始恢复? (y/N): ").strip().lower()
        if confirm in ['y', 'yes']:
            # 更新配置并运行
            global START_FROM_BATCH
            START_FROM_BATCH = last_batch + 1
            return main()
        else:
            print("已取消恢复")
            return False
            
    except Exception as e:
        print(f"❌ 读取检查点失败: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--resume":
        # 恢复模式
        resume_analysis()
    else:
        # 正常模式
        main()
