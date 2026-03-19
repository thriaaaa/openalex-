#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Works Topics Edge 模式映射脚本
将 works_topics.csv 映射为边数据格式 work_topic_e.csv
边类型: work -> topic (论文指向主题)
"""

import csv
import json
from pathlib import Path
import logging
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('schema_mapping.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WorksTopicsEdgeMapper:
    """Works Topics 到边数据格式的映射器"""
    
    def __init__(self, config_file="mapping_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)
        
        # 路径配置
        self.input_dir = Path(self.config.get('input_dir', '../openalex_merged'))
        self.output_base_dir = Path(self.config.get('edge_output_dir', '../graph_edges'))
        
        # 创建输出目录
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Works Topics边映射器初始化完成")
        logger.info(f"输入目录: {self.input_dir}")
        logger.info(f"输出目录: {self.output_base_dir}")
    
    @staticmethod
    def load_config(config_file):
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"配置文件 {config_file} 不存在，使用默认配置")
            return {}
    
    def map_to_edge(self, row):
        """将works_topics行映射为边数据格式
        
        Args:
            row: 原始works_topics CSV行（字典）
            
        Returns:
            dict: 边数据格式 {'startid': ..., 'endid': ..., 'properties': ...}
        """
        # 构建properties JSON对象
        properties = {}
        
        # score字段
        score = row.get('score', '')
        if score:
            try:
                # 尝试转换为浮点数以验证格式
                score_float = float(score)
                properties['score'] = score_float
            except:
                properties['score'] = score
        
        return {
            'startid': row.get('work_id', ''),       # 起始节点：论文ID
            'endid': row.get('topic_id', ''),        # 终止节点：主题ID
            'properties': json.dumps(properties, ensure_ascii=False)
        }
    
    def process_works_topics(self, batch_size=100000):
        """处理works_topics文件并生成边数据文件
        
        Args:
            batch_size: 批量写入大小
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"开始映射 works_topics.csv -> work_topic_e.csv")
        logger.info(f"{'='*60}")
        
        input_file = self.input_dir / 'works_topics.csv'
        output_file = self.output_base_dir / 'work_topic_e.csv'
        
        if not input_file.exists():
            logger.error(f"输入文件不存在: {input_file}")
            return False
        
        logger.info(f"读取: {input_file}")
        logger.info(f"输出: {output_file}")
        
        total_count = 0
        error_count = 0
        
        start_time = datetime.now()
        
        try:
            with open(input_file, 'r', encoding='utf-8') as inf:
                reader = csv.DictReader(inf)
                
                with open(output_file, 'w', encoding='utf-8', newline='') as outf:
                    # 边数据格式的列
                    fieldnames = ['startid', 'endid', 'properties']
                    writer = csv.DictWriter(outf, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    batch = []
                    
                    for i, row in enumerate(reader, 1):
                        try:
                            # 映射为边数据格式
                            edge_row = self.map_to_edge(row)
                            batch.append(edge_row)
                            
                            # 批量写入
                            if len(batch) >= batch_size:
                                writer.writerows(batch)
                                total_count += len(batch)
                                batch = []
                                logger.info(f"  已处理 {total_count:,} 行...")
                        
                        except Exception as e:
                            error_count += 1
                            if error_count <= 10:  # 只记录前10个错误
                                logger.error(f"  处理第 {i} 行失败: {e}")
                    
                    # 写入剩余数据
                    if batch:
                        writer.writerows(batch)
                        total_count += len(batch)
        
        except Exception as e:
            logger.error(f"处理失败: {e}")
            return False
        
        elapsed = datetime.now() - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✓ 映射完成:")
        logger.info(f"  总记录数: {total_count:,}")
        logger.info(f"  错误记录: {error_count:,}")
        if total_count > 0:
            logger.info(f"  成功率: {((total_count - error_count) / total_count * 100):.2f}%")
        logger.info(f"  耗时: {elapsed}")
        logger.info(f"  输出文件: {output_file}")
        
        if output_file.exists():
            file_size = output_file.stat().st_size / (1024**2)
            logger.info(f"  文件大小: {file_size:.2f} MB")
        
        logger.info(f"{'='*60}")
        
        return True
    
    def run(self):
        """运行映射流程"""
        logger.info(f"\n{'#'*80}")
        logger.info(f"Works Topics 边数据映射工具")
        logger.info(f"{'#'*80}\n")
        
        success = self.process_works_topics()
        
        if success:
            logger.info(f"\n✓ Works Topics 边数据映射完成！")
        else:
            logger.error(f"\n✗ Works Topics 边数据映射失败")
        
        return success


def main():
    """主函数"""
    mapper = WorksTopicsEdgeMapper()
    mapper.run()


if __name__ == '__main__':
    main()
