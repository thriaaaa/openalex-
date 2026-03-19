#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Works 模式映射脚本
将 works.csv 映射为图数据库顶点格式 works_v.csv
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


class WorksVertexMapper:
    """Works 到顶点格式的映射器"""
    
    def __init__(self, config_file="mapping_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)
        
        # 路径配置
        self.input_dir = Path(self.config.get('input_dir', '../openalex_merged'))
        self.output_base_dir = Path(self.config.get('output_dir', '../graph_vertex'))
        
        # 创建输出目录
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Works顶点映射器初始化完成")
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
    
    def map_work_to_vertex(self, work_row):
        """将work行映射为顶点格式
        
        Args:
            work_row: 原始work CSV行（字典）
            
        Returns:
            dict: 顶点格式 {'id': ..., 'properties': ...}
        """
        # 构建properties JSON对象（只包含必需字段）
        properties = {
            'title': work_row.get('title', ''),
            'publication_year': work_row.get('publication_year', ''),
            'publication_date': work_row.get('publication_date', ''),
            'type': work_row.get('type', ''),
            'cited_by_count': work_row.get('cited_by_count', '0'),
            'is_retracted': work_row.get('is_retracted', 'False'),
            'is_paratext': work_row.get('is_paratext', 'False')
        }
        
        return {
            'id': work_row.get('id', ''),
            'properties': json.dumps(properties, ensure_ascii=False)
        }
    
    def process_works(self, batch_size=100000):
        """处理works文件并生成顶点文件"""
        logger.info(f"\n{'='*60}")
        logger.info(f"开始映射 works.csv -> works_v.csv")
        logger.info(f"{'='*60}")
        
        input_file = self.input_dir / 'works.csv'
        output_file = self.output_base_dir / 'works_v.csv'
        
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
                    # 顶点格式的列
                    fieldnames = ['id', 'properties']
                    writer = csv.DictWriter(outf, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    batch = []
                    
                    for i, row in enumerate(reader, 1):
                        try:
                            # 映射为顶点格式
                            vertex = self.map_work_to_vertex(row)
                            batch.append(vertex)
                            
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
        logger.info(f"Works 顶点映射工具")
        logger.info(f"{'#'*80}\n")
        
        success = self.process_works()
        
        if success:
            logger.info(f"\n✓ Works 顶点映射完成！")
        else:
            logger.error(f"\n✗ Works 顶点映射失败")
        
        return success


def main():
    """主函数"""
    mapper = WorksVertexMapper()
    mapper.run()


if __name__ == '__main__':
    main()
