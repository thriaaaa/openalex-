#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 works_referenced_works.csv 映射为论文引用边表
输出格式: startid, endid, properties
其中 startid 是引用论文，endid 是被引用论文，properties 为空对象 {}
"""

import csv
import json
import logging
from pathlib import Path
from datetime import datetime

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('work_referenced_works_mapping.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class WorkReferencedWorksEdgeMapper:
    """论文引用关系边映射器"""
    
    def __init__(self, config_file="mapping_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)
        self.mapping_config = self.config['works_referenced_works_edge_mapping']
        
        # 路径配置
        self.input_dir = Path(self.config['input_dir'])
        self.edge_output_dir = Path(self.config['edge_output_dir'])
        
        # 输入文件
        self.input_file = self.input_dir / "works_referenced_works.csv"
        
        # 输出文件
        self.output_file = self.edge_output_dir / self.mapping_config['output_file']
        
        # 创建输出目录
        self.edge_output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("论文引用边映射器初始化完成")
    
    @staticmethod
    def load_config(config_file):
        """加载配置文件"""
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def map_to_edge(self):
        """执行映射"""
        logger.info(f"\n{'='*60}")
        logger.info(f"开始映射: 论文引用关系 -> 边表")
        logger.info(f"{'='*60}")
        logger.info(f"输入文件: {self.input_file}")
        logger.info(f"输出文件: {self.output_file}")
        
        start_time = datetime.now()
        
        total = 0
        written = 0
        skipped = 0
        
        try:
            with open(self.input_file, 'r', encoding='utf-8') as inf:
                reader = csv.DictReader(inf)
                
                with open(self.output_file, 'w', encoding='utf-8', newline='') as outf:
                    writer = csv.writer(outf)
                    
                    # 写入表头
                    writer.writerow(['startid', 'endid', 'properties'])
                    
                    # 处理每一行
                    for row in reader:
                        work_id = row.get('work_id', '').strip()
                        referenced_work_id = row.get('referenced_work_id', '').strip()
                        
                        total += 1
                        
                        # 跳过无效数据
                        if not work_id or not referenced_work_id:
                            skipped += 1
                            continue
                        
                        # 写入边数据
                        writer.writerow([
                            work_id,
                            referenced_work_id,
                            '{}'  # 空 JSON 对象
                        ])
                        
                        written += 1
                        
                        # 进度日志
                        if total % 1000000 == 0:
                            logger.info(f"  已处理 {total:,} 行，已写入 {written:,} 条边...")
        
        except FileNotFoundError:
            logger.error(f"输入文件不存在: {self.input_file}")
            return
        except Exception as e:
            logger.error(f"映射失败: {e}")
            return
        
        elapsed = datetime.now() - start_time
        
        logger.info(f"\n{'='*60}")
        logger.info(f"映射完成!")
        logger.info(f"  输入记录: {total:,}")
        logger.info(f"  输出边数: {written:,}")
        logger.info(f"  跳过记录: {skipped:,}")
        logger.info(f"  输出文件: {self.output_file}")
        logger.info(f"  耗时: {elapsed}")
        logger.info(f"{'='*60}")


def main():
    """主函数"""
    mapper = WorkReferencedWorksEdgeMapper()
    mapper.map_to_edge()


if __name__ == '__main__':
    main()
