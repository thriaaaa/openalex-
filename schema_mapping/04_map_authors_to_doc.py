#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Authors Doc 模式映射脚本
将 authors.csv 映射为文档模型格式 author_doc.csv
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


class AuthorsDocMapper:
    """Authors 到文档模型格式的映射器"""
    
    def __init__(self, config_file="mapping_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)
        
        # 路径配置
        self.input_dir = Path(self.config.get('input_dir', '../openalex_merged'))
        self.output_base_dir = Path(self.config.get('doc_output_dir', '../doc'))
        
        # 创建输出目录
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Authors文档模型映射器初始化完成")
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
    
    def map_author_to_doc(self, author_row):
        """将author行映射为文档模型格式
        
        Args:
            author_row: 原始author CSV行（字典）
            
        Returns:
            dict: 文档模型格式 {'id': ..., 'doc': ...}
        """
        # 构建doc JSON对象
        doc = {}
        
        # orcid - 可选字段，只在有值时添加
        orcid = author_row.get('orcid', '').strip()
        if orcid:
            doc['orcid'] = orcid
        
        # display_name_alternatives - 字符串数组
        # 假设原始数据用分号或其他分隔符分隔，需要解析为数组
        alternatives_str = author_row.get('display_name_alternatives', '').strip()
        if alternatives_str:
            # 尝试解析JSON数组格式
            try:
                # 如果已经是JSON格式
                alternatives = json.loads(alternatives_str)
            except:
                # 如果是分号分隔的字符串
                if ';' in alternatives_str:
                    alternatives = [alt.strip() for alt in alternatives_str.split(';') if alt.strip()]
                elif ',' in alternatives_str:
                    alternatives = [alt.strip() for alt in alternatives_str.split(',') if alt.strip()]
                else:
                    alternatives = [alternatives_str]
            
            doc['display_name_alternatives'] = alternatives
        else:
            doc['display_name_alternatives'] = []
        
        return {
            'id': author_row.get('id', ''),
            'doc': json.dumps(doc, ensure_ascii=False)
        }
    
    def process_authors(self, batch_size=100000):
        """处理authors文件并生成文档模型文件"""
        logger.info(f"\n{'='*60}")
        logger.info(f"开始映射 authors.csv -> author_doc.csv")
        logger.info(f"{'='*60}")
        
        input_file = self.input_dir / 'authors.csv'
        output_file = self.output_base_dir / 'author_doc.csv'
        
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
                    # 文档模型格式的列
                    fieldnames = ['id', 'doc']
                    writer = csv.DictWriter(outf, fieldnames=fieldnames)
                    writer.writeheader()
                    
                    batch = []
                    
                    for i, row in enumerate(reader, 1):
                        try:
                            # 映射为文档模型格式
                            doc_row = self.map_author_to_doc(row)
                            batch.append(doc_row)
                            
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
        logger.info(f"Authors 文档模型映射工具")
        logger.info(f"{'#'*80}\n")
        
        success = self.process_authors()
        
        if success:
            logger.info(f"\n✓ Authors 文档模型映射完成！")
        else:
            logger.error(f"\n✗ Authors 文档模型映射失败")
        
        return success


def main():
    """主函数"""
    mapper = AuthorsDocMapper()
    mapper.run()


if __name__ == '__main__':
    main()
