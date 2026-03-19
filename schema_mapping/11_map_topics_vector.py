#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Topics 向量映射脚本
将 topics.csv 中的 keywords 通过 BERT 模型编码为 384 维向量，
输出 topics_vector.csv，字段: id, vec
"""

import csv
import json
from pathlib import Path
import logging
from datetime import datetime
from sentence_transformers import SentenceTransformer

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


class TopicsVectorMapper:
    """Topics 向量映射器"""

    def __init__(self, config_file="mapping_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)

        # 路径配置
        self.input_dir = Path(self.config.get('input_dir', '../openalex_merged'))
        self.output_base_dir = Path(self.config.get('vector_output_dir',
                                                     self.config.get('output_dir', '../graph_vertex')))

        # 向量模型路径
        self.bert_model_path = self.config.get(
            'bert_model_path',
            str(Path(__file__).resolve().parent.parent / 'all-MiniLM-L6-v2')
        )

        # 创建输出目录
        self.output_base_dir.mkdir(parents=True, exist_ok=True)

        logger.info("Topics 向量映射器初始化完成")
        logger.info(f"输入目录: {self.input_dir}")
        logger.info(f"输出目录: {self.output_base_dir}")
        logger.info(f"BERT 模型路径: {self.bert_model_path}")

    @staticmethod
    def load_config(config_file):
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"配置文件 {config_file} 不存在，使用默认配置")
            return {}

    def process_topics(self, batch_size=500):
        """处理 topics 文件并生成向量文件"""
        logger.info(f"\n{'='*60}")
        logger.info("开始映射 topics.csv -> topics_vector.csv")
        logger.info(f"{'='*60}")

        input_file = self.input_dir / 'topics.csv'
        topics_vector_config = self.config.get('topics_vector_mapping', {})
        output_filename = topics_vector_config.get('output_file', 'topics_vector.csv')
        output_file = self.output_base_dir / output_filename

        if not input_file.exists():
            logger.error(f"输入文件不存在: {input_file}")
            return False

        logger.info(f"读取: {input_file}")
        logger.info(f"输出: {output_file}")

        # 加载 BERT 模型
        logger.info(f"加载 BERT 模型: {self.bert_model_path}")
        model = SentenceTransformer(self.bert_model_path)
        logger.info("BERT 模型加载完成")

        total_count = 0
        error_count = 0
        start_time = datetime.now()

        try:
            with open(input_file, 'r', encoding='utf-8') as inf, \
                 open(output_file, 'w', encoding='utf-8', newline='') as outf:

                reader = csv.DictReader(inf)
                fieldnames = ['id', 'vec']
                writer = csv.DictWriter(outf, fieldnames=fieldnames)
                writer.writeheader()

                batch_rows = []

                for i, row in enumerate(reader, 1):
                    try:
                        topic_id = row.get('id', '').strip()
                        keywords = row.get('keywords', '').strip()

                        # 生成 384 维向量（不截断）
                        embedding = model.encode(keywords).tolist()

                        batch_rows.append({
                            'id': topic_id,
                            'vec': json.dumps(embedding, ensure_ascii=False)
                        })

                        if len(batch_rows) >= batch_size:
                            writer.writerows(batch_rows)
                            total_count += len(batch_rows)
                            batch_rows = []
                            elapsed = (datetime.now() - start_time).total_seconds()
                            logger.info(f"  已处理 {total_count:,} 行，耗时 {elapsed:.2f} 秒")

                    except Exception as e:
                        error_count += 1
                        if error_count <= 10:
                            logger.error(f"  处理第 {i} 行失败: {e}")

                # 写入剩余数据
                if batch_rows:
                    writer.writerows(batch_rows)
                    total_count += len(batch_rows)

        except Exception as e:
            logger.error(f"处理失败: {e}")
            return False

        elapsed = datetime.now() - start_time
        logger.info(f"\n{'='*60}")
        logger.info("✓ 映射完成:")
        logger.info(f"  总记录数: {total_count:,}")
        logger.info(f"  错误记录: {error_count:,}")
        if total_count > 0:
            logger.info(f"  成功率: {((total_count - error_count) / total_count * 100):.2f}%")
        logger.info(f"  耗时: {elapsed}")
        logger.info(f"  输出文件: {output_file}")

        if output_file.exists():
            file_size = output_file.stat().st_size / (1024 ** 2)
            logger.info(f"  文件大小: {file_size:.2f} MB")

        logger.info(f"{'='*60}")
        return True

    def run(self):
        """运行映射流程"""
        logger.info(f"\n{'#'*80}")
        logger.info("Topics 向量映射工具")
        logger.info(f"{'#'*80}\n")

        success = self.process_topics()

        if success:
            logger.info("\n✓ Topics 向量映射完成！")
        else:
            logger.error("\n✗ Topics 向量映射失败")

        return success


def main():
    """主函数"""
    mapper = TopicsVectorMapper()
    mapper.run()


if __name__ == '__main__':
    main()
