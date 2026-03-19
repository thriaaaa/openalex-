#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Works 向量映射脚本（单 GPU 加速版）
将 works.csv 中的 title + abstract 通过 BERT 模型编码为向量，
输出 works_vector.csv，字段: work_id, doi, vec
"""

import os
import csv
import json
from pathlib import Path
import logging
from datetime import datetime
import torch
from sentence_transformers import SentenceTransformer

# ✅ 使用单个 GPU
os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"
os.environ["CUDA_VISIBLE_DEVICES"] = "0"

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

logger.info(f"PyTorch 发现的 GPU 数量: {torch.cuda.device_count()}")
if torch.cuda.device_count() > 0:
    logger.info(f"使用 GPU: {torch.cuda.get_device_name(0)}")
    torch.cuda.set_device(0)
else:
    logger.warning("没有检测到可用的 GPU，将使用 CPU 运行")


def set_gpu_memory_limit(memory_limit_gb=15):
    """设置 GPU 内存限制"""
    try:
        total_memory = torch.cuda.get_device_properties(0).total_memory
        memory_fraction = min(1.0, (memory_limit_gb * 1024 ** 3) / total_memory)
        torch.cuda.set_per_process_memory_fraction(memory_fraction)
        logger.info(f"GPU 内存限制设置为 {memory_limit_gb}GB (比例: {memory_fraction:.2f})")
    except Exception as e:
        logger.warning(f"无法设置 GPU 内存限制: {e}")


def reconstruct_abstract(abstract_inverted_index_str):
    """根据 inverted index 还原摘要"""
    if not abstract_inverted_index_str or abstract_inverted_index_str.strip() == "":
        return None
    try:
        abstract_inverted_index = json.loads(abstract_inverted_index_str)
    except json.JSONDecodeError:
        logger.warning(f"无法解析 JSON 字符串: {abstract_inverted_index_str[:80]}")
        return None

    abstract_index = {}
    for word, positions in abstract_inverted_index.items():
        for position in positions:
            abstract_index[position] = word

    return ' '.join(abstract_index[pos] for pos in sorted(abstract_index.keys()))


class WorksVectorMapper:
    """Works 向量映射器（单 GPU 加速版）"""

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

        logger.info("Works 向量映射器初始化完成")
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

    def load_model(self):
        """加载模型到 GPU（失败时回退到 CPU）"""
        try:
            set_gpu_memory_limit(15)
            logger.info("正在加载模型到 GPU...")
            model = SentenceTransformer(self.bert_model_path, device='cuda')
            logger.info("模型成功加载到 GPU")
            return model
        except Exception as e:
            logger.warning(f"GPU 加载失败: {e}，尝试 CPU 模式...")
            try:
                model = SentenceTransformer(self.bert_model_path, device='cpu')
                logger.info("模型加载到 CPU")
                return model
            except Exception as e2:
                logger.error(f"CPU 加载也失败: {e2}")
                return None

    def process_batch(self, batch, model, writer):
        """处理一个批次的数据"""
        try:
            texts = [item["text"] for item in batch]
            embeddings = model.encode(
                texts,
                convert_to_tensor=False,
                show_progress_bar=False,
                batch_size=min(32, len(texts))
            )
            for i, item in enumerate(batch):
                embedding = embeddings[i]
                if hasattr(embedding, 'tolist'):
                    embedding = embedding.tolist()
                elif hasattr(embedding, 'cpu'):
                    embedding = embedding.cpu().numpy().tolist()
                writer.writerow({
                    'work_id': item['work_id'],
                    'doi': item['doi'],
                    'vec': json.dumps(embedding, ensure_ascii=False)
                })
        except Exception as e:
            logger.error(f"批处理出错: {e}，降级为逐条处理")
            for item in batch:
                try:
                    embedding = model.encode(item["text"], convert_to_tensor=False,
                                             show_progress_bar=False)
                    if hasattr(embedding, 'tolist'):
                        embedding = embedding.tolist()
                    elif hasattr(embedding, 'cpu'):
                        embedding = embedding.cpu().numpy().tolist()
                    writer.writerow({
                        'work_id': item['work_id'],
                        'doi': item['doi'],
                        'vec': json.dumps(embedding, ensure_ascii=False)
                    })
                except Exception as e2:
                    logger.error(f"单条处理也失败: {e2}")

    def process_works(self, batch_size=64):
        """处理 works 文件并生成向量文件"""
        logger.info(f"\n{'='*60}")
        logger.info("开始映射 works.csv -> works_vector.csv")
        logger.info(f"{'='*60}")

        input_file = self.input_dir / 'works.csv'
        works_vector_config = self.config.get('works_vector_mapping', {})
        output_filename = works_vector_config.get('output_file', 'works_vector.csv')
        output_file = self.output_base_dir / output_filename

        if not input_file.exists():
            logger.error(f"输入文件不存在: {input_file}")
            return False

        logger.info(f"读取: {input_file}")
        logger.info(f"输出: {output_file}")

        # 加载模型
        model = self.load_model()
        if model is None:
            logger.error("模型加载失败，退出")
            return False

        total_count = 0
        error_count = 0
        start_time = datetime.now()

        try:
            with open(input_file, 'r', encoding='utf-8') as inf, \
                 open(output_file, 'w', encoding='utf-8', newline='') as outf:

                reader = csv.DictReader(inf)
                fieldnames = ['work_id', 'doi', 'vec']
                writer = csv.DictWriter(outf, fieldnames=fieldnames)
                writer.writeheader()

                batch = []

                for i, row in enumerate(reader, 1):
                    try:
                        work_id = row.get('id', '').strip()
                        doi = row.get('doi', '').strip()
                        title = row.get('title', '').strip()
                        abstract = reconstruct_abstract(
                            row.get('abstract_inverted_index', '')
                        ) or ''
                        combined_text = f"{title} {abstract}".strip()

                        batch.append({
                            'work_id': work_id,
                            'doi': doi,
                            'text': combined_text
                        })

                        if len(batch) >= batch_size:
                            self.process_batch(batch, model, writer)
                            total_count += len(batch)
                            batch = []

                            # 定期清理 GPU 缓存并报告进度
                            if total_count % 1000 == 0:
                                if torch.cuda.is_available():
                                    torch.cuda.empty_cache()
                                    gpu_memory = torch.cuda.memory_allocated() / 1024 ** 3
                                    elapsed = (datetime.now() - start_time).total_seconds()
                                    logger.info(
                                        f"  已处理 {total_count:,} 行，耗时 {elapsed:.2f} 秒，"
                                        f"GPU 内存使用: {gpu_memory:.2f}GB"
                                    )
                                else:
                                    elapsed = (datetime.now() - start_time).total_seconds()
                                    logger.info(f"  已处理 {total_count:,} 行，耗时 {elapsed:.2f} 秒")

                    except Exception as e:
                        error_count += 1
                        if error_count <= 10:
                            logger.error(f"  处理第 {i} 行失败: {e}")

                # 处理剩余批次
                if batch:
                    self.process_batch(batch, model, writer)
                    total_count += len(batch)

        except Exception as e:
            logger.error(f"处理失败: {e}")
            return False
        finally:
            del model
            if torch.cuda.is_available():
                torch.cuda.empty_cache()

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
        logger.info("Works 向量映射工具（单 GPU 加速版）")
        logger.info(f"{'#'*80}\n")

        success = self.process_works()

        if success:
            logger.info("\n✓ Works 向量映射完成！")
        else:
            logger.error("\n✗ Works 向量映射失败")

        return success


def main():
    """主函数"""
    mapper = WorksVectorMapper()
    mapper.run()


if __name__ == '__main__':
    main()
