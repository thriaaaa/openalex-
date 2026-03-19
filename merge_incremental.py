#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlex 增量数据去重合并工具（多线程优化版）
解压时直接转换ID为纯数字格式
支持多线程加速和断点续传
"""

import os
import gzip
import csv
import json
from pathlib import Path
import logging
from datetime import datetime
import shutil
import sqlite3
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('merge_incremental.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OptimizedIncrementalMerger:
    """优化的增量数据合并器（多线程版）"""
    
    def __init__(self, config_file="merge_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)
        
        # 路径配置
        self.incremental_dir = Path(self.config['incremental_dir'])
        self.full_data_dir = Path(self.config['full_data_dir'])
        self.output_dir = Path(self.config['output_dir'])
        self.temp_dir = Path(self.config.get('temp_dir', './temp_decompressed'))
        
        # 特殊文件路径配置（针对某些文件全量数据路径不同的情况）
        self.special_file_paths = self.config.get('special_file_paths', {})
        
        # 列名映射配置（针对增量数据和全量数据列名不一致的情况）
        # 格式: {"文件名": {"增量列名": "全量列名"}}
        self.column_mappings = self.config.get('column_mappings', {})
        
        # 处理配置
        self.files_to_process = self.config.get('files_to_process', [])
        
        # 性能配置
        self.max_workers = self.config.get('max_workers', 4)  # 默认4线程
        self.skip_existing = self.config.get('skip_existing', True)  # 跳过已存在的文件
        
        # 创建目录
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 线程锁（用于线程安全的日志输出）
        self.lock = threading.Lock()
        
        logger.info(f"优化合并器初始化完成")
        logger.info(f"最大线程数: {self.max_workers}")
        logger.info(f"跳过已存在文件: {self.skip_existing}")
    
    @staticmethod
    def load_config(config_file):
        """加载配置文件"""
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    def normalize_id(self, id_value):
        """标准化ID：从URL格式提取纯数字ID
        
        例如: https://openalex.org/A5028767461 -> 5028767461
        """
        if not id_value:
            return ''
        
        # 如果是URL格式，提取最后的数字部分
        if id_value.startswith('http'):
            parts = id_value.split('/')
            if parts:
                last_part = parts[-1]
                # 去掉首字母（A, I, W, T等）
                if len(last_part) > 1 and last_part[0].isalpha():
                    return last_part[1:]
        
        return id_value
    
    def normalize_row_ids(self, row):
        """标准化CSV行中的所有ID字段"""
        id_fields = ['id', 'author_id', 'institution_id', 'work_id', 'topic_id', 
                     'referenced_work_id', 'related_work_id', 'source_id',
                     'concept_id', 'publisher_id', 'last_known_institution']
        
        for field in id_fields:
            if field in row and row[field]:
                row[field] = self.normalize_id(row[field])
        
        return row
    
    def decompress_gz_to_csv(self, file_pattern):
        """解压 .gz 文件并转换ID为纯数字格式（支持断点续传和多线程）"""
        logger.info(f"\n{'='*60}")
        logger.info(f"解压并转换ID: {file_pattern}...")
        logger.info(f"{'='*60}")
        
        output_file = self.temp_dir / f"{file_pattern}.csv"
        
        # 检查是否已解压（断点续传）
        if self.skip_existing and output_file.exists():
            # 检查文件大小，确保不是空文件
            file_size = output_file.stat().st_size
            if file_size > 1000:  # 至少1KB
                logger.info(f"✓ 文件已存在，跳过解压: {output_file}")
                logger.info(f"  文件大小: {file_size / (1024**3):.2f} GB")
                return output_file
            else:
                logger.warning(f"文件太小，可能不完整，重新解压: {output_file}")
                output_file.unlink()
        
        gz_files = list(self.incremental_dir.glob(f"**/{file_pattern}.csv.gz"))
        
        if not gz_files:
            logger.warning(f"未找到 {file_pattern}.csv.gz")
            return None
        
        logger.info(f"找到 {len(gz_files)} 个文件")
        logger.info(f"使用 {min(self.max_workers, len(gz_files))} 个线程并行解压...")
        
        # 多线程解压
        def process_gz_file(gz_file):
            """处理单个.gz文件"""
            temp_output = self.temp_dir / f"{gz_file.stem}_{gz_file.parent.name}.csv"
            file_lines = 0
            
            try:
                with gzip.open(gz_file, 'rt', encoding='utf-8') as inf:
                    reader = csv.DictReader(inf)
                    
                    with open(temp_output, 'w', encoding='utf-8', newline='') as outf:
                        writer = csv.DictWriter(outf, fieldnames=reader.fieldnames)
                        writer.writeheader()
                        
                        for i, row in enumerate(reader, 1):
                            row = self.normalize_row_ids(row)
                            writer.writerow(row)
                            file_lines += 1
                            
                            if i % 500000 == 0:
                                with self.lock:
                                    logger.info(f"    [{gz_file.name}] 已处理 {i:,} 行...")
                
                with self.lock:
                    logger.info(f"  ✓ {gz_file.name}: {file_lines:,} 行")
                
                return temp_output, file_lines, reader.fieldnames
            
            except Exception as e:
                with self.lock:
                    logger.error(f"  ✗ 处理 {gz_file.name} 失败: {e}")
                return None, 0, None
        
        # 并行处理所有.gz文件
        temp_files = []
        total_lines = 0
        fieldnames = None
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {executor.submit(process_gz_file, gz_file): gz_file for gz_file in gz_files}
            
            for future in as_completed(futures):
                temp_file, lines, fields = future.result()
                if temp_file:
                    temp_files.append(temp_file)
                    total_lines += lines
                    if fieldnames is None:
                        fieldnames = fields
        
        # 合并所有临时文件
        logger.info(f"\n合并 {len(temp_files)} 个临时文件...")
        with open(output_file, 'w', encoding='utf-8', newline='') as outf:
            writer = csv.DictWriter(outf, fieldnames=fieldnames)
            writer.writeheader()
            
            for temp_file in temp_files:
                with open(temp_file, 'r', encoding='utf-8') as inf:
                    reader = csv.DictReader(inf)
                    for row in reader:
                        writer.writerow(row)
                
                # 删除临时文件
                temp_file.unlink()
        
        logger.info(f"✓ 解压完成，共 {total_lines:,} 行，所有ID已转换为纯数字")
        logger.info(f"  输出文件: {output_file}")
        logger.info(f"  文件大小: {output_file.stat().st_size / (1024**3):.2f} GB")
        return output_file
    
    def get_key_columns(self, filename):
        """获取文件的主键列"""
        key_mapping = {
            'authors.csv': ('id',),
            'institutions.csv': ('id',),
            'institutions_geo.csv': ('institution_id',),
            'institutions_ids.csv': ('institution_id',),
            'topics.csv': ('id',),
            'works.csv': ('id',),
            'works_topics.csv': ('work_id', 'topic_id'),
            'works_authorships.csv': ('work_id', 'author_id'),
            'works_referenced_works.csv': ('work_id', 'referenced_work_id'),
            'works_biblio.csv': ('work_id',),
        }
        return key_mapping.get(filename, ('id',))
    
    def get_shard_id(self, key_value, num_shards=100):
        """计算分片ID（使用hash）"""
        if isinstance(key_value, tuple):
            # 组合键：连接后hash
            key_str = '|'.join(str(v) for v in key_value)
        else:
            key_str = str(key_value)
        
        # 使用MD5 hash的前8位转整数，然后取模
        hash_val = int(hashlib.md5(key_str.encode()).hexdigest()[:8], 16)
        return hash_val % num_shards
    
    def build_shards_from_full_data(self, csv_file, key_columns, filename, num_shards=100):
        """阶段1: 将全量数据分片存储
        
        Args:
            csv_file: CSV文件路径
            key_columns: 键列名
            filename: 文件名（用于列名映射）
            num_shards: 分片数量
            
        Returns:
            分片目录路径
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"阶段1: 分片全量数据")
        logger.info(f"{'='*60}")
        logger.info(f"  文件: {csv_file.name}")
        logger.info(f"  分片数量: {num_shards}")
        
        # 获取列名映射
        column_mapping = {}
        if filename and filename in self.column_mappings:
            column_mapping = self.column_mappings[filename]
            logger.info(f"  使用列名映射: {column_mapping}")
        
        # 创建分片目录
        shard_dir = self.temp_dir / f"shards_{csv_file.stem}"
        
        # 检查是否已存在（断点续传）
        if self.skip_existing and shard_dir.exists():
            existing_shards = list(shard_dir.glob("shard_*.txt"))
            if len(existing_shards) == num_shards:
                logger.info(f"  ✓ 分片已存在，跳过分片过程: {len(existing_shards)} 个分片")
                return shard_dir
            else:
                logger.warning(f"  分片不完整（{len(existing_shards)}/{num_shards}），重新分片")
                shutil.rmtree(shard_dir)
        
        shard_dir.mkdir(parents=True, exist_ok=True)
        
        # 创建分片文件句柄
        shard_files = {}
        for i in range(num_shards):
            shard_path = shard_dir / f"shard_{i:03d}.txt"
            shard_files[i] = open(shard_path, 'w', encoding='utf-8')
        
        logger.info(f"  开始读取并分片...")
        
        try:
            total_count = 0
            shard_counts = {i: 0 for i in range(num_shards)}
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for i, row in enumerate(reader, 1):
                    # 构建key
                    if len(key_columns) == 1:
                        col_name = column_mapping.get(key_columns[0], key_columns[0])
                        key = row.get(col_name, '')
                    else:
                        key_values = tuple(
                            row.get(column_mapping.get(col, col), '') 
                            for col in key_columns
                        )
                        key = key_values
                    
                    # 计算分片ID
                    shard_id = self.get_shard_id(key, num_shards)
                    
                    # 写入分片文件（只存储key）
                    if isinstance(key, tuple):
                        shard_files[shard_id].write('|'.join(key) + '\n')
                    else:
                        shard_files[shard_id].write(key + '\n')
                    
                    shard_counts[shard_id] += 1
                    total_count += 1
                    
                    # 每500万行显示进度
                    if i % 5000000 == 0:
                        logger.info(f"    已处理 {i:,} 行...")
            
            logger.info(f"  ✓ 分片完成: {total_count:,} 行")
            logger.info(f"  分片大小分布:")
            for shard_id in range(min(5, num_shards)):  # 只显示前5个
                logger.info(f"    shard_{shard_id:03d}: {shard_counts[shard_id]:,} 行")
            if num_shards > 5:
                logger.info(f"    ... (共{num_shards}个分片)")
        
        finally:
            # 关闭所有分片文件
            for f in shard_files.values():
                f.close()
        
        return shard_dir
    
    def load_single_shard(self, shard_path):
        """加载单个分片到set（供多线程调用）"""
        shard_set = set()
        try:
            with open(shard_path, 'r', encoding='utf-8') as f:
                for line in f:
                    shard_set.add(line.strip())
            return shard_path.name, shard_set
        except Exception as e:
            logger.error(f"  加载分片失败 {shard_path.name}: {e}")
            return shard_path.name, set()
    
    def load_shards_parallel(self, shard_dir, num_shards=100, max_workers=20):
        """阶段2: 并行加载所有分片到内存
        
        Args:
            shard_dir: 分片目录
            num_shards: 分片数量
            max_workers: 最大线程数
            
        Returns:
            dict: {shard_id: set of IDs}
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"阶段2: 并行加载分片到内存")
        logger.info(f"{'='*60}")
        logger.info(f"  分片数量: {num_shards}")
        logger.info(f"  并行线程: {max_workers}")
        
        shards = {}
        load_start = datetime.now()
        
        # 收集所有分片文件
        shard_files = sorted(shard_dir.glob("shard_*.txt"))
        
        if len(shard_files) != num_shards:
            logger.error(f"  分片数量不匹配: 期望{num_shards}, 实际{len(shard_files)}")
            return {}
        
        logger.info(f"  开始并行加载...")
        
        # 使用线程池并行加载
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有加载任务
            futures = {executor.submit(self.load_single_shard, shard_path): i 
                      for i, shard_path in enumerate(shard_files)}
            
            completed = 0
            for future in as_completed(futures):
                shard_name, shard_set = future.result()
                shard_id = int(shard_name.split('_')[1].split('.')[0])
                shards[shard_id] = shard_set
                
                completed += 1
                if completed % 10 == 0 or completed == num_shards:
                    logger.info(f"    已加载 {completed}/{num_shards} 个分片...")
        
        load_elapsed = datetime.now() - load_start
        
        # 统计信息
        total_ids = sum(len(s) for s in shards.values())
        logger.info(f"  ✓ 加载完成，耗时: {load_elapsed}")
        logger.info(f"  总ID数量: {total_ids:,}")
        logger.info(f"  平均每分片: {total_ids//num_shards:,}")
        
        return shards
    
    def merge_with_dedup_sharding(self, filename, num_shards=100):
        """使用分片策略去重并输出新记录（流式处理版：不预加载数据）
        
        Args:
            filename: 文件名
            num_shards: 分片数量
        """
        logger.info(f"\n{'#'*60}")
        logger.info(f"去重(分片策略-流式处理): {filename}")
        logger.info(f"{'#'*60}")
        
        incremental_file = self.temp_dir / filename
        
        # 检查是否有特殊路径配置
        if filename in self.special_file_paths:
            full_file = Path(self.special_file_paths[filename])
            logger.info(f"  使用特殊路径: {full_file}")
        else:
            full_file = self.full_data_dir / filename
        
        output_file = self.output_dir / filename
        
        # 检查输出文件是否已存在（断点续传）
        if self.skip_existing and output_file.exists():
            file_size = output_file.stat().st_size
            if file_size > 1000:  # 至少1KB
                logger.info(f"✓ 输出文件已存在，跳过去重: {output_file}")
                logger.info(f"  文件大小: {file_size / (1024**3):.2f} GB")
                return True
            else:
                logger.warning(f"输出文件太小，可能不完整，重新处理")
                output_file.unlink()
        
        if not incremental_file.exists():
            logger.warning(f"增量文件不存在，跳过")
            return False
        
        if not full_file.exists():
            logger.warning(f"全量文件不存在: {full_file}")
            logger.warning(f"直接复制增量数据")
            shutil.copy(incremental_file, output_file)
            return True
        
        key_columns = self.get_key_columns(filename)
        
        # 阶段1: 分片全量数据
        shard_dir = self.build_shards_from_full_data(full_file, key_columns, filename, num_shards)
        
        # 阶段2: 流式处理 - 多次扫描增量数据,每次只处理一批分片
        logger.info(f"\n{'='*60}")
        logger.info(f"阶段2: 流式去重（分批处理,避免内存溢出）")
        logger.info(f"{'='*60}")
        logger.info(f"  分片总数: {num_shards}")
        
        # 每次处理的分片数量（根据内存调整）
        batch_size = self.config.get('shard_batch_size', 20)
        logger.info(f"  每批处理: {batch_size} 个分片")
        logger.info(f"  预计扫描次数: {(num_shards + batch_size - 1) // batch_size}")
        
        column_mapping = {}
        if filename in self.column_mappings:
            column_mapping = self.column_mappings[filename]
        
        total_processed = 0
        total_duplicates = 0
        dedup_start = datetime.now()
        
        # 创建输出文件
        with open(output_file, 'w', encoding='utf-8', newline='') as outf:
            writer = None
            
            # 分批处理分片
            for batch_num, batch_start in enumerate(range(0, num_shards, batch_size)):
                batch_end = min(batch_start + batch_size, num_shards)
                logger.info(f"\n  【批次 {batch_num + 1}】处理分片 {batch_start}-{batch_end-1} ...")
                
                # 加载这批分片到内存
                logger.info(f"    加载分片到内存...")
                batch_shards = {}
                for shard_id in range(batch_start, batch_end):
                    shard_file = shard_dir / f"shard_{shard_id:03d}.txt"
                    shard_set = set()
                    with open(shard_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            shard_set.add(line.strip())
                    batch_shards[shard_id] = shard_set
                
                logger.info(f"    ✓ 已加载 {len(batch_shards)} 个分片")
                
                # 扫描增量数据,只处理属于当前批次分片的记录
                logger.info(f"    扫描增量数据并去重...")
                new_records = []
                batch_processed = 0
                batch_duplicates = 0
                chunk_size = 100000
                
                with open(incremental_file, 'r', encoding='utf-8') as inf:
                    reader = csv.DictReader(inf)
                    
                    if writer is None:
                        fieldnames = reader.fieldnames
                        writer = csv.DictWriter(outf, fieldnames=fieldnames)
                        writer.writeheader()
                    
                    for i, row in enumerate(reader, 1):
                        # 构建key
                        if len(key_columns) == 1:
                            col_name = key_columns[0]
                            key = row.get(col_name, '')
                            key_str = key
                        else:
                            key_values = tuple(row.get(col, '') for col in key_columns)
                            key = key_values
                            key_str = '|'.join(key_values)
                        
                        # 计算分片ID
                        shard_id = self.get_shard_id(key, num_shards)
                        
                        # 只处理属于当前批次的记录
                        if shard_id in batch_shards:
                            batch_processed += 1
                            
                            # 检查是否重复
                            if key_str not in batch_shards[shard_id]:
                                new_records.append(row)
                                
                                # 批量写入
                                if len(new_records) >= chunk_size:
                                    writer.writerows(new_records)
                                    new_records = []
                            else:
                                batch_duplicates += 1
                        
                        # 每1000万行显示进度
                        if i % 10000000 == 0:
                            logger.info(f"      已扫描 {i:,} 行...")
                
                # 写入剩余数据
                if new_records:
                    writer.writerows(new_records)
                
                total_processed += batch_processed
                total_duplicates += batch_duplicates
                
                logger.info(f"    ✓ 批次完成: 处理 {batch_processed:,} 行, 新增 {batch_processed - batch_duplicates:,}, 重复 {batch_duplicates:,}")
                
                # 释放内存
                batch_shards.clear()
        
        dedup_elapsed = datetime.now() - dedup_start
        new_count = total_processed - total_duplicates
        
        logger.info(f"\n{'='*60}")
        logger.info(f"✓ 去重完成:")
        logger.info(f"  总处理数: {total_processed:,}")
        logger.info(f"  新记录数: {new_count:,}")
        logger.info(f"  重复记录: {total_duplicates:,}")
        logger.info(f"  去重耗时: {dedup_elapsed}")
        logger.info(f"  输出文件: {output_file}")
        logger.info(f"  文件大小: {output_file.stat().st_size / (1024**3):.2f} GB")
        logger.info(f"{'='*60}")
        
        # 清理分片文件（可选）
        if not self.config.get('keep_shards', False):
            logger.info(f"  清理分片文件...")
            shutil.rmtree(shard_dir)
        
        return True
    
    def build_id_index_chunked(self, csv_file, key_columns, filename=None):
        """分块构建 ID 索引（使用SQLite存储，避免内存溢出，高性能优化）
        
        Args:
            csv_file: CSV文件路径
            key_columns: 键列名（增量数据的列名）
            filename: 文件名，用于查找列名映射
        """
        logger.info(f"  构建索引: {csv_file.name} (列: {key_columns})")
        
        # 获取列名映射（如果有）
        column_mapping = {}
        if filename and filename in self.column_mappings:
            column_mapping = self.column_mappings[filename]
            logger.info(f"  使用列名映射: {column_mapping}")
        
        # 使用SQLite数据库来存储ID索引（避免内存溢出）
        db_path = self.temp_dir / f"index_{csv_file.stem}.db"
        
        # 检查索引是否已存在（断点续传）
        if self.skip_existing and db_path.exists():
            # 验证数据库完整性
            try:
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM id_index')
                count = cursor.fetchone()[0]
                conn.close()
                
                if count > 0:
                    logger.info(f"  ✓ 索引已存在，跳过构建: {count:,} 条记录")
                    logger.info(f"  数据库大小: {db_path.stat().st_size / (1024**3):.2f} GB")
                    return db_path
            except:
                logger.warning(f"  索引数据库损坏，重新构建")
                db_path.unlink()
        else:
            # 删除旧索引
            if db_path.exists():
                db_path.unlink()
        
        logger.info(f"  ⚡ 启用高性能模式构建索引...")
        
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # ==================== 性能优化设置 ====================
        # 1. WAL模式 - 允许并发读写，大幅提升性能
        cursor.execute('PRAGMA journal_mode=WAL')
        
        # 2. 禁用同步 - 牺牲安全性换取速度（进程崩溃可能损坏数据库）
        cursor.execute('PRAGMA synchronous=OFF')
        
        # 3. 增大缓存 - 使用2GB内存缓存
        cursor.execute('PRAGMA cache_size=-2000000')  # 负数表示KB
        
        # 4. 内存临时存储
        cursor.execute('PRAGMA temp_store=MEMORY')
        
        # 5. 延迟写入
        cursor.execute('PRAGMA locking_mode=EXCLUSIVE')
        
        logger.info(f"  ✓ 性能优化已启用: WAL模式, 2GB缓存, 禁用同步")
        # =====================================================
        
        # 创建表（不创建主键，后续再创建索引）
        cursor.execute('''
            CREATE TABLE id_index (
                id_hash TEXT
            )
        ''')
        
        # 超大批量提交 - 每50万条提交一次
        chunk_size = 500000
        total_count = 0
        insert_start = datetime.now()  # 记录插入开始时间
        
        logger.info(f"  开始批量插入数据（批量大小: {chunk_size:,}）...")
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                batch = []
                last_progress_time = datetime.now()
                
                for i, row in enumerate(reader, 1):
                    # 每10万行显示一次"正在读取"进度(避免看起来卡死)
                    if i % 100000 == 0:
                        current_time = datetime.now()
                        if (current_time - last_progress_time).total_seconds() > 10:  # 至少10秒显示一次
                            logger.info(f"    正在读取CSV... 已读取 {i:,} 行")
                            last_progress_time = current_time
                    
                    if len(key_columns) == 1:
                        # 单列键
                        col_name = column_mapping.get(key_columns[0], key_columns[0])
                        key = row.get(col_name, '')
                        id_hash = key  # 单列直接使用值
                    else:
                        # 组合键：连接后计算hash
                        key_values = tuple(
                            row.get(column_mapping.get(col, col), '') 
                            for col in key_columns
                        )
                        # 使用分隔符连接，避免碰撞
                        id_hash = '|'.join(key_values)
                    
                    batch.append((id_hash,))
                    
                    if len(batch) >= chunk_size:
                        cursor.executemany('INSERT INTO id_index VALUES (?)', batch)
                        conn.commit()
                        total_count += len(batch)
                        
                        # 每次批量提交后显示进度
                        elapsed_seconds = (datetime.now() - insert_start).total_seconds()
                        if elapsed_seconds > 0:
                            speed = (i / elapsed_seconds) * 60  # 行/分钟
                        else:
                            speed = 0
                        db_size_mb = db_path.stat().st_size / (1024**2)
                        logger.info(f"    已索引 {i:,} 行 | 速度: {speed:,.0f} 行/分钟 | 数据库: {db_size_mb:.0f} MB")
                        
                        batch = []
                
                # 处理剩余数据
                if batch:
                    cursor.executemany('INSERT INTO id_index VALUES (?)', batch)
                    conn.commit()
                    total_count += len(batch)
        
        except Exception as e:
            logger.error(f"构建索引失败: {e}")
            conn.close()
            return None
        
        logger.info(f"  数据插入完成,开始创建普通索引(加速查询,不去重)...")
        logger.info(f"  ⚠️  索引创建中,预计需要5-10分钟...")
        
        # 创建普通索引(不去重,速度快很多)
        index_start = datetime.now()
        cursor.execute('CREATE INDEX idx_hash ON id_index(id_hash)')
        conn.commit()
        index_elapsed = datetime.now() - index_start
        logger.info(f"  ✓ 索引创建完成,耗时: {index_elapsed}")
        
        # 获取记录数(包含重复)
        cursor.execute('SELECT COUNT(*) FROM id_index')
        total_count = cursor.fetchone()[0]
        
        # 获取唯一ID数量(通过DISTINCT查询)
        logger.info(f"  正在统计唯一ID数量...")
        cursor.execute('SELECT COUNT(DISTINCT id_hash) FROM id_index')
        unique_count = cursor.fetchone()[0]
        duplicate_count = total_count - unique_count
        
        logger.info(f"  ✓ 索引完成:")
        logger.info(f"    总记录数: {total_count:,}")
        logger.info(f"    唯一记录: {unique_count:,}")
        logger.info(f"    重复记录: {duplicate_count:,}")
        logger.info(f"  数据库大小: {db_path.stat().st_size / (1024**3):.2f} GB")
        
        conn.close()
        return db_path  # 返回数据库路径而不是set
    
    def merge_with_dedup(self, filename):
        """去重并输出新记录（支持断点续传，高性能优化）"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"去重: {filename}")
        logger.info(f"{'#'*60}")
        
        incremental_file = self.temp_dir / filename
        
        # 检查是否有特殊路径配置
        if filename in self.special_file_paths:
            full_file = Path(self.special_file_paths[filename])
            logger.info(f"  使用特殊路径: {full_file}")
        else:
            full_file = self.full_data_dir / filename
        
        output_file = self.output_dir / filename
        
        # 检查输出文件是否已存在（断点续传）
        if self.skip_existing and output_file.exists():
            file_size = output_file.stat().st_size
            if file_size > 1000:  # 至少1KB
                logger.info(f"✓ 输出文件已存在，跳过去重: {output_file}")
                logger.info(f"  文件大小: {file_size / (1024**3):.2f} GB")
                return True
            else:
                logger.warning(f"输出文件太小，可能不完整，重新处理")
                output_file.unlink()
        
        if not incremental_file.exists():
            logger.warning(f"增量文件不存在，跳过")
            return False
        
        if not full_file.exists():
            logger.warning(f"全量文件不存在: {full_file}")
            logger.warning(f"直接复制增量数据")
            shutil.copy(incremental_file, output_file)
            return True
        
        key_columns = self.get_key_columns(filename)
        
        logger.info("步骤 1/2: 构建全量数据索引...")
        db_path = self.build_id_index_chunked(full_file, key_columns, filename)
        
        if db_path is None:
            logger.error("索引构建失败")
            return False
        
        logger.info("步骤 2/2: 过滤重复数据，输出新记录...")
        logger.info("  ⚡ 启用高性能查询模式...")
        
        # 连接到索引数据库
        conn = sqlite3.connect(str(db_path))
        
        # 性能优化 - 只读模式
        conn.execute('PRAGMA query_only=ON')
        conn.execute('PRAGMA cache_size=-1000000')  # 1GB查询缓存
        
        cursor = conn.cursor()
        
        new_records = []
        duplicate_count = 0
        chunk_size = 100000  # 增大批量写入
        query_batch_size = 10000  # 批量查询
        
        logger.info(f"  ✓ 查询优化已启用: 1GB缓存, 批量查询({query_batch_size:,})")
        
        with open(incremental_file, 'r', encoding='utf-8') as inf:
            reader = csv.DictReader(inf)
            fieldnames = reader.fieldnames
            
            with open(output_file, 'w', encoding='utf-8', newline='') as outf:
                writer = csv.DictWriter(outf, fieldnames=fieldnames)
                writer.writeheader()
                
                query_batch = []
                row_batch = []
                
                for i, row in enumerate(reader, 1):
                    # 构建查询key
                    if len(key_columns) == 1:
                        id_hash = row.get(key_columns[0], '')
                    else:
                        key_values = tuple(row.get(col, '') for col in key_columns)
                        id_hash = '|'.join(key_values)
                    
                    query_batch.append(id_hash)
                    row_batch.append(row)
                    
                    # 批量查询
                    if len(query_batch) >= query_batch_size:
                        # 使用 IN 查询批量检查(即使有重复,IN也只返回唯一值)
                        placeholders = ','.join('?' * len(query_batch))
                        cursor.execute(f'SELECT DISTINCT id_hash FROM id_index WHERE id_hash IN ({placeholders})', query_batch)
                        existing_ids = set(r[0] for r in cursor.fetchall())
                        
                        # 过滤新记录
                        for idx, (qid, qrow) in enumerate(zip(query_batch, row_batch)):
                            if qid not in existing_ids:
                                new_records.append(qrow)
                            else:
                                duplicate_count += 1
                        
                        # 批量写入
                        if len(new_records) >= chunk_size:
                            writer.writerows(new_records)
                            new_records = []
                        
                        query_batch = []
                        row_batch = []
                    
                    # 每100万行显示进度
                    if i % 1000000 == 0:
                        speed = i / ((datetime.now().timestamp() - self.start_time.timestamp()) / 60)
                        logger.info(f"  已处理 {i:,} 行 | 速度: {speed:,.0f} 行/分钟 | 新: {i - duplicate_count:,} | 重复: {duplicate_count:,}")
                
                # 处理剩余数据
                if query_batch:
                    placeholders = ','.join('?' * len(query_batch))
                    cursor.execute(f'SELECT DISTINCT id_hash FROM id_index WHERE id_hash IN ({placeholders})', query_batch)
                    existing_ids = set(r[0] for r in cursor.fetchall())
                    
                    for qid, qrow in zip(query_batch, row_batch):
                        if qid not in existing_ids:
                            new_records.append(qrow)
                        else:
                            duplicate_count += 1
                
                # 写入剩余数据
                if new_records:
                    writer.writerows(new_records)
        
        conn.close()
        
        # 删除临时数据库（如果配置允许）
        if not self.config.get('keep_index', False):
            if db_path.exists():
                db_path.unlink()
                logger.info(f"  已清理临时索引数据库")
        else:
            logger.info(f"  保留索引数据库: {db_path}")
        
        new_count = i - duplicate_count
        logger.info(f"\n{'='*60}")
        logger.info(f"✓ 去重完成:")
        logger.info(f"  总记录数: {i:,}")
        logger.info(f"  新记录数: {new_count:,}")
        logger.info(f"  重复记录: {duplicate_count:,}")
        logger.info(f"  去重率: {(duplicate_count / i * 100):.2f}%")
        logger.info(f"  输出文件: {output_file}")
        logger.info(f"  文件大小: {output_file.stat().st_size / (1024**3):.2f} GB")
        logger.info(f"{'='*60}")
        
        return True
    
    def run(self):
        """运行完整流程"""
        logger.info(f"\n{'#'*80}")
        logger.info(f"OpenAlex 增量数据去重工具")
        logger.info(f"{'#'*80}")
        logger.info(f"增量数据目录: {self.incremental_dir}")
        logger.info(f"全量数据目录: {self.full_data_dir}")
        logger.info(f"临时目录: {self.temp_dir}")
        logger.info(f"输出目录: {self.output_dir}")
        logger.info(f"{'#'*80}\n")
        
        self.start_time = datetime.now()  # 记录开始时间，用于计算速度
        
        # 获取分片配置
        use_sharding = self.config.get('use_sharding', True)  # 默认使用分片
        num_shards = self.config.get('num_shards', 100)  # 默认100个分片
        large_files = self.config.get('large_files', ['works_referenced_works.csv'])  # 大文件列表
        
        for filename in self.files_to_process:
            try:
                pattern = filename.replace('.csv', '')
                decompressed = self.decompress_gz_to_csv(pattern)
                
                if not decompressed:
                    logger.warning(f"跳过 {filename}")
                    continue
                
                # 根据文件大小选择策略
                if use_sharding and filename in large_files:
                    logger.info(f"  使用分片策略处理大文件: {filename}")
                    self.merge_with_dedup_sharding(filename, num_shards)
                else:
                    logger.info(f"  使用SQLite策略处理: {filename}")
                    self.merge_with_dedup(filename)
                
            except Exception as e:
                logger.error(f"处理 {filename} 失败: {e}")
                continue
        
        elapsed = datetime.now() - self.start_time
        logger.info(f"\n{'#'*80}")
        logger.info(f"全部完成！")
        logger.info(f"总耗时: {elapsed}")
        logger.info(f"输出目录: {self.output_dir}")
        logger.info(f"{'#'*80}")


def main():
    """主函数"""
    merger = OptimizedIncrementalMerger()
    merger.run()


if __name__ == '__main__':
    main()
