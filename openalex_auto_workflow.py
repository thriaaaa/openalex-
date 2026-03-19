#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlex 全自动化数据处理流程
功能：下载 → 展平 → 去重合并
使用方法：修改 openalex_auto_config.json，然后运行此脚本
"""

import os
import sys
import gzip
import csv
import json
import glob
import requests
import concurrent.futures
from bs4 import BeautifulSoup
from pathlib import Path
from tqdm import tqdm
import logging
from datetime import datetime
import shutil

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('openalex_auto_workflow.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OpenAlexAutoWorkflow:
    """OpenAlex 全自动化处理流程"""
    
    BASE_URL = "https://openalex.s3.amazonaws.com/data/"
    
    def __init__(self, config_file="openalex_auto_config.json"):
        """初始化"""
        self.config = self.load_config(config_file)
        
        # 下载设置
        dl_cfg = self.config['download_settings']
        self.start_date = dl_cfg['start_date']
        self.end_date = dl_cfg.get('end_date', 'latest')
        self.data_types = dl_cfg['data_types']
        self.download_dir = Path(dl_cfg['download_dir'])
        self.csv_dir = Path(dl_cfg['csv_dir'])
        self.parallel_downloads = dl_cfg['parallel_downloads']
        
        # 合并设置
        mg_cfg = self.config['merge_settings']
        self.full_data_dir = Path(mg_cfg['full_data_dir'])
        self.output_dir = Path(mg_cfg['output_dir'])
        self.temp_dir = Path(mg_cfg.get('temp_dir', './temp_decompressed'))
        self.special_file_paths = mg_cfg.get('special_file_paths', {})
        self.column_mappings = mg_cfg.get('column_mappings', {})
        self.files_to_process = mg_cfg.get('files_to_process', [])
        
        # 工作流设置
        wf_cfg = self.config['workflow']
        self.workflow_steps = wf_cfg.get('steps', ['download', 'flatten', 'merge'])
        self.skip_existing = wf_cfg.get('skip_existing_downloads', True)
        self.cleanup_after_merge = wf_cfg.get('cleanup_after_merge', False)

        # schema mapping 设置
        self.schema_mapping_steps = self.config.get(
            'schema_mapping_settings', {}
        ).get('steps', None)  # None 表示执行全部
        
        # 创建目录
        self.download_dir.mkdir(parents=True, exist_ok=True)
        self.csv_dir.mkdir(parents=True, exist_ok=True)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("=" * 80)
        logger.info("OpenAlex 全自动化处理流程初始化")
        logger.info("=" * 80)
        logger.info(f"日期范围: {self.start_date} → {self.end_date}")
        logger.info(f"数据类型: {', '.join(self.data_types)}")
        logger.info(f"工作流步骤: {' → '.join(self.workflow_steps)}")
        logger.info(f"下载目录: {self.download_dir}")
        logger.info(f"CSV目录: {self.csv_dir}")
        logger.info(f"输出目录: {self.output_dir}")
        if 'schema_mapping' in self.workflow_steps:
            sm_steps = self.schema_mapping_steps or ['全部']
            logger.info(f"Schema Mapping 子步骤: {', '.join(sm_steps)}")
        logger.info("=" * 80)
    
    @staticmethod
    def load_config(config_file):
        """加载配置文件"""
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"配置文件 {config_file} 不存在！")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"配置文件格式错误: {e}")
            raise
    
    # ==================== 步骤1: 下载 ====================
    
    def get_available_dates(self, data_type):
        """获取指定数据类型的所有可用日期"""
        try:
            api_url = f"https://openalex.s3.amazonaws.com/?prefix=data/{data_type}/updated_date="
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            dates = set()
            
            for content in soup.find_all('Contents'):
                key = content.find('Key')
                if key:
                    parts = key.text.split('/')
                    for part in parts:
                        if part.startswith('updated_date='):
                            date_str = part.replace('updated_date=', '')
                            dates.add(date_str)
            
            return sorted(dates, reverse=True)
            
        except Exception as e:
            logger.error(f"获取可用日期失败: {e}")
            return []
    
    def get_dates_in_range(self, data_type, start_date, end_date):
        """获取指定日期范围内的所有可用日期"""
        available_dates = self.get_available_dates(data_type)
        
        if not available_dates:
            logger.error(f"无法获取 {data_type} 的可用日期列表")
            return []
        
        logger.info(f"{data_type} 共有 {len(available_dates)} 个可用日期")
        
        try:
            if end_date.lower() == 'latest':
                end_date = available_dates[0]
                logger.info(f"end_date='latest' → {end_date}")
            
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            dates_in_range = []
            for date_str in available_dates:
                try:
                    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                    if start_dt <= date_dt <= end_dt:
                        dates_in_range.append(date_str)
                except ValueError:
                    continue
            
            dates_in_range.sort()
            logger.info(f"✓ 找到 {len(dates_in_range)} 个日期在范围内")
            return dates_in_range
            
        except Exception as e:
            logger.error(f"获取日期范围时出错: {e}")
            return []
    
    def list_files_from_s3(self, data_type, use_date):
        """从S3获取文件列表"""
        try:
            api_url = f"https://openalex.s3.amazonaws.com/?prefix=data/{data_type}/updated_date={use_date}/"
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'xml')
            files = []
            
            for content in soup.find_all('Contents'):
                key = content.find('Key')
                if key and key.text.endswith('.gz'):
                    file_url = f"https://openalex.s3.amazonaws.com/{key.text}"
                    files.append(file_url)
            
            return files
            
        except Exception as e:
            logger.error(f"从S3获取文件列表失败: {e}")
            return []
    
    def download_file(self, url, destination):
        """下载单个文件"""
        try:
            response = requests.get(url, stream=True, timeout=300)
            response.raise_for_status()
            
            total_size = int(response.headers.get('content-length', 0))
            
            with open(destination, 'wb') as f, tqdm(
                desc=destination.name,
                total=total_size,
                unit='B',
                unit_scale=True,
                unit_divisor=1024,
                leave=False
            ) as pbar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
            
            logger.info(f"✓ 下载: {destination.name}")
            return True
            
        except Exception as e:
            logger.error(f"✗ 下载失败 {url}: {e}")
            if destination.exists():
                destination.unlink()
            return False
    
    def download_data_type(self, data_type):
        """下载特定类型的数据"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"下载 {data_type} 数据")
        logger.info(f"{'#'*60}")
        
        dates_to_download = self.get_dates_in_range(data_type, self.start_date, self.end_date)
        
        if not dates_to_download:
            logger.error(f"未找到可用日期")
            return False
        
        logger.info(f"将下载 {len(dates_to_download)} 个日期的数据")
        
        for idx, date in enumerate(dates_to_download, 1):
            logger.info(f"\n[{idx}/{len(dates_to_download)}] 日期: {date}")
            
            # 创建目录
            type_dir = self.download_dir / data_type / f"updated_date={date}"
            type_dir.mkdir(parents=True, exist_ok=True)
            
            # 获取文件列表
            file_urls = self.list_files_from_s3(data_type, date)
            
            if not file_urls:
                logger.warning(f"未找到文件")
                continue
            
            # 过滤已下载的文件
            files_to_download = []
            for url in file_urls:
                filename = url.split('/')[-1]
                dest_path = type_dir / filename
                if dest_path.exists() and self.skip_existing:
                    logger.info(f"跳过: {filename}")
                else:
                    files_to_download.append((url, dest_path))
            
            if not files_to_download:
                logger.info(f"所有文件已存在")
                continue
            
            logger.info(f"需要下载 {len(files_to_download)} 个文件")
            
            # 并行下载
            success_count = 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.parallel_downloads) as executor:
                futures = [executor.submit(self.download_file, url, dest) 
                          for url, dest in files_to_download]
                
                for future in concurrent.futures.as_completed(futures):
                    if future.result():
                        success_count += 1
            
            logger.info(f"下载完成: {success_count}/{len(files_to_download)} 成功")
        
        return True
    
    # ==================== 步骤2: 展平 ====================
    
    def init_dict_writer(self, csv_file, columns, **kwargs):
        """初始化CSV写入器"""
        writer = csv.DictWriter(csv_file, fieldnames=columns, **kwargs)
        writer.writeheader()
        return writer
    
    def flatten_authors(self):
        """展平 authors 数据"""
        logger.info(f"\n{'='*60}")
        logger.info(f"展平 authors 数据")
        logger.info(f"{'='*60}")
        
        with gzip.open(self.csv_dir / 'authors.csv.gz', 'wt', encoding='utf-8') as authors_csv, \
             gzip.open(self.csv_dir / 'authors_ids.csv.gz', 'wt', encoding='utf-8') as ids_csv:
            
            authors_writer = self.init_dict_writer(authors_csv, [
                'id', 'orcid', 'display_name', 'display_name_alternatives',
                'works_count', 'cited_by_count', 'last_known_institution',
                'works_api_url', 'updated_date'
            ], extrasaction='ignore')
            
            ids_writer = self.init_dict_writer(ids_csv, [
                'author_id', 'openalex', 'orcid', 'scopus', 'twitter',
                'wikipedia', 'mag'
            ])
            
            file_count = 0
            record_count = 0
            
            pattern = str(self.download_dir / 'authors' / 'updated_date=*' / '*.gz')
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            author = json.loads(line)
                            authors_writer.writerow(author)
                            
                            # IDs
                            if author.get('ids'):
                                ids_row = {'author_id': author['id']}
                                ids_row.update(author['ids'])
                                ids_writer.writerow(ids_row)
                            
                            record_count += 1
                        except Exception as e:
                            continue
                
                file_count += 1
            
            logger.info(f"✓ 完成: {file_count} 文件, {record_count:,} 记录")
    
    def flatten_topics(self):
        """展平 topics 数据"""
        logger.info(f"\n{'='*60}")
        logger.info(f"展平 topics 数据")
        logger.info(f"{'='*60}")
        
        with gzip.open(self.csv_dir / 'topics.csv.gz', 'wt', encoding='utf-8') as topics_csv:
            topics_writer = self.init_dict_writer(topics_csv, [
                'id', 'display_name', 'subfield_id', 'subfield_display_name',
                'field_id', 'field_display_name', 'domain_id', 'domain_display_name',
                'description', 'keywords', 'works_api_url', 'wikipedia_id',
                'works_count', 'cited_by_count', 'updated_date'
            ], extrasaction='ignore')
            
            file_count = 0
            record_count = 0
            
            pattern = str(self.download_dir / 'topics' / 'updated_date=*' / '*.gz')
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            topic = json.loads(line)
                            
                            # 展平嵌套字段
                            row = {
                                'id': topic.get('id'),
                                'display_name': topic.get('display_name'),
                                'description': topic.get('description'),
                                'keywords': ','.join(topic.get('keywords', [])),
                                'works_api_url': topic.get('works_api_url'),
                                'wikipedia_id': topic.get('ids', {}).get('wikipedia'),
                                'works_count': topic.get('works_count'),
                                'cited_by_count': topic.get('cited_by_count'),
                                'updated_date': topic.get('updated_date')
                            }
                            
                            # 展平层级结构
                            if topic.get('subfield'):
                                row['subfield_id'] = topic['subfield'].get('id')
                                row['subfield_display_name'] = topic['subfield'].get('display_name')
                            
                            if topic.get('field'):
                                row['field_id'] = topic['field'].get('id')
                                row['field_display_name'] = topic['field'].get('display_name')
                            
                            if topic.get('domain'):
                                row['domain_id'] = topic['domain'].get('id')
                                row['domain_display_name'] = topic['domain'].get('display_name')
                            
                            topics_writer.writerow(row)
                            record_count += 1
                        except Exception as e:
                            continue
                
                file_count += 1
            
            logger.info(f"✓ 完成: {file_count} 文件, {record_count:,} 记录")
    
    def flatten_institutions(self):
        """展平 institutions 数据"""
        logger.info(f"\n{'='*60}")
        logger.info(f"展平 institutions 数据")
        logger.info(f"{'='*60}")
        
        with gzip.open(self.csv_dir / 'institutions.csv.gz', 'wt', encoding='utf-8') as inst_csv, \
             gzip.open(self.csv_dir / 'institutions_ids.csv.gz', 'wt', encoding='utf-8') as ids_csv, \
             gzip.open(self.csv_dir / 'institutions_geo.csv.gz', 'wt', encoding='utf-8') as geo_csv:
            
            inst_writer = self.init_dict_writer(inst_csv, [
                'id', 'ror', 'display_name', 'country_code', 'type',
                'homepage_url', 'image_url', 'image_thumbnail_url',
                'works_count', 'cited_by_count', 'works_api_url', 'updated_date'
            ], extrasaction='ignore')
            
            ids_writer = self.init_dict_writer(ids_csv, [
                'institution_id', 'openalex', 'ror', 'grid', 'wikipedia',
                'wikidata', 'mag'
            ])
            
            geo_writer = self.init_dict_writer(geo_csv, [
                'institution_id', 'city', 'geonames_city_id', 'region',
                'country_code', 'country', 'latitude', 'longitude'
            ])
            
            file_count = 0
            record_count = 0
            
            pattern = str(self.download_dir / 'institutions' / 'updated_date=*' / '*.gz')
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            inst = json.loads(line)
                            inst_writer.writerow(inst)
                            
                            # IDs
                            if inst.get('ids'):
                                ids_row = {'institution_id': inst['id']}
                                ids_row.update(inst['ids'])
                                ids_writer.writerow(ids_row)
                            
                            # Geo
                            if inst.get('geo'):
                                geo_row = {'institution_id': inst['id']}
                                geo_row.update(inst['geo'])
                                geo_writer.writerow(geo_row)
                            
                            record_count += 1
                        except Exception as e:
                            continue
                
                file_count += 1
            
            logger.info(f"✓ 完成: {file_count} 文件, {record_count:,} 记录")
    
    def flatten_works(self):
        """展平 works 数据"""
        logger.info(f"\n{'='*60}")
        logger.info(f"展平 works 数据")
        logger.info(f"{'='*60}")
        
        with gzip.open(self.csv_dir / 'works.csv.gz', 'wt', encoding='utf-8') as works_csv, \
             gzip.open(self.csv_dir / 'works_authorships.csv.gz', 'wt', encoding='utf-8') as auth_csv, \
             gzip.open(self.csv_dir / 'works_topics.csv.gz', 'wt', encoding='utf-8') as topics_csv, \
             gzip.open(self.csv_dir / 'works_referenced_works.csv.gz', 'wt', encoding='utf-8') as ref_csv:
            
            works_writer = self.init_dict_writer(works_csv, [
                'id', 'doi', 'title', 'display_name', 'publication_year',
                'publication_date', 'type', 'cited_by_count', 'is_retracted',
                'is_paratext', 'language'
            ], extrasaction='ignore')
            
            auth_writer = self.init_dict_writer(auth_csv, [
                'work_id', 'author_position', 'author_id', 'institution_id',
                'raw_affiliation_string'
            ])
            
            topics_writer = self.init_dict_writer(topics_csv, [
                'work_id', 'topic_id', 'score'
            ])
            
            ref_writer = self.init_dict_writer(ref_csv, [
                'work_id', 'referenced_work_id'
            ])
            
            file_count = 0
            record_count = 0
            
            pattern = str(self.download_dir / 'works' / 'updated_date=*' / '*.gz')
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        try:
                            work = json.loads(line)
                            works_writer.writerow(work)
                            
                            work_id = work['id']
                            
                            # Authorships
                            for idx, authorship in enumerate(work.get('authorships', [])):
                                auth_row = {
                                    'work_id': work_id,
                                    'author_position': authorship.get('author_position'),
                                    'author_id': authorship.get('author', {}).get('id'),
                                    'institution_id': authorship.get('institutions', [{}])[0].get('id') if authorship.get('institutions') else None,
                                    'raw_affiliation_string': authorship.get('raw_affiliation_string')
                                }
                                auth_writer.writerow(auth_row)
                            
                            # Topics
                            for topic in work.get('topics', []):
                                topics_writer.writerow({
                                    'work_id': work_id,
                                    'topic_id': topic.get('id'),
                                    'score': topic.get('score')
                                })
                            
                            # Referenced works
                            for ref_id in work.get('referenced_works', []):
                                ref_writer.writerow({
                                    'work_id': work_id,
                                    'referenced_work_id': ref_id
                                })
                            
                            record_count += 1
                        except Exception as e:
                            continue
                
                file_count += 1
            
            logger.info(f"✓ 完成: {file_count} 文件, {record_count:,} 记录")
    
    # ==================== 步骤3: 去重合并 ====================
    
    def normalize_id(self, id_value):
        """标准化ID：从URL格式提取纯数字ID"""
        if not id_value:
            return ''
        
        if id_value.startswith('http'):
            parts = id_value.split('/')
            if parts:
                last_part = parts[-1]
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
        """解压 .gz 文件并转换ID为纯数字格式"""
        logger.info(f"\n{'='*60}")
        logger.info(f"解压并转换ID: {file_pattern}")
        logger.info(f"{'='*60}")
        
        gz_files = list(self.csv_dir.glob(f"{file_pattern}.csv.gz"))
        
        if not gz_files:
            logger.warning(f"未找到 {file_pattern}.csv.gz")
            return None
        
        output_file = self.temp_dir / f"{file_pattern}.csv"
        
        total_lines = 0
        with open(output_file, 'w', encoding='utf-8', newline='') as outf:
            writer = None
            
            for gz_file in gz_files:
                logger.info(f"  处理: {gz_file.name}")
                
                try:
                    with gzip.open(gz_file, 'rt', encoding='utf-8') as inf:
                        reader = csv.DictReader(inf)
                        
                        if writer is None:
                            writer = csv.DictWriter(outf, fieldnames=reader.fieldnames)
                            writer.writeheader()
                        
                        for i, row in enumerate(reader, 1):
                            row = self.normalize_row_ids(row)
                            writer.writerow(row)
                            total_lines += 1
                            
                            if i % 100000 == 0:
                                logger.info(f"    已处理 {i:,} 行")
                
                except Exception as e:
                    logger.error(f"  处理失败: {e}")
        
        logger.info(f"✓ 解压完成: {total_lines:,} 行")
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
        }
        return key_mapping.get(filename, ('id',))
    
    def build_id_index_chunked(self, csv_file, key_columns, filename=None):
        """分块构建 ID 索引"""
        logger.info(f"  构建索引: {csv_file.name}")
        
        # 获取列名映射
        column_mapping = {}
        if filename and filename in self.column_mappings:
            column_mapping = self.column_mappings[filename]
            logger.info(f"  列名映射: {column_mapping}")
        
        id_set = set()
        chunk_size = 50000
        
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                chunk = []
                for row in reader:
                    if len(key_columns) == 1:
                        col_name = column_mapping.get(key_columns[0], key_columns[0])
                        key = row.get(col_name, '')
                    else:
                        key = tuple(
                            row.get(column_mapping.get(col, col), '') 
                            for col in key_columns
                        )
                    
                    chunk.append(key)
                    
                    if len(chunk) >= chunk_size:
                        id_set.update(chunk)
                        chunk = []
                
                if chunk:
                    id_set.update(chunk)
        
        except Exception as e:
            logger.error(f"构建索引失败: {e}")
            return set()
        
        logger.info(f"  ✓ 索引: {len(id_set):,} 记录")
        return id_set
    
    def merge_with_dedup(self, filename):
        """去重并输出新记录"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"去重: {filename}")
        logger.info(f"{'#'*60}")
        
        incremental_file = self.temp_dir / filename
        
        # 检查特殊路径
        if filename in self.special_file_paths:
            full_file = Path(self.special_file_paths[filename])
            logger.info(f"  特殊路径: {full_file}")
        else:
            full_file = self.full_data_dir / filename
        
        output_file = self.output_dir / filename
        
        if not incremental_file.exists():
            logger.warning(f"增量文件不存在")
            return False
        
        if not full_file.exists():
            logger.warning(f"全量文件不存在，复制增量数据")
            shutil.copy(incremental_file, output_file)
            return True
        
        key_columns = self.get_key_columns(filename)
        
        logger.info("步骤 1/2: 构建全量数据索引")
        full_id_set = self.build_id_index_chunked(full_file, key_columns, filename)
        
        logger.info("步骤 2/2: 过滤重复数据")
        new_records = []
        duplicate_count = 0
        chunk_size = 50000
        
        with open(incremental_file, 'r', encoding='utf-8') as inf:
            reader = csv.DictReader(inf)
            fieldnames = reader.fieldnames
            
            with open(output_file, 'w', encoding='utf-8', newline='') as outf:
                writer = csv.DictWriter(outf, fieldnames=fieldnames)
                writer.writeheader()
                
                for i, row in enumerate(reader, 1):
                    if len(key_columns) == 1:
                        key = row.get(key_columns[0], '')
                    else:
                        key = tuple(row.get(col, '') for col in key_columns)
                    
                    if key not in full_id_set:
                        new_records.append(row)
                        
                        if len(new_records) >= chunk_size:
                            writer.writerows(new_records)
                            new_records = []
                    else:
                        duplicate_count += 1
                    
                    if i % 100000 == 0:
                        logger.info(f"  已处理 {i:,} 行")
                
                if new_records:
                    writer.writerows(new_records)
        
        new_count = i - duplicate_count
        logger.info(f"✓ 完成:")
        logger.info(f"  总记录: {i:,}")
        logger.info(f"  新记录: {new_count:,}")
        logger.info(f"  重复: {duplicate_count:,}")
        
        return True
    
    # ==================== 主流程 ====================
    
    def run(self):
        """运行完整的自动化流程"""
        overall_start = datetime.now()
        
        logger.info("\n" + "=" * 80)
        logger.info("开始 OpenAlex 全自动化处理流程")
        logger.info("=" * 80)
        
        # 步骤1: 下载
        if 'download' in self.workflow_steps:
            logger.info("\n" + "#" * 80)
            logger.info("步骤 1/3: 下载数据")
            logger.info("#" * 80)
            
            for data_type in self.data_types:
                try:
                    self.download_data_type(data_type)
                except Exception as e:
                    logger.error(f"下载 {data_type} 失败: {e}")
        
        # 步骤2: 展平
        if 'flatten' in self.workflow_steps:
            logger.info("\n" + "#" * 80)
            logger.info("步骤 2/3: 展平数据")
            logger.info("#" * 80)
            
            for data_type in self.data_types:
                try:
                    if data_type == 'authors':
                        self.flatten_authors()
                    elif data_type == 'topics':
                        self.flatten_topics()
                    elif data_type == 'institutions':
                        self.flatten_institutions()
                    elif data_type == 'works':
                        self.flatten_works()
                except Exception as e:
                    logger.error(f"展平 {data_type} 失败: {e}")
        
        # 步骤3: 去重合并
        if 'merge' in self.workflow_steps:
            logger.info("\n" + "#" * 80)
            logger.info("步骤 3: 去重合并")
            logger.info("#" * 80)
            
            for filename in self.files_to_process:
                try:
                    pattern = filename.replace('.csv', '')
                    decompressed = self.decompress_gz_to_csv(pattern)
                    
                    if decompressed:
                        self.merge_with_dedup(filename)
                except Exception as e:
                    logger.error(f"合并 {filename} 失败: {e}")
        
        # 清理
        if self.cleanup_after_merge and self.temp_dir.exists():
            logger.info("\n清理临时文件...")
            shutil.rmtree(self.temp_dir)

        # 步骤4: Schema Mapping（顶点 / 文档 / 边 / 向量）
        if 'schema_mapping' in self.workflow_steps:
            logger.info("\n" + "#" * 80)
            logger.info("步骤 4: Schema Mapping（顶点 / 文档 / 边 / 向量）")
            logger.info("#" * 80)
            self._run_schema_mapping()

        # 总结
        overall_time = datetime.now() - overall_start
        logger.info("\n" + "=" * 80)
        logger.info("✓ 全自动化流程完成！")
        logger.info(f"总耗时: {overall_time}")
        logger.info(f"输出目录: {self.output_dir}")
        logger.info("=" * 80)

    def _run_schema_mapping(self):
        """调用 schema_mapping/run_schema_mapping.py 中的调度器"""
        schema_mapping_dir = Path(__file__).resolve().parent / 'schema_mapping'
        runner_path = schema_mapping_dir / 'run_schema_mapping.py'

        if not runner_path.exists():
            logger.error(f"找不到 Schema Mapping 调度脚本: {runner_path}")
            return

        # 动态加载，避免顶层 import 依赖 GPU 库
        import importlib.util
        spec = importlib.util.spec_from_file_location("run_schema_mapping", runner_path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        try:
            runner = mod.SchemaMappingRunner(
                config_file=str(Path(__file__).resolve().parent / 'openalex_auto_config.json')
            )
            # self.schema_mapping_steps 为 None 时执行配置中启用的全部子步骤
            success = runner.run_steps(steps_to_run=self.schema_mapping_steps)
            if success:
                logger.info("✓ Schema Mapping 全部子步骤完成")
            else:
                logger.warning("⚠ Schema Mapping 部分子步骤失败，请查看日志")
        except Exception as e:
            logger.error(f"Schema Mapping 执行异常: {e}", exc_info=True)


def main():
    """主函数"""
    import argparse

    parser = argparse.ArgumentParser(
        description="OpenAlex 全自动化数据处理流程",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
步骤说明:
  download        下载增量数据（从 OpenAlex S3）
  flatten         展平 JSONL → CSV.GZ
  merge           去重合并到 openalex_merged/
  schema_mapping  模式映射（顶点 / 文档 / 边 / 向量）

示例:
  python openalex_auto_workflow.py                              # 按配置执行所有步骤
  python openalex_auto_workflow.py --steps download flatten    # 只执行下载和展平
  python openalex_auto_workflow.py --steps schema_mapping      # 只执行模式映射
  python openalex_auto_workflow.py --steps merge schema_mapping --mapping-steps 01 02 03
        """
    )
    parser.add_argument(
        "--steps", nargs="+",
        metavar="STEP",
        choices=["download", "flatten", "merge", "schema_mapping"],
        help="指定要执行的大流程步骤（覆盖配置文件中的 workflow.steps）"
    )
    parser.add_argument(
        "--mapping-steps", nargs="+",
        metavar="MAPPING_STEP",
        dest="mapping_steps",
        help="指定 schema_mapping 的子步骤（数字前缀或完整key），覆盖配置"
    )
    args = parser.parse_args()

    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     OpenAlex 全自动化数据处理流程                         ║
    ║                                                          ║
    ║  功能：下载 → 展平 → 去重合并 → 模式映射                 ║
    ║  使用：修改 openalex_auto_config.json 后运行             ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    try:
        workflow = OpenAlexAutoWorkflow()

        # 命令行参数覆盖配置
        if args.steps:
            workflow.workflow_steps = args.steps
            logger.info(f"[命令行] 覆盖 workflow.steps = {args.steps}")

        if args.mapping_steps:
            workflow.schema_mapping_steps = args.mapping_steps
            logger.info(f"[命令行] 覆盖 schema_mapping.steps = {args.mapping_steps}")

        workflow.run()
    except KeyboardInterrupt:
        logger.info("\n用户中断")
    except Exception as e:
        logger.error(f"发生错误: {e}", exc_info=True)


if __name__ == "__main__":
    main()
