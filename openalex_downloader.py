#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenAlex 数据快照自动下载工具
使用方法：修改 config.json 中的日期，然后运行此脚本
"""

import os
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import concurrent.futures
from pathlib import Path
from tqdm import tqdm
import logging
from datetime import datetime, timedelta
import csv
import gzip
import glob

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('openalex_download.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class OpenAlexDownloader:
    """OpenAlex 数据下载器"""
    
    BASE_URL = "https://openalex.s3.amazonaws.com/data/"
    
    def __init__(self, config_file="config.json"):
        """初始化下载器"""
        self.config = self.load_config(config_file)
        
        # 支持新旧配置格式
        if 'start_date' in self.config['download_settings']:
            self.start_date = self.config['download_settings']['start_date']
            self.end_date = self.config['download_settings'].get('end_date', 'latest')
            self.updated_date = None  # 不再使用单一日期
        else:
            # 兼容旧配置
            self.updated_date = self.config['download_settings']['updated_date']
            self.start_date = self.updated_date
            self.end_date = self.updated_date
        
        self.data_types = self.config['download_settings']['data_types']
        self.download_dir = Path(self.config['download_settings']['download_dir'])
        self.csv_dir = Path(self.config['download_settings'].get('csv_dir', './csv-files'))
        self.parallel_downloads = self.config['download_settings']['parallel_downloads']
        self.auto_download = self.config['download_settings'].get('auto_download', True)
        self.auto_flatten = self.config['download_settings'].get('auto_flatten', False)
        
        # 创建下载目录
        self.download_dir.mkdir(parents=True, exist_ok=True)
        if self.auto_flatten:
            self.csv_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"初始化下载器")
        logger.info(f"日期范围: {self.start_date} 到 {self.end_date}")
        logger.info(f"数据类型: {', '.join(self.data_types)}")
        logger.info(f"自动下载: {self.auto_download}")
        logger.info(f"自动展平: {self.auto_flatten}")
    
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
    
    def get_available_dates(self, data_type):
        """获取指定数据类型的所有可用日期"""
        try:
            api_url = f"https://openalex.s3.amazonaws.com/?prefix=data/{data_type}/updated_date="
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            # 解析XML响应
            soup = BeautifulSoup(response.content, 'xml')
            dates = set()
            
            for content in soup.find_all('Contents'):
                key = content.find('Key')
                if key:
                    # 提取日期部分，格式如: data/authors/updated_date=2024-01-01/...
                    parts = key.text.split('/')
                    for part in parts:
                        if part.startswith('updated_date='):
                            date_str = part.replace('updated_date=', '')
                            dates.add(date_str)
            
            return sorted(dates, reverse=True)  # 降序排列，最新日期在前
            
        except Exception as e:
            logger.error(f"获取可用日期失败: {e}")
            return []
    
    def find_nearest_date(self, target_date, data_type):
        """查找最接近目标日期的可用日期"""
        available_dates = self.get_available_dates(data_type)
        
        if not available_dates:
            logger.error(f"无法获取 {data_type} 的可用日期列表")
            return None
        
        logger.info(f"{data_type} 共有 {len(available_dates)} 个可用日期")
        logger.info(f"最新日期: {available_dates[0]}, 最旧日期: {available_dates[-1]}")
        
        # 如果目标日期在可用列表中，直接返回
        if target_date in available_dates:
            logger.info(f"✓ 找到目标日期: {target_date}")
            return target_date
        
        # 查找最接近的日期
        try:
            from datetime import datetime
            target_dt = datetime.strptime(target_date, '%Y-%m-%d')
            
            # 计算所有日期与目标日期的差距
            date_diffs = []
            for date_str in available_dates:
                try:
                    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                    diff = abs((date_dt - target_dt).days)
                    date_diffs.append((date_str, diff))
                except ValueError:
                    continue
            
            if not date_diffs:
                logger.warning(f"无法解析日期，使用最新可用日期")
                return available_dates[0]
            
            # 找到差距最小的日期
            nearest_date = min(date_diffs, key=lambda x: x[1])
            logger.warning(f"⚠ 未找到目标日期 {target_date}")
            logger.info(f"→ 自动选择最近的日期: {nearest_date[0]} (相差 {nearest_date[1]} 天)")
            
            return nearest_date[0]
            
        except Exception as e:
            logger.error(f"查找最近日期时出错: {e}")
            logger.info(f"使用最新可用日期: {available_dates[0]}")
            return available_dates[0]
    
    def get_dates_in_range(self, data_type, start_date, end_date):
        """获取指定日期范围内的所有可用日期"""
        available_dates = self.get_available_dates(data_type)
        
        if not available_dates:
            logger.error(f"无法获取 {data_type} 的可用日期列表")
            return []
        
        logger.info(f"{data_type} 共有 {len(available_dates)} 个可用日期")
        logger.info(f"可用日期范围: {available_dates[-1]} 到 {available_dates[0]}")
        
        try:
            from datetime import datetime
            
            # 处理 'latest' 关键字
            if end_date.lower() == 'latest':
                end_date = available_dates[0]  # 最新日期
                logger.info(f"end_date='latest' → 使用最新日期: {end_date}")
            
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
            
            # 筛选在范围内的日期
            dates_in_range = []
            for date_str in available_dates:
                try:
                    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                    if start_dt <= date_dt <= end_dt:
                        dates_in_range.append(date_str)
                except ValueError:
                    continue
            
            # 按日期排序（从旧到新）
            dates_in_range.sort()
            
            logger.info(f"✓ 找到 {len(dates_in_range)} 个日期在范围内: {start_date} 到 {end_date}")
            if dates_in_range:
                logger.info(f"  范围内日期: {dates_in_range[0]} ... {dates_in_range[-1]}")
            
            return dates_in_range
            
        except Exception as e:
            logger.error(f"获取日期范围时出错: {e}")
            return []
    
    def get_manifest_url(self, data_type, use_date=None):
        """获取 manifest 文件的URL"""
        # OpenAlex 数据快照的 manifest 文件路径
        # 格式通常为: data/{data_type}/updated_date={date}/manifest
        date_to_use = use_date if use_date else self.updated_date
        manifest_url = f"{self.BASE_URL}{data_type}/updated_date={date_to_use}/manifest"
        return manifest_url
    
    def download_manifest(self, data_type, use_date=None):
        """下载并解析 manifest 文件"""
        date_to_use = use_date if use_date else self.updated_date
        manifest_url = self.get_manifest_url(data_type, date_to_use)
        logger.info(f"正在获取 {data_type} 的 manifest: {manifest_url}")
        
        try:
            response = requests.get(manifest_url, timeout=30)
            response.raise_for_status()
            
            # Manifest 文件通常包含需要下载的所有文件列表
            file_urls = []
            for line in response.text.strip().split('\n'):
                if line.strip():
                    # 构建完整的文件URL
                    file_url = f"{self.BASE_URL}{data_type}/updated_date={date_to_use}/{line.strip()}"
                    file_urls.append(file_url)
            
            logger.info(f"{data_type} 共有 {len(file_urls)} 个文件需要下载")
            return file_urls, date_to_use
            
        except requests.exceptions.RequestException as e:
            logger.error(f"获取 {data_type} manifest 失败: {e}")
            return [], date_to_use
    
    def list_files_from_s3(self, data_type, use_date=None):
        """从S3列表页面获取文件列表（备用方案）"""
        date_to_use = use_date if use_date else self.updated_date
        browse_url = f"https://openalex.s3.amazonaws.com/browse.html#data/{data_type}/updated_date={date_to_use}/"
        logger.info(f"尝试从浏览页面获取文件列表: {browse_url}")
        
        try:
            # 直接构造API URL
            api_url = f"https://openalex.s3.amazonaws.com/?prefix=data/{data_type}/updated_date={date_to_use}/"
            response = requests.get(api_url, timeout=30)
            response.raise_for_status()
            
            # 解析XML响应
            soup = BeautifulSoup(response.content, 'xml')
            files = []
            
            for content in soup.find_all('Contents'):
                key = content.find('Key')
                if key:
                    file_path = key.text
                    # 只下载 .gz 文件
                    if file_path.endswith('.gz'):
                        file_url = f"https://openalex.s3.amazonaws.com/{file_path}"
                        files.append(file_url)
            
            logger.info(f"从S3找到 {len(files)} 个文件")
            return files, date_to_use
            
        except Exception as e:
            logger.error(f"从S3获取文件列表失败: {e}")
            return [], date_to_use
    
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
            
            logger.info(f"✓ 下载完成: {destination.name}")
            return True
            
        except Exception as e:
            logger.error(f"✗ 下载失败 {url}: {e}")
            if destination.exists():
                destination.unlink()
            return False
    
    def download_data_type(self, data_type):
        """下载特定类型的所有数据（支持日期范围）"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"开始处理 {data_type} 数据")
        logger.info(f"日期范围: {self.start_date} 到 {self.end_date}")
        logger.info(f"{'#'*60}")
        
        # 获取日期范围内的所有可用日期
        dates_to_download = self.get_dates_in_range(data_type, self.start_date, self.end_date)
        
        if not dates_to_download:
            logger.error(f"在指定范围内未找到 {data_type} 的可用日期")
            return
        
        logger.info(f"\n将处理 {len(dates_to_download)} 个日期的数据")
        
        # 收集所有日期的文件信息
        all_dates_data = []
        success_dates = []
        failed_dates = []
        
        for idx, date in enumerate(dates_to_download, 1):
            logger.info(f"\n进度: [{idx}/{len(dates_to_download)}] - 日期: {date}")
            
            # 获取该日期的文件列表
            file_urls, used_date = self.download_manifest(data_type, date)
            
            if not file_urls:
                logger.info("尝试备用方案：从S3列表获取文件")
                file_urls, used_date = self.list_files_from_s3(data_type, date)
            
            if not file_urls:
                logger.warning(f"未找到 {data_type} 在日期 {date} 的文件")
                failed_dates.append(date)
                continue
            
            logger.info(f"找到 {len(file_urls)} 个文件")
            all_dates_data.append((date, file_urls))
            success_dates.append(date)
        
        # 如果不需要自动下载，到此结束
        if not self.auto_download:
            logger.info(f"\n跳过自动下载")
            logger.info(f"成功处理: {len(success_dates)} 个日期")
            if failed_dates:
                logger.warning(f"失败: {len(failed_dates)} 个日期 - {', '.join(failed_dates)}")
            return
        
        # 开始下载所有日期的文件
        logger.info(f"\n{'='*60}")
        logger.info(f"开始下载文件...")
        logger.info(f"{'='*60}")
        
        download_success_dates = []
        download_failed_dates = []
        
        for date, file_urls in all_dates_data:
            logger.info(f"\n下载日期: {date}")
            
            # 创建目录
            type_dir = self.download_dir / data_type / f"updated_date={date}"
            type_dir.mkdir(parents=True, exist_ok=True)
            
            # 过滤已下载的文件
            files_to_download = []
            for url in file_urls:
                filename = url.split('/')[-1]
                dest_path = type_dir / filename
                if dest_path.exists():
                    logger.info(f"跳过已存在: {filename}")
                else:
                    files_to_download.append((url, dest_path))
            
            if not files_to_download:
                logger.info(f"{date} 的所有文件已下载")
                download_success_dates.append(date)
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
            
            logger.info(f"{date} 下载完成: {success_count}/{len(files_to_download)} 成功")
            
            if success_count == len(files_to_download):
                download_success_dates.append(date)
            else:
                download_failed_dates.append(date)
        
        # 总结
        logger.info(f"\n{'='*60}")
        logger.info(f"{data_type} 所有处理完成")
        logger.info(f"文件获取成功: {len(success_dates)} 个日期")
        logger.info(f"下载成功: {len(download_success_dates)} 个日期")
        if download_failed_dates:
            logger.warning(f"下载失败: {len(download_failed_dates)} 个日期 - {', '.join(download_failed_dates)}")
        logger.info(f"{'='*60}")
    
    def download_all(self):
        """下载所有配置的数据类型"""
        start_time = datetime.now()
        logger.info(f"\n{'#'*60}")
        logger.info(f"开始下载 OpenAlex 数据快照")
        logger.info(f"日期范围: {self.start_date} 到 {self.end_date}")
        logger.info(f"数据类型: {', '.join(self.data_types)}")
        logger.info(f"{'#'*60}\n")
        
        for data_type in self.data_types:
            try:
                self.download_data_type(data_type)
            except Exception as e:
                logger.error(f"下载 {data_type} 时发生错误: {e}", exc_info=True)
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"所有下载任务完成！")
        logger.info(f"总耗时: {duration}")
        logger.info(f"文件保存在: {self.download_dir.absolute()}")
        logger.info(f"{'#'*60}\n")
        
        # 如果启用自动展平，开始展平数据
        if self.auto_flatten:
            logger.info(f"\n开始自动展平数据...")
            self.flatten_all_data()
    
    def flatten_all_data(self):
        """展平所有下载的数据为CSV格式"""
        logger.info(f"\n{'#'*60}")
        logger.info(f"开始数据展平处理")
        logger.info(f"CSV 输出目录: {self.csv_dir.absolute()}")
        logger.info(f"{'#'*60}\n")
        
        flattener = OpenAlexFlattener(self.download_dir, self.csv_dir)
        
        for data_type in self.data_types:
            try:
                if data_type == 'authors':
                    flattener.flatten_authors()
                elif data_type == 'topics':
                    flattener.flatten_topics()
                elif data_type == 'institutions':
                    flattener.flatten_institutions()
                elif data_type == 'works':
                    flattener.flatten_works()
                else:
                    logger.warning(f"未知的数据类型: {data_type}")
            except Exception as e:
                logger.error(f"展平 {data_type} 时发生错误: {e}", exc_info=True)
        
        logger.info(f"\n{'#'*60}")
        logger.info(f"数据展平完成！")
        logger.info(f"CSV 文件保存在: {self.csv_dir.absolute()}")
        logger.info(f"{'#'*60}\n")


class OpenAlexFlattener:
    """OpenAlex 数据展平器 - 将JSONL转换为CSV"""
    
    def __init__(self, download_dir, csv_dir):
        self.download_dir = Path(download_dir)
        self.csv_dir = Path(csv_dir)
        self.csv_dir.mkdir(parents=True, exist_ok=True)
    
    def init_dict_writer(self, csv_file, columns, **kwargs):
        """初始化CSV写入器"""
        writer = csv.DictWriter(csv_file, fieldnames=columns, **kwargs)
        writer.writeheader()
        return writer
    
    def flatten_authors(self):
        """展平 authors 数据"""
        logger.info(f"正在展平 authors 数据...")
        
        with gzip.open(self.csv_dir / 'authors.csv.gz', 'wt', encoding='utf-8') as authors_csv, \
             gzip.open(self.csv_dir / 'authors_ids.csv.gz', 'wt', encoding='utf-8') as ids_csv, \
             gzip.open(self.csv_dir / 'authors_counts_by_year.csv.gz', 'wt', encoding='utf-8') as counts_csv:
            
            authors_writer = self.init_dict_writer(authors_csv, [
                'id', 'orcid', 'display_name', 'display_name_alternatives',
                'works_count', 'cited_by_count', 'last_known_institution',
                'works_api_url', 'updated_date'
            ], extrasaction='ignore')
            
            ids_writer = self.init_dict_writer(ids_csv, [
                'author_id', 'openalex', 'orcid', 'scopus', 'twitter',
                'wikipedia', 'mag'
            ])
            
            counts_writer = self.init_dict_writer(counts_csv, [
                'author_id', 'year', 'works_count', 'cited_by_count',
                'oa_works_count'
            ])
            
            file_count = 0
            record_count = 0
            
            # 使用正确的路径模式：authors/updated_date=*/
            pattern = str(self.download_dir / 'authors' / 'updated_date=*' / '*.gz')
            logger.info(f"  搜索文件: {pattern}")
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理文件: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        
                        author = json.loads(line)
                        author_id = author.get('id')
                        if not author_id:
                            continue
                        
                        # authors主表
                        author['display_name_alternatives'] = json.dumps(
                            author.get('display_name_alternatives'), ensure_ascii=False)
                        author['last_known_institution'] = (
                            author.get('last_known_institution') or {}).get('id')
                        authors_writer.writerow(author)
                        
                        # ids表
                        if author_ids := author.get('ids'):
                            author_ids['author_id'] = author_id
                            ids_writer.writerow(author_ids)
                        
                        # counts_by_year表
                        if counts_by_year := author.get('counts_by_year'):
                            for count in counts_by_year:
                                count['author_id'] = author_id
                                counts_writer.writerow(count)
                        
                        record_count += 1
                
                file_count += 1
            
            logger.info(f"✓ authors 完成: {file_count} 个文件, {record_count} 条记录")
    
    def flatten_topics(self):
        """展平 topics 数据"""
        logger.info(f"正在展平 topics 数据...")
        
        with gzip.open(self.csv_dir / 'topics.csv.gz', 'wt', encoding='utf-8') as topics_csv:
            topics_writer = self.init_dict_writer(topics_csv, [
                'id', 'display_name', 'subfield_id', 'subfield_display_name',
                'field_id', 'field_display_name', 'domain_id', 'domain_display_name',
                'description', 'keywords', 'works_api_url', 'wikipedia_id',
                'works_count', 'cited_by_count', 'updated_date', 'siblings'
            ])
            
            seen_ids = set()
            file_count = 0
            record_count = 0
            
            pattern = str(self.download_dir / 'topics' / 'updated_date=*' / '*.gz')
            logger.info(f"  搜索文件: {pattern}")
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理文件: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        
                        topic = json.loads(line)
                        topic_id = topic.get('id')
                        if not topic_id or topic_id in seen_ids:
                            continue
                        
                        seen_ids.add(topic_id)
                        
                        # 展开嵌套字段
                        for key in ('subfield', 'field', 'domain'):
                            if key in topic and topic[key]:
                                topic[f'{key}_id'] = topic[key].get('id')
                                topic[f'{key}_display_name'] = topic[key].get('display_name')
                                del topic[key]
                        
                        topic['keywords'] = '; '.join(topic.get('keywords', []))
                        topic['updated_date'] = topic.get('updated')
                        topic['wikipedia_id'] = (topic.get('ids') or {}).get('wikipedia')
                        
                        topics_writer.writerow(topic)
                        record_count += 1
                
                file_count += 1
            
            logger.info(f"✓ topics 完成: {file_count} 个文件, {record_count} 条记录")
    
    def flatten_institutions(self):
        """展平 institutions 数据"""
        logger.info(f"正在展平 institutions 数据...")
        
        with gzip.open(self.csv_dir / 'institutions.csv.gz', 'wt', encoding='utf-8') as inst_csv, \
             gzip.open(self.csv_dir / 'institutions_ids.csv.gz', 'wt', encoding='utf-8') as ids_csv, \
             gzip.open(self.csv_dir / 'institutions_geo.csv.gz', 'wt', encoding='utf-8') as geo_csv:
            
            inst_writer = self.init_dict_writer(inst_csv, [
                'id', 'ror', 'display_name', 'country_code', 'type',
                'homepage_url', 'image_url', 'image_thumbnail_url',
                'display_name_acronyms', 'display_name_alternatives',
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
            
            seen_ids = set()
            file_count = 0
            record_count = 0
            
            pattern = str(self.download_dir / 'institutions' / 'updated_date=*' / '*.gz')
            logger.info(f"  搜索文件: {pattern}")
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理文件: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        
                        inst = json.loads(line)
                        inst_id = inst.get('id')
                        if not inst_id or inst_id in seen_ids:
                            continue
                        
                        seen_ids.add(inst_id)
                        
                        # institutions主表
                        inst['display_name_acronyms'] = json.dumps(
                            inst.get('display_name_acronyms'), ensure_ascii=False)
                        inst['display_name_alternatives'] = json.dumps(
                            inst.get('display_name_alternatives'), ensure_ascii=False)
                        inst_writer.writerow(inst)
                        
                        # ids表
                        if inst_ids := inst.get('ids'):
                            inst_ids['institution_id'] = inst_id
                            ids_writer.writerow(inst_ids)
                        
                        # geo表
                        if geo := inst.get('geo'):
                            geo['institution_id'] = inst_id
                            geo_writer.writerow(geo)
                        
                        record_count += 1
                
                file_count += 1
            
            logger.info(f"✓ institutions 完成: {file_count} 个文件, {record_count} 条记录")
    
    def flatten_works(self):
        """展平 works 数据"""
        logger.info(f"正在展平 works 数据...")
        
        with gzip.open(self.csv_dir / 'works.csv.gz', 'wt', encoding='utf-8') as works_csv, \
             gzip.open(self.csv_dir / 'works_authorships.csv.gz', 'wt', encoding='utf-8') as auth_csv, \
             gzip.open(self.csv_dir / 'works_biblio.csv.gz', 'wt', encoding='utf-8') as biblio_csv, \
             gzip.open(self.csv_dir / 'works_topics.csv.gz', 'wt', encoding='utf-8') as topics_csv, \
             gzip.open(self.csv_dir / 'works_referenced_works.csv.gz', 'wt', encoding='utf-8') as ref_csv:
            
            works_writer = self.init_dict_writer(works_csv, [
                'id', 'doi', 'title', 'display_name', 'publication_year',
                'publication_date', 'type', 'cited_by_count', 'is_retracted',
                'is_paratext', 'cited_by_api_url', 'abstract_inverted_index', 'language'
            ], extrasaction='ignore')
            
            auth_writer = self.init_dict_writer(auth_csv, [
                'work_id', 'author_position', 'author_id', 'institution_id',
                'raw_affiliation_string'
            ])
            
            biblio_writer = self.init_dict_writer(biblio_csv, [
                'work_id', 'volume', 'issue', 'first_page', 'last_page'
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
            logger.info(f"  搜索文件: {pattern}")
            
            for jsonl_file in glob.glob(pattern):
                logger.info(f"  处理文件: {Path(jsonl_file).name}")
                with gzip.open(jsonl_file, 'rt', encoding='utf-8') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        
                        work = json.loads(line)
                        work_id = work.get('id')
                        if not work_id:
                            continue
                        
                        # works主表
                        if (abstract := work.get('abstract_inverted_index')) is not None:
                            work['abstract_inverted_index'] = json.dumps(abstract, ensure_ascii=False)
                        works_writer.writerow(work)
                        
                        # authorships表
                        if authorships := work.get('authorships'):
                            for authorship in authorships:
                                author_id = (authorship.get('author') or {}).get('id')
                                if not author_id:
                                    continue
                                
                                institutions = authorship.get('institutions', [])
                                institution_ids = [i.get('id') for i in institutions if i.get('id')]
                                
                                if not institution_ids:
                                    institution_ids = [None]
                                
                                for inst_id in institution_ids:
                                    auth_writer.writerow({
                                        'work_id': work_id,
                                        'author_position': authorship.get('author_position'),
                                        'author_id': author_id,
                                        'institution_id': inst_id,
                                        'raw_affiliation_string': authorship.get('raw_affiliation_string')
                                    })
                        
                        # biblio表
                        if biblio := work.get('biblio'):
                            biblio['work_id'] = work_id
                            biblio_writer.writerow(biblio)
                        
                        # topics表
                        for topic in work.get('topics', []):
                            if topic_id := topic.get('id'):
                                topics_writer.writerow({
                                    'work_id': work_id,
                                    'topic_id': topic_id,
                                    'score': topic.get('score')
                                })
                        
                        # referenced_works表
                        for ref_work in work.get('referenced_works', []):
                            if ref_work:
                                ref_writer.writerow({
                                    'work_id': work_id,
                                    'referenced_work_id': ref_work
                                })
                        
                        record_count += 1
                
                file_count += 1
            
            logger.info(f"✓ works 完成: {file_count} 个文件, {record_count} 条记录")


def main():
    """主函数"""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║     OpenAlex 数据快照下载 + 展平工具                     ║
    ║                                                          ║
    ║  使用说明：                                              ║
    ║  1. 编辑 config.json 文件，修改日期范围                 ║
    ║  2. 运行此脚本自动下载并展平为CSV                       ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    try:
        downloader = OpenAlexDownloader()
        downloader.download_all()
    except KeyboardInterrupt:
        logger.info("\n用户中断")
    except Exception as e:
        logger.error(f"发生错误: {e}", exc_info=True)


if __name__ == "__main__":
    main()
