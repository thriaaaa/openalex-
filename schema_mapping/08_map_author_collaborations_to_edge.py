#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
将 works_authorships.csv 和 works.csv 映射为作者合作边表
输出格式: startid, endid, properties
其中 properties 包含: {"cnt": 合作次数, "list": [{year, work_id}, ...]}

内存优化方案:
  1. works.csv 只保留 id->year 精简字典（sys.intern 压缩字符串占用）
  2. authorships 按 CHUNK_SIZE 行分块读取，每块生成合作对后直接落盘到临时文件，
     不在内存中累积全量 collaborations
  3. 对临时文件做外部排序归并（heapq.merge），边聚合边输出，整个归并过程
     内存占用约等于"并发打开的临时文件句柄数 × 单行大小"
  4. 分块读取 authorships 使用多进程并行生成合作对，加快 CPU 密集型的 combinations 运算
"""

import csv
import json
import logging
import os
import sys
import heapq
import tempfile
import multiprocessing as mp
from pathlib import Path
from datetime import datetime
from itertools import combinations, groupby

# ──────────────────────────────────────────────
# 可调参数
# ──────────────────────────────────────────────
# 每次读取 authorships 的行数（一块约占几十 MB 内存）
CHUNK_SIZE = 500_000
# 每个临时文件写满多少合作对后滚动到下一个文件（控制单临时文件大小）
PAIRS_PER_TMPFILE = 5_000_000
# 外部归并时每轮最多同时打开的临时文件数（防止文件句柄耗尽）
MERGE_FAN_IN = 16
# 工作进程数（None = CPU 核数）
NUM_WORKERS = None
# ──────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('author_collaborations_mapping.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────
# 多进程 worker：处理一批 (work_id, authors, year) 生成合作对
# ──────────────────────────────────────────────
def _worker_generate_pairs(args):
    """
    输入: [(work_id, [author_id, ...], year), ...]
    输出: [(a1, a2, year, work_id), ...]  已保证 a1 < a2
    """
    batch, = args  # 解包单元素元组
    result = []
    for work_id, authors, year in batch:
        if len(authors) >= 2:
            for a1, a2 in combinations(authors, 2):
                if a1 > a2:
                    a1, a2 = a2, a1
                result.append((a1, a2, year, work_id))
    return result


class AuthorCollaborationEdgeMapper:
    """作者合作关系边映射器（低内存版）"""

    def __init__(self, config_file="mapping_config.json"):
        self.config = self._load_config(config_file)
        self.mapping_config = self.config['author_collaboration_edge_mapping']

        self.input_dir = Path(self.config['input_dir'])
        self.edge_output_dir = Path(self.config['edge_output_dir'])

        self.works_file = self.input_dir / "works.csv"
        self.authorships_file = self.input_dir / "works_authorships.csv"
        self.output_file = self.edge_output_dir / self.mapping_config['output_file']

        self.edge_output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("作者合作边映射器初始化完成")

    @staticmethod
    def _load_config(config_file):
        with open(config_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    # ──────────────────────────────────────────
    # 步骤 1：加载 work_id -> year（精简字典）
    # ──────────────────────────────────────────
    def _load_work_years(self):
        logger.info(f"加载作品年份: {self.works_file}")
        work_years = {}
        total = 0
        try:
            with open(self.works_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    wid = row.get('id', '').strip()
                    yr  = row.get('publication_year', '').strip()
                    if wid and yr:
                        # sys.intern 让相同字符串共享内存
                        work_years[sys.intern(wid)] = sys.intern(yr)
                    total += 1
                    if total % 500_000 == 0:
                        logger.info(f"  已加载 {total:,} 条作品年份...")
        except FileNotFoundError:
            logger.error(f"文件不存在: {self.works_file}")
            return {}
        logger.info(f"  完成: {len(work_years):,} 条有效年份映射")
        return work_years

    # ──────────────────────────────────────────
    # 步骤 2：流式读取 authorships，分块生成合作对，写入临时文件
    # ──────────────────────────────────────────
    def _stream_pairs_to_tmpfiles(self, work_years, tmp_dir):
        """
        流式读取 authorships，每 CHUNK_SIZE 行为一块，
        用进程池并行生成合作对，写入排好序的临时 CSV 文件。
        返回临时文件路径列表。
        """
        logger.info(f"流式处理 authorships: {self.authorships_file}")

        tmp_files = []
        pool = mp.Pool(processes=NUM_WORKERS)

        # 当前块缓冲
        current_work_id = None
        current_authors = []
        chunk_batch = []       # [(work_id, authors, year), ...]
        pair_buffer = []       # 待写合作对
        total_rows = 0
        total_pairs = 0

        def _flush_pair_buffer(buf, is_final=False):
            """将 pair_buffer 排序后写入临时文件"""
            nonlocal tmp_files
            if not buf:
                return
            buf.sort()   # 按 (a1, a2, year, work_id) 字典序排序
            tmp_path = os.path.join(tmp_dir, f"tmp_{len(tmp_files):06d}.csv")
            with open(tmp_path, 'w', encoding='utf-8', newline='') as tf:
                w = csv.writer(tf)
                w.writerows(buf)
            tmp_files.append(tmp_path)
            logger.info(f"  临时文件 #{len(tmp_files)}: {len(buf):,} 对 -> {tmp_path}")
            buf.clear()

        def _process_chunk(batch):
            """提交一块到进程池，收集结果"""
            if not batch:
                return []
            results = pool.map(_worker_generate_pairs, [(batch,)])
            flat = []
            for r in results:
                flat.extend(r)
            return flat

        try:
            with open(self.authorships_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    wid = row.get('work_id', '').strip()
                    aid = row.get('author_id', '').strip()
                    if not wid or not aid:
                        total_rows += 1
                        continue

                    wid = sys.intern(wid)
                    aid = sys.intern(aid)

                    # 按 work_id 分组收集 authors
                    if wid != current_work_id:
                        if current_work_id is not None and len(current_authors) >= 2:
                            yr = work_years.get(current_work_id, '')
                            chunk_batch.append((current_work_id, current_authors, yr))
                        current_work_id = wid
                        current_authors = [aid]
                    else:
                        current_authors.append(aid)

                    total_rows += 1

                    # 每 CHUNK_SIZE 行提交一次进程池
                    if total_rows % CHUNK_SIZE == 0:
                        new_pairs = _process_chunk(chunk_batch)
                        chunk_batch = []
                        pair_buffer.extend(new_pairs)
                        total_pairs += len(new_pairs)
                        logger.info(f"  已处理 {total_rows:,} 行，累计 {total_pairs:,} 对合作关系...")

                        if len(pair_buffer) >= PAIRS_PER_TMPFILE:
                            _flush_pair_buffer(pair_buffer)

                # 处理最后一个 work_id
                if current_work_id is not None and len(current_authors) >= 2:
                    yr = work_years.get(current_work_id, '')
                    chunk_batch.append((current_work_id, current_authors, yr))

                # 处理剩余 chunk
                new_pairs = _process_chunk(chunk_batch)
                pair_buffer.extend(new_pairs)
                total_pairs += len(new_pairs)
                _flush_pair_buffer(pair_buffer, is_final=True)

        finally:
            pool.close()
            pool.join()

        logger.info(f"  authorships 处理完成: {total_rows:,} 行 -> {total_pairs:,} 对，{len(tmp_files)} 个临时文件")
        return tmp_files

    # ──────────────────────────────────────────
    # 步骤 3：多路归并临时文件，聚合相同作者对，流式写出
    # ──────────────────────────────────────────
    def _merge_and_write(self, tmp_files):
        """
        外部归并排序：将所有临时文件（已各自排序）做多路归并，
        聚合相同 (a1, a2) 的所有记录后直接写入输出文件。
        采用分轮归并避免同时打开太多文件句柄。
        """
        logger.info(f"开始外部归并排序: {len(tmp_files)} 个临时文件 -> {self.output_file}")

        # 若临时文件超过 MERGE_FAN_IN，先做多轮合并
        while len(tmp_files) > MERGE_FAN_IN:
            tmp_files = self._merge_round(tmp_files)

        # 最终一轮归并直接写出结果
        written = self._final_merge(tmp_files)
        return written

    def _open_sorted_csv(self, path):
        """打开临时文件，返回迭代器（每行为元组）"""
        fh = open(path, 'r', encoding='utf-8', newline='')
        reader = csv.reader(fh)
        return fh, reader

    def _merge_round(self, tmp_files):
        """将 tmp_files 按 MERGE_FAN_IN 分组，每组合并成一个新临时文件"""
        new_files = []
        groups = [tmp_files[i:i+MERGE_FAN_IN] for i in range(0, len(tmp_files), MERGE_FAN_IN)]
        for g_idx, group in enumerate(groups):
            handles = []
            iterators = []
            for p in group:
                fh, it = self._open_sorted_csv(p)
                handles.append(fh)
                iterators.append(it)

            merged_path = group[0].replace('.csv', f'_m{g_idx}.csv')
            with open(merged_path, 'w', encoding='utf-8', newline='') as out:
                w = csv.writer(out)
                for row in heapq.merge(*iterators):
                    w.writerow(row)

            for fh in handles:
                fh.close()
            for p in group:
                try:
                    os.remove(p)
                except OSError:
                    pass
            new_files.append(merged_path)
            logger.info(f"  归并第 {g_idx+1}/{len(groups)} 组 -> {merged_path}")

        return new_files

    def _final_merge(self, tmp_files):
        """最终归并：聚合并写出结果 CSV"""
        handles = []
        iterators = []
        for p in tmp_files:
            fh, it = self._open_sorted_csv(p)
            handles.append(fh)
            iterators.append(it)

        written = 0
        try:
            with open(self.output_file, 'w', encoding='utf-8', newline='') as out:
                writer = csv.writer(out)
                writer.writerow(['startid', 'endid', 'properties'])

                merged_iter = heapq.merge(*iterators)

                # groupby (a1, a2) 聚合
                for key, group in groupby(merged_iter, key=lambda r: (r[0], r[1])):
                    a1, a2 = key
                    works_list = [{'year': r[2], 'work_id': r[3]} for r in group]
                    properties = json.dumps(
                        {'cnt': len(works_list), 'list': works_list},
                        ensure_ascii=False
                    )
                    writer.writerow([a1, a2, properties])
                    written += 1
                    if written % 200_000 == 0:
                        logger.info(f"  已写入 {written:,} 条边...")
        finally:
            for fh in handles:
                fh.close()
            for p in tmp_files:
                try:
                    os.remove(p)
                except OSError:
                    pass

        return written

    # ──────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────
    def map_to_edge(self):
        logger.info(f"\n{'='*60}")
        logger.info("开始映射: 作者合作关系 -> 边表（低内存外部排序版）")
        logger.info(f"{'='*60}")
        start_time = datetime.now()

        # 步骤 1: 加载 work_id -> year
        work_years = self._load_work_years()
        if not work_years:
            logger.error("作品年份加载失败，终止映射")
            return

        # 使用与输出目录同盘的临时目录，避免跨盘复制
        tmp_dir = tempfile.mkdtemp(dir=self.edge_output_dir, prefix='collab_tmp_')
        logger.info(f"临时目录: {tmp_dir}")

        try:
            # 步骤 2: 流式生成合作对，写入临时文件
            tmp_files = self._stream_pairs_to_tmpfiles(work_years, tmp_dir)
            if not tmp_files:
                logger.warning("未生成任何合作对，检查输入文件")
                return

            # 步骤 3: 外部归并排序 + 聚合 + 写出
            written = self._merge_and_write(tmp_files)

        finally:
            # 清理临时目录
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass

        elapsed = datetime.now() - start_time
        logger.info(f"\n{'='*60}")
        logger.info("映射完成!")
        logger.info(f"  输出文件: {self.output_file}")
        logger.info(f"  边数量:   {written:,}")
        logger.info(f"  耗时:     {elapsed}")
        logger.info(f"{'='*60}")


def main():
    mapper = AuthorCollaborationEdgeMapper()
    mapper.map_to_edge()


if __name__ == '__main__':
    # Windows 多进程必须在 __main__ 内启动
    mp.freeze_support()
    main()
