#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Works Doc 模式映射脚本
将 works.csv + works_authorships.csv + works_topics.csv 映射为文档模型格式 work_doc.csv
整合文章元数据、作者数组、主题数组到一个JSONB文档中

内存优化方案:
  原始实现将 authorships / topics 全量载入内存 defaultdict，数据量大时 OOM。
  改为"外部排序归并 join"方案:
    1. 对 works_authorships.csv / works_topics.csv 分块读取，
       每块按 work_id 排序后写临时文件，再做多路归并得到全局有序临时文件。
    2. 顺序扫描 works.csv，与上述有序临时文件做流式归并 join，
       每次只在内存中保留当前 work_id 的关联行，写完立刻释放。
  整个过程内存峰值约为单临时文件大小（可通过 SORT_CHUNK_LINES 调节），
  不再受全量数据规模限制。
"""

import csv
import json
import os
import sys
import heapq
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from itertools import groupby

# ──────────────────────────────────────────────
# 可调参数
# ──────────────────────────────────────────────
# 外部排序每块读取行数（调小可降低内存，调大可减少临时文件数）
SORT_CHUNK_LINES = 1_000_000
# 多路归并每轮最多同时打开的文件句柄数
MERGE_FAN_IN = 16
# ──────────────────────────────────────────────

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


# ──────────────────────────────────────────────
# 外部排序工具函数
# ──────────────────────────────────────────────

def _sort_csv_by_key(src_path, key_col, tmp_dir):
    """
    对 src_path（CSV 文件）按 key_col 列做外部排序，
    返回一个全局有序的临时文件路径（调用方负责删除）。
    """
    src_path = Path(src_path)
    tmp_files = []

    # ── 阶段1：分块读取 + 内存排序 + 写临时文件 ──
    with open(src_path, 'r', encoding='utf-8', newline='') as fh:
        reader = csv.DictReader(fh)
        fieldnames = reader.fieldnames

        chunk = []
        total = 0
        for row in reader:
            chunk.append(row)
            total += 1
            if len(chunk) >= SORT_CHUNK_LINES:
                chunk.sort(key=lambda r: r.get(key_col, ''))
                tmp_path = _write_tmp_csv(chunk, fieldnames, tmp_dir, len(tmp_files))
                tmp_files.append(tmp_path)
                chunk = []
                logger.info(f"  [{src_path.name}] 已排序 {total:,} 行 -> 临时文件 #{len(tmp_files)}")

        if chunk:
            chunk.sort(key=lambda r: r.get(key_col, ''))
            tmp_path = _write_tmp_csv(chunk, fieldnames, tmp_dir, len(tmp_files))
            tmp_files.append(tmp_path)
            logger.info(f"  [{src_path.name}] 共 {total:,} 行 -> {len(tmp_files)} 个临时块")

    if not tmp_files:
        return None

    # ── 阶段2：多路归并 ──
    merged = _external_merge(tmp_files, fieldnames, key_col, tmp_dir,
                             prefix=src_path.stem + '_sorted')
    return merged, fieldnames


def _write_tmp_csv(rows, fieldnames, tmp_dir, idx):
    path = os.path.join(tmp_dir, f'chunk_{idx:06d}.csv')
    with open(path, 'w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    return path


def _external_merge(tmp_files, fieldnames, key_col, tmp_dir, prefix='merged'):
    """多路归并：将若干已排序 CSV 归并为单个有序 CSV，返回路径。"""
    round_idx = 0  # 轮次编号，保证每轮产出的中间文件名全局唯一
    while len(tmp_files) > MERGE_FAN_IN:
        new_files = []
        groups = [tmp_files[i:i+MERGE_FAN_IN]
                  for i in range(0, len(tmp_files), MERGE_FAN_IN)]
        for g_idx, grp in enumerate(groups):
            # 用 round_idx + g_idx 组合，确保多轮间文件名不冲突
            out_path = os.path.join(tmp_dir, f'{prefix}_r{round_idx}_g{g_idx}.csv')
            _merge_group(grp, fieldnames, key_col, out_path)
            for p in grp:
                _safe_remove(p)
            new_files.append(out_path)
            logger.info(f"  归并 round={round_idx} group={g_idx}: {len(grp)} 个文件 -> {out_path}")
        tmp_files = new_files
        round_idx += 1

    # 最终一轮
    out_path = os.path.join(tmp_dir, f'{prefix}_final.csv')
    _merge_group(tmp_files, fieldnames, key_col, out_path)
    for p in tmp_files:
        _safe_remove(p)
    logger.info(f"  最终归并完成 -> {out_path}")
    return out_path


def _merge_group(paths, fieldnames, key_col, out_path):
    handles, iters = [], []
    for p in paths:
        fh = open(p, 'r', encoding='utf-8', newline='')
        handles.append(fh)
        iters.append(csv.DictReader(fh))

    with open(out_path, 'w', encoding='utf-8', newline='') as out:
        w = csv.DictWriter(out, fieldnames=fieldnames)
        w.writeheader()
        # heapq.merge 需要可比较的 key，用 (key_value, row) 包装
        tagged = (((r.get(key_col, ''), r) for r in it) for it in iters)
        for _, row in heapq.merge(*tagged, key=lambda x: x[0]):
            w.writerow(row)

    for fh in handles:
        fh.close()


def _safe_remove(path):
    try:
        os.remove(path)
    except OSError:
        pass


def _sorted_csv_iter(path, fieldnames=None):
    """打开已排序 CSV，返回 (文件句柄, DictReader迭代器)"""
    fh = open(path, 'r', encoding='utf-8', newline='')
    reader = csv.DictReader(fh)
    return fh, reader


class WorksDocMapper:
    """Works 到文档模型格式的映射器（低内存外部排序版）"""

    def __init__(self, config_file="mapping_config.json"):
        self.config = self._load_config(config_file)
        self.input_dir = Path(self.config.get('input_dir', '../openalex_merged'))
        self.output_base_dir = Path(self.config.get('doc_output_dir', '../doc'))
        self.output_base_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Works文档模型映射器初始化完成")
        logger.info(f"输入目录: {self.input_dir}")
        logger.info(f"输出目录: {self.output_base_dir}")

    @staticmethod
    def _load_config(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"配置文件 {config_file} 不存在，使用默认配置")
            return {}

    # ──────────────────────────────────────────
    # 将关联文件外部排序为按 work_id 有序的临时文件
    # ──────────────────────────────────────────
    def _prepare_sorted_file(self, src_path, key_col, tmp_dir, label):
        """对 src_path 按 key_col 做外部排序，返回有序临时文件路径。"""
        logger.info(f"\n{'='*60}")
        logger.info(f"外部排序 {label}: {src_path}")
        logger.info(f"{'='*60}")

        if not Path(src_path).exists():
            logger.warning(f"文件不存在，跳过: {src_path}")
            return None, None

        result = _sort_csv_by_key(src_path, key_col, tmp_dir)
        if result is None:
            return None, None
        sorted_path, fieldnames = result
        logger.info(f"  ✓ 排序完成 -> {sorted_path}")
        return sorted_path, fieldnames

    # ──────────────────────────────────────────
    # 从有序关联文件中构建"当前 work_id 的关联行"缓冲
    # ──────────────────────────────────────────
    @staticmethod
    def _make_lookup_iter(sorted_path):
        """
        返回一个生成器：每次 send(work_id) 就返回该 work_id 对应的行列表。
        调用方使用协程风格驱动：
            gen = _make_lookup_iter(path)
            next(gen)               # 启动
            rows = gen.send(wid)    # 获取结果
        """
        if sorted_path is None:
            # 文件不存在，始终返回空列表
            wid = yield
            while True:
                wid = yield []
            return

        fh = open(sorted_path, 'r', encoding='utf-8', newline='')
        reader = csv.DictReader(fh)

        # 预读第一行
        try:
            cur_row = next(reader)
        except StopIteration:
            fh.close()
            wid = yield
            while True:
                wid = yield []
            return

        wid = yield  # 启动，等待第一次 send

        while True:
            if wid is None:
                # 调用方结束，释放资源
                fh.close()
                return

            result = []
            # 跳过 key < wid 的行（关联文件可能有 works.csv 中没有的 work_id）
            while cur_row is not None and cur_row.get('work_id', '') < wid:
                try:
                    cur_row = next(reader)
                except StopIteration:
                    cur_row = None

            # 收集 key == wid 的行
            while cur_row is not None and cur_row.get('work_id', '') == wid:
                result.append(cur_row)
                try:
                    cur_row = next(reader)
                except StopIteration:
                    cur_row = None

            wid = yield result

        fh.close()

    # ──────────────────────────────────────────
    # 将 authorship 原始行转换为文档对象
    # ──────────────────────────────────────────
    @staticmethod
    def _row_to_authorship(row):
        authorship = {}
        if row.get('author_position'):
            authorship['author_position'] = row['author_position']
        author_id   = row.get('author_id', '')
        author_name = row.get('author_display_name', '')
        if author_id or author_name:
            authorship['author'] = {}
            if author_id:
                authorship['author']['id'] = author_id
            if author_name:
                authorship['author']['display_name'] = author_name
        institution_id   = row.get('institution_id', '')
        institution_name = row.get('institution_display_name', '')
        if institution_id or institution_name:
            authorship['institution'] = {}
            if institution_id:
                authorship['institution']['id'] = institution_id
            if institution_name:
                authorship['institution']['display_name'] = institution_name
        return authorship

    # ──────────────────────────────────────────
    # 将 topic 原始行转换为文档对象
    # ──────────────────────────────────────────
    @staticmethod
    def _row_to_topic(row):
        topic = {}
        if row.get('topic_id'):
            topic['id'] = row['topic_id']
        if row.get('topic_display_name'):
            topic['display_name'] = row['topic_display_name']
        if row.get('score'):
            topic['score'] = row['score']
        return topic

    # ──────────────────────────────────────────
    # 解析倒排索引
    # ──────────────────────────────────────────
    @staticmethod
    def _parse_inverted_index(s):
        if not s or not s.strip():
            return None
        try:
            return json.loads(s)
        except Exception:
            return None

    # ──────────────────────────────────────────
    # 核心：流式 join + 写出
    # ──────────────────────────────────────────
    def process_works(self):
        logger.info(f"\n{'='*60}")
        logger.info("开始映射 works -> work_doc.csv（低内存外部排序版）")
        logger.info(f"{'='*60}")

        input_file  = self.input_dir / 'works.csv'
        output_file = self.output_base_dir / 'work_doc.csv'

        if not input_file.exists():
            logger.error(f"输入文件不存在: {input_file}")
            return False

        # 临时目录与输出目录同盘，避免跨盘复制
        tmp_dir = tempfile.mkdtemp(dir=self.output_base_dir, prefix='works_doc_tmp_')
        logger.info(f"临时目录: {tmp_dir}")

        try:
            # ── 步骤1：对两个关联文件做外部排序 ──
            auth_sorted, _ = self._prepare_sorted_file(
                self.input_dir / 'works_authorships.csv', 'work_id', tmp_dir, 'authorships')
            topic_sorted, _ = self._prepare_sorted_file(
                self.input_dir / 'works_topics.csv', 'work_id', tmp_dir, 'topics')

            # ── 步骤2：顺序扫描 works.csv，流式 join ──
            logger.info(f"\n{'='*60}")
            logger.info("流式 join works.csv ...")
            logger.info(f"{'='*60}")

            # 启动协程迭代器
            auth_gen   = self._make_lookup_iter(auth_sorted)
            topic_gen  = self._make_lookup_iter(topic_sorted)
            next(auth_gen)
            next(topic_gen)

            total_count = 0
            error_count = 0
            start_time  = datetime.now()

            with open(input_file, 'r', encoding='utf-8') as inf, \
                 open(output_file, 'w', encoding='utf-8', newline='') as outf:

                reader = csv.DictReader(inf)
                writer = csv.DictWriter(outf, fieldnames=['id', 'doi', 'doc'])
                writer.writeheader()

                batch = []
                BATCH = 50_000

                for i, row in enumerate(reader, 1):
                    try:
                        work_id = row.get('id', '')

                        # 取当前 work_id 对应的关联行（内存中只存这几行）
                        auth_rows  = auth_gen.send(work_id)
                        topic_rows = topic_gen.send(work_id)

                        # 构建 doc 对象
                        doc = {}
                        for field in ('language', 'abstract', 'volume', 'issue',
                                      'first_page', 'last_page'):
                            if row.get(field):
                                doc[field] = row[field]

                        doc['authorships'] = [self._row_to_authorship(r) for r in auth_rows]
                        doc['topics']      = [self._row_to_topic(r)      for r in topic_rows]

                        inv = self._parse_inverted_index(
                            row.get('abstract_inverted_index', ''))
                        if inv:
                            doc['abstract_inverted_index'] = inv

                        batch.append({
                            'id':  work_id,
                            'doi': row.get('doi', ''),
                            'doc': json.dumps(doc, ensure_ascii=False)
                        })

                        if len(batch) >= BATCH:
                            writer.writerows(batch)
                            total_count += len(batch)
                            batch = []
                            if total_count % 500_000 == 0:
                                logger.info(f"  已处理 {total_count:,} 行...")

                    except Exception as e:
                        error_count += 1
                        if error_count <= 10:
                            logger.error(f"  处理第 {i} 行失败: {e}")

                if batch:
                    writer.writerows(batch)
                    total_count += len(batch)

            # 关闭协程
            try:
                auth_gen.send(None)
            except StopIteration:
                pass
            try:
                topic_gen.send(None)
            except StopIteration:
                pass

        finally:
            # 清理临时目录
            try:
                os.rmdir(tmp_dir)
            except OSError:
                pass

        elapsed = datetime.now() - start_time
        logger.info(f"\n{'='*60}")
        logger.info("✓ 映射完成:")
        logger.info(f"  总记录数: {total_count:,}")
        logger.info(f"  错误记录: {error_count:,}")
        if total_count:
            logger.info(f"  成功率:   {(total_count - error_count) / total_count * 100:.2f}%")
        logger.info(f"  耗时:     {elapsed}")
        logger.info(f"  输出文件: {output_file}")
        if output_file.exists():
            logger.info(f"  文件大小: {output_file.stat().st_size / 1024**3:.2f} GB")
        logger.info(f"{'='*60}")
        return True

    def run(self):
        logger.info(f"\n{'#'*80}")
        logger.info("Works 文档模型映射工具")
        logger.info(f"{'#'*80}\n")
        success = self.process_works()
        if success:
            logger.info("\n✓ Works 文档模型映射完成！")
        else:
            logger.error("\n✗ Works 文档模型映射失败")
        return success


def main():
    mapper = WorksDocMapper()
    mapper.run()


if __name__ == '__main__':
    main()
