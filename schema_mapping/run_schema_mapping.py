#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Schema Mapping 全流程调度脚本
支持一次全执行，也支持只执行某一个或某几个子步骤。

用法（独立运行）:
  python run_schema_mapping.py                     # 按配置文件执行所有子步骤
  python run_schema_mapping.py --steps 01 02 03   # 只执行指定编号的步骤
  python run_schema_mapping.py --list              # 列出所有可用步骤

配置来源:
  优先读取上级目录的 openalex_auto_config.json（整合配置），
  若不存在则回退到同目录的 mapping_config.json。
"""

import sys
import os
import json
import argparse
import logging
from pathlib import Path
from datetime import datetime

# ── 确保能 import 同目录的各映射器 ──────────────────────────────────────────
_THIS_DIR = Path(__file__).resolve().parent
if str(_THIS_DIR) not in sys.path:
    sys.path.insert(0, str(_THIS_DIR))

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


# ── 步骤注册表 ────────────────────────────────────────────────────────────────
# 每条: (步骤key, 描述, 映射器模块, 映射器类名, 调用方法名)
STEP_REGISTRY = [
    ("01_authors_vertex",             "Authors → 顶点",         "01_map_authors_to_vertex",           "AuthorsVertexMapper",           "run"),
    ("02_works_vertex",               "Works → 顶点",           "02_map_works_to_vertex",             "WorksVertexMapper",             "run"),
    ("03_topics_vertex",              "Topics → 顶点",          "03_map_topics_to_vertex",            "TopicsVertexMapper",            "run"),
    ("04_authors_doc",                "Authors → 文档",         "04_map_authors_to_doc",              "AuthorsDocMapper",              "run"),
    ("05_works_doc",                  "Works → 文档",           "05_map_works_to_doc",                "WorksDocMapper",                "run"),
    ("06_works_topics_edge",          "Works-Topics → 边",      "06_map_works_topics_to_edge",        "WorksTopicsEdgeMapper",         "run"),
    ("07_works_authorships_edge",     "Works-Authorships → 边", "07_map_works_authorships_to_edge",   "WorksAuthorshipsEdgeMapper",    "run"),
    ("08_author_collaborations_edge", "作者合作 → 边",           "08_map_author_collaborations_to_edge","AuthorCollaborationEdgeMapper", "map_to_edge"),
    ("09_works_referenced_works_edge","论文引用 → 边",           "09_map_works_referenced_works_to_edge","WorkReferencedWorksEdgeMapper","map_to_edge"),
    ("10_works_vector",               "Works → 向量",           "10_map_works_vector",                "WorksVectorMapper",             "run"),
    ("11_topics_vector",              "Topics → 向量",          "11_map_topics_vector",               "TopicsVectorMapper",            "run"),
]

# 数字前缀 → 步骤key 的快速查找
_PREFIX_MAP = {key.split("_")[0]: key for key, *_ in STEP_REGISTRY}


def _step_key_from_token(token: str) -> str | None:
    """将用户输入的步骤标识（数字前缀或完整key）解析为注册表中的key"""
    token = token.strip()
    if token in _PREFIX_MAP:
        return _PREFIX_MAP[token]
    # 完整 key 或部分匹配
    for key, *_ in STEP_REGISTRY:
        if token == key or key.startswith(token):
            return key
    return None


def _load_mapping_config(base_config: dict) -> dict:
    """从整合配置中提取 schema_mapping_settings，生成 mapping_config 格式"""
    sm = base_config.get("schema_mapping_settings", {})
    if not sm:
        return base_config   # 已经是 mapping_config 格式，直接返回

    # 把 schema_mapping_settings 的顶层路径字段映射到 mapping_config 键名
    mapping = {
        "input_dir":        sm.get("input_dir", "./openalex_merged"),
        "output_dir":       sm.get("output_dir", "./graph_vertex"),
        "doc_output_dir":   sm.get("doc_output_dir", "./doc"),
        "edge_output_dir":  sm.get("edge_output_dir", "./graph_edges"),
        "vector_output_dir": sm.get("vector_output_dir", "./vector"),
        "bert_model_path":  sm.get("bert_model_path", "./all-MiniLM-L6-v2"),
    }
    # 把各 mapping 子配置原样搬入
    for k, v in sm.items():
        if k not in mapping and k not in ("steps", "comment_steps"):
            mapping[k] = v
    return mapping


def _resolve_config_path(config_file: str | None) -> tuple[Path, bool]:
    """
    返回 (配置文件路径, 是否整合配置)
    搜索顺序:
      1. 用户显式指定
      2. 上级目录 openalex_auto_config.json
      3. 当前目录 mapping_config.json
    """
    if config_file:
        p = Path(config_file)
        is_integrated = "openalex_auto_config" in p.name
        return p, is_integrated

    integrated = _THIS_DIR.parent / "openalex_auto_config.json"
    if integrated.exists():
        return integrated, True

    local = _THIS_DIR / "mapping_config.json"
    return local, False


class SchemaMappingRunner:
    """Schema Mapping 全流程调度器"""

    def __init__(self, config_file: str | None = None):
        config_path, is_integrated = _resolve_config_path(config_file)
        logger.info(f"加载配置: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw_config = json.load(f)

        self.mapping_config = _load_mapping_config(raw_config)

        # 从整合配置读取启用的子步骤列表
        if is_integrated:
            sm_cfg = raw_config.get("schema_mapping_settings", {})
            self.enabled_steps = sm_cfg.get("steps", [key for key, *_ in STEP_REGISTRY])
        else:
            # 旧版 mapping_config.json 没有 steps 字段，默认全开
            self.enabled_steps = [key for key, *_ in STEP_REGISTRY]

        # mapping_config.json 的路径（供各映射器读取）
        # 将整合后的 mapping_config 写到临时文件，或直接传字典
        self._mapping_cfg_path = config_path if not is_integrated else None

    def _get_mapper_config_arg(self) -> str:
        """
        各映射器构造函数需要 config_file 路径。
        若使用整合配置，把提取出的 mapping_config 字典写入临时文件后返回路径。
        """
        if self._mapping_cfg_path:
            return str(self._mapping_cfg_path)

        # 写临时 mapping_config.json
        tmp_path = _THIS_DIR / "_tmp_mapping_config.json"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(self.mapping_config, f, ensure_ascii=False, indent=2)
        return str(tmp_path)

    def _cleanup_tmp(self):
        tmp = _THIS_DIR / "_tmp_mapping_config.json"
        if tmp.exists():
            tmp.unlink()

    def run_steps(self, steps_to_run: list[str] | None = None):
        """
        执行指定的步骤列表。
        steps_to_run=None 时执行配置文件中启用的全部步骤。
        """
        if steps_to_run is None:
            steps_to_run = self.enabled_steps

        # 解析成规范 key
        resolved = []
        for token in steps_to_run:
            key = _step_key_from_token(token)
            if key is None:
                logger.warning(f"未知步骤标识: '{token}'，已跳过")
            else:
                resolved.append(key)

        if not resolved:
            logger.error("没有有效的步骤可执行，退出")
            return False

        cfg_arg = self._get_mapper_config_arg()
        total = len(resolved)
        success_count = 0
        overall_start = datetime.now()

        logger.info("\n" + "=" * 80)
        logger.info(f"Schema Mapping 开始，共 {total} 个步骤")
        logger.info(f"步骤列表: {resolved}")
        logger.info("=" * 80)

        for idx, step_key in enumerate(resolved, 1):
            # 从注册表查找
            entry = next((e for e in STEP_REGISTRY if e[0] == step_key), None)
            if entry is None:
                logger.warning(f"[{idx}/{total}] 注册表中找不到步骤: {step_key}，跳过")
                continue

            _, desc, module_name, class_name, method_name = entry

            logger.info(f"\n{'#' * 80}")
            logger.info(f"[{idx}/{total}] {step_key}  —  {desc}")
            logger.info(f"{'#' * 80}")

            step_start = datetime.now()
            try:
                import importlib
                mod = importlib.import_module(module_name)
                cls = getattr(mod, class_name)
                instance = cls(config_file=cfg_arg)
                method = getattr(instance, method_name)
                result = method()

                elapsed = datetime.now() - step_start
                if result is False:
                    logger.error(f"✗ [{idx}/{total}] {desc} 失败，耗时 {elapsed}")
                else:
                    logger.info(f"✓ [{idx}/{total}] {desc} 完成，耗时 {elapsed}")
                    success_count += 1

            except Exception as e:
                elapsed = datetime.now() - step_start
                logger.error(f"✗ [{idx}/{total}] {desc} 异常: {e}，耗时 {elapsed}",
                             exc_info=True)

        self._cleanup_tmp()

        overall_elapsed = datetime.now() - overall_start
        logger.info("\n" + "=" * 80)
        logger.info(f"Schema Mapping 结束: {success_count}/{total} 步骤成功")
        logger.info(f"总耗时: {overall_elapsed}")
        logger.info("=" * 80)

        return success_count == total


def list_steps():
    """打印所有可用步骤"""
    print("\n可用的 Schema Mapping 步骤：")
    print(f"  {'编号':<6} {'步骤Key':<35} {'说明'}")
    print("  " + "-" * 65)
    for key, desc, *_ in STEP_REGISTRY:
        prefix = key.split("_")[0]
        print(f"  {prefix:<6} {key:<35} {desc}")
    print()
    print("使用示例:")
    print("  python run_schema_mapping.py                      # 执行配置中启用的全部步骤")
    print("  python run_schema_mapping.py --steps 01 02 03     # 只执行01-03")
    print("  python run_schema_mapping.py --steps 10 11        # 只跑向量生成")
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Schema Mapping 全流程调度脚本",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_schema_mapping.py                     执行配置文件中所有启用步骤
  python run_schema_mapping.py --steps 01 02 03   只执行前三步（顶点映射）
  python run_schema_mapping.py --steps 10 11      只执行向量生成
  python run_schema_mapping.py --list             列出所有步骤
  python run_schema_mapping.py --config my.json  指定配置文件路径
        """
    )
    parser.add_argument("--steps", nargs="+", metavar="STEP",
                        help="指定要执行的步骤，可用数字前缀(01)或完整key(01_authors_vertex)")
    parser.add_argument("--list", action="store_true",
                        help="列出所有可用步骤后退出")
    parser.add_argument("--config", metavar="FILE",
                        help="指定配置文件路径（默认自动查找 openalex_auto_config.json）")
    args = parser.parse_args()

    if args.list:
        list_steps()
        return

    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         Schema Mapping 全流程调度                         ║
    ║                                                          ║
    ║  功能：顶点 / 文档 / 边 / 向量 映射                       ║
    ║  配置：openalex_auto_config.json                         ║
    ╚══════════════════════════════════════════════════════════╝
    """)

    try:
        runner = SchemaMappingRunner(config_file=args.config)
        success = runner.run_steps(steps_to_run=args.steps)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\n用户中断")
        sys.exit(1)
    except Exception as e:
        logger.error(f"发生错误: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
