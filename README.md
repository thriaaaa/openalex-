# OpenAlex 全自动数据处理流水线 (OpenAlex Automated Data Pipeline)

本项目是一个专门用于自动化处理 **[OpenAlex](https://openalex.org/) 开放学术知识图谱数据** 的完整工具链。涵盖了从增量数据下载、JSONL数据展平、全量数据去重合并，到最终图数据库、文档数据库的模式映射的全流程端到端解决方案。

## ✨ 核心特性

- 🚀 **全链路自动化**：支持 下载 (Download) -> 展平 (Flatten) -> 去重合并 (Merge) -> 模式映射 (Schema Mapping) 完整 4 步无缝衔接。
- 🛠️ **智能增量管理**：支持自定义起始/截止日期、断点续传、多线程并行下载，自动匹配 OpenAlex S3 的最新更新片段。
- 🗃️ **去重与合并机制**：针对下载的增量 CSV 数据与本地全量数据集进行主键 (id) 交叉比对，精准输出**净增量记录**。
- 🕸️ **多数据库协同映射**：基于一套数据源，能同时洗出适用不同检索和图算法场景的定制结构表：
  - **Graph Vertex (图顶点)**: authors_v.csv, works_v.csv, topics_v.csv
  - **Graph Edges (图边)**: work_topic_e.csv, work_author_e.csv, author_author_e.csv 等
  - **Document (文档模型)**: author_doc.csv, work_doc.csv
- 🧠 **文本向量化提取**：内嵌本地轻量级离线 BERT 模型引擎 (ll-MiniLM-L6-v2)，在映射步骤中对标题、摘要及关键词等字段生成高维特征向量 (384维度)，并可按需支持 GPU 显存自动调度。
- ⚙️ **统一化配置**：告别零散参数，整个系统的一切行为通过配置 openalex_auto_config.json 进行管控。
- 🧩 **模块化按需执行**：支持完整一条龙运行，也支持通过 CLI 参数单独运行某一个特定功能的细分脚本（比如只重新生成论文向量特征）。

## 📁 核心文件结构

`	ext
├── openalex_auto_config.json      # 项目唯一核心配置文件
├── openalex_auto_workflow.py      # 全流程主入口脚本（整合了下载、展平、去重和映射的调度）
├── run_auto_workflow.bat          # Windows 快捷一键执行菜单
├── requirements.txt               # 第三方 Python 依赖
├── openalex_downloader.py         # S3 增量包检索及下载模块
├── flatten.py                     # JSONL 转 CSV 逻辑解析模块
├── merge_incremental.py           # 增量去重比对模块
├── generate_vector.py             # 向量计算引擎模块
└── schema_mapping/                # 图计算与文档模式的核心清洗映射逻辑库 (包含11个步骤)
    ├── run_schema_mapping.py      # 映射总调度器 
    ├── 01_map_authors_to_vertex.py
    └── ... (其余 10 个特定表或图结构的清洗映射脚本)
`

## 🚀 快速开始

### 1. 环境依赖安装
确保电脑上安装了 Python 3.8+ 版本环境，然后通过以下命令安装必要依赖：

`ash
git clone https://github.com/thriaaaa/openalex-.git
cd openalex-
pip install -r requirements.txt
`

### 2. 参数项配置
复制或打开 openalex_auto_config.json 进行对应修改：
- download_settings: 用于设置 start_date 和 data_types (下载的表名)。
- merge_settings: 定义了生成的增量文件以及进行除重的 ull_data_dir。
- schema_mapping_settings: 所有文件格式映射的步骤开关以及源数据和映射路径等。

### 3. 一键运行 
**方式一：Windows 用户（推荐）**
直接双击项目的 un_auto_workflow.bat，即可按照交互层指引操作：
`	ext
[1] 执行全套流水线任务 (Download -> Flatten -> Merge -> Mapping)
[2] 仅执行模式数据映射 (Mapping)
[3] 跳过映射，只生成净增量 (Download -> Flatten -> Merge)
[4] 自定义指定任意步骤组合 
`

**方式二：全端命令行模式**
您可以利用强大的内置 CLI 工具，自由度更高：

`ash
# 执行配置文件记录的全部步骤
python openalex_auto_workflow.py

# 只执行下载、展平、合并步骤（忽略配置里的映射）
python openalex_auto_workflow.py --steps download flatten merge

# 仅指定其中的某几项映射逻辑重新执行（比如我想单独刷新一边作者合作的图谱结构边表和所有的向量数据）
python openalex_auto_workflow.py --steps schema_mapping --mapping-steps 08 10 11
`

## 📝 常见问题 (FAQ)
- **Q: 提示显存 OOM 或 GPU 不可用怎么办？** 
  A: 系统会在执行 schema_mapping 里的 10_works_vector 时对本地硬件自审。无需人工干预或配置，若无 GPU 或显存吃紧，系统会自动把推断切回纯 CPU 计算以兜底。

- **Q: 增量拉取途中网断了？** 
  A: 本系统配置了可靠的断点续传功能（需在配置表里确保 skip_existing_downloads 为 true），重新执行后会自动从出错的 gz 压缩包分片处继续。

## 📄 许可证
本项目采用 [MIT License](LICENSE) 授权自由使用与二次创作。