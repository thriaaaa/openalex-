# OpenAlex 全自动数据处理流程

完整的 OpenAlex 增量数据处理工具链：下载 -> 展平 -> 去重合并 -> 模式映射，全程只需维护一个配置文件。

## 功能特点

- 从 OpenAlex S3 自动下载增量快照数据（authors / institutions / topics / works）
- 将 JSONL 压缩包展平为 CSV 关系表
- 与全量数据去重合并，输出净增量记录
- 模式映射：自动生成图数据库所需的顶点、文档、边、向量文件（共 11 个子步骤）
- 全流程统一配置：只需编辑 openalex_auto_config.json
- 灵活选步：可任意组合大流程步骤，也可单独运行某个映射子步骤
- 智能日期匹配：指定日期不存在时自动选最近可用日期
- 并行下载 + 断点续传
- GPU 加速向量生成（自动回退 CPU）

## 项目文件结构

    openalex_update_tackle/
    - openalex_auto_config.json      唯一配置文件
    - openalex_auto_workflow.py      全流程主入口（步骤 1~4）
    - run_auto_workflow.bat          Windows 快速启动菜单
    - requirements.txt
    - all-MiniLM-L6-v2/             本地 BERT 模型
    - schema_mapping/
        - run_schema_mapping.py      模式映射调度脚本（可独立运行）
        - 01_map_authors_to_vertex.py
        - 02_map_works_to_vertex.py
        - 03_map_topics_to_vertex.py
        - 04_map_authors_to_doc.py
        - 05_map_works_to_doc.py
        - 06_map_works_topics_to_edge.py
        - 07_map_works_authorships_to_edge.py
        - 08_map_author_collaborations_to_edge.py
        - 09_map_works_referenced_works_to_edge.py
        - 10_map_works_vector.py
        - 11_map_topics_vector.py


## 安装依赖

    pip install -r requirements.txt

## 快速开始

### 方式一：Windows 菜单（推荐）

双击 run_auto_workflow.bat，按提示选择执行模式：

    [1] 全流程执行（下载 -> 展平 -> 合并 -> 映射）
    [2] 只执行模式映射
    [3] 只执行下载+展平+合并
    [4] 自定义命令行参数

### 方式二：命令行

    # 按配置文件执行所有步骤
    python openalex_auto_workflow.py

    # 只执行指定大步骤
    python openalex_auto_workflow.py --steps download flatten merge

    # 只执行模式映射的所有子步骤
    python openalex_auto_workflow.py --steps schema_mapping

    # 只执行模式映射中的向量生成（子步骤 10、11）
    python openalex_auto_workflow.py --steps schema_mapping --mapping-steps 10 11

    # 合并后直接做顶点映射（前三个子步骤）
    python openalex_auto_workflow.py --steps merge schema_mapping --mapping-steps 01 02 03

    # 查看帮助
    python openalex_auto_workflow.py --help

### 方式三：只跑模式映射（独立）

    cd schema_mapping

    # 执行配置中所有启用的子步骤
    python run_schema_mapping.py

    # 只执行指定子步骤（数字前缀或完整 key 均可）
    python run_schema_mapping.py --steps 01 02 03
    python run_schema_mapping.py --steps 10 11

    # 列出所有可用子步骤
    python run_schema_mapping.py --list

## 配置文件说明（openalex_auto_config.json）

### 大流程步骤控制

workflow.steps 控制整体执行哪些阶段，删除不需要的条目即可跳过：

    download        从 OpenAlex S3 下载增量 JSONL 压缩包
    flatten         将 JSONL 展平为 CSV.GZ 关系表
    merge           与全量数据去重，输出净增量 CSV
    schema_mapping  模式映射，生成图数据库所需文件

### download_settings

    start_date          增量起始日期，格式 YYYY-MM-DD
    end_date            增量截止日期，"latest" 表示最新可用日期
    data_types          要下载的类型：authors / institutions / topics / works
    download_dir        原始 JSONL 文件保存目录
    csv_dir             展平后 CSV.GZ 文件保存目录
    parallel_downloads  并行下载线程数（建议 2~8）

### merge_settings

    incremental_dir   展平后的增量 CSV 目录（即 csv_dir）
    full_data_dir     已有全量数据目录（用于去重比对）
    output_dir        去重后净增量输出目录
    temp_dir          临时解压目录
    max_workers       并行线程数
    chunk_size        每批处理行数
    special_file_paths    特定文件指向全量数据中的自定义路径
    column_mappings       全量文件列名与增量列名不一致时的映射
    files_to_process      要处理的 CSV 文件列表

### schema_mapping_settings

    input_dir         去重合并后的数据目录（对应 merge_settings.output_dir）
    output_dir        顶点文件输出目录
    doc_output_dir    文档模型文件输出目录
    edge_output_dir   边文件输出目录
    vector_output_dir 向量文件输出目录
    bert_model_path   本地 BERT 模型路径（all-MiniLM-L6-v2）
    steps             要执行的子步骤列表，删除不需要的条目即可跳过

模式映射子步骤：

    01  01_authors_vertex              Authors 顶点              -> authors_v.csv
    02  02_works_vertex                Works 顶点                -> works_v.csv
    03  03_topics_vertex               Topics 顶点               -> topics_v.csv
    04  04_authors_doc                 Authors 文档              -> author_doc.csv
    05  05_works_doc                   Works 文档                -> work_doc.csv
    06  06_works_topics_edge           Works-Topics 边           -> work_topic_e.csv
    07  07_works_authorships_edge      Works-Authorships 边      -> work_author_e.csv
    08  08_author_collaborations_edge  作者合作关系 边            -> author_author_e.csv
    09  09_works_referenced_works_edge 论文引用关系 边            -> work_referenced_work_e.csv
    10  10_works_vector                Works title+abstract 向量  -> works_vector.csv
    11  11_topics_vector               Topics keywords 向量       -> topics_vector.csv

## 数据流与目录结构

    下载 (downloaded_data/)
      JSONL .gz 原始文件（按 data_type/updated_date= 分目录）

    展平 (csv-files/)
      authors.csv.gz / works.csv.gz / works_authorships.csv.gz 等

    去重合并 (openalex_merged/)
      authors.csv / works.csv / works_authorships.csv 等（净增量）

    模式映射
      graph_vertex/  -> authors_v.csv / works_v.csv / topics_v.csv
      doc/           -> author_doc.csv / work_doc.csv
      graph_edges/   -> work_topic_e.csv / work_author_e.csv
                        author_author_e.csv / work_referenced_work_e.csv
      vector/        -> works_vector.csv / topics_vector.csv

## 日志文件

    openalex_auto_workflow.log              全流程主日志（步骤 1~4）
    schema_mapping/schema_mapping.log       模式映射详细日志

## 注意事项

1. 唯一配置文件：只需编辑 openalex_auto_config.json，不再有其他配置文件
2. full_data_dir：需指向已有全量数据所在目录，否则去重步骤会直接复制增量数据
3. 向量生成：需要 CUDA GPU 且显存不低于 4GB；无 GPU 时自动使用 CPU
4. BERT 模型：all-MiniLM-L6-v2/ 目录需在项目根目录，或在配置中修改 bert_model_path
5. 存储空间：全量 OpenAlex 数据很大，请确保磁盘空间充足
6. 选择性执行：通过 --steps / --mapping-steps 参数，或修改配置中的 steps 数组来跳过不需要的步骤

## 常见问题

Q: 只想跑模式映射，不重新下载数据？
A: python openalex_auto_workflow.py --steps schema_mapping
   或双击 run_auto_workflow.bat 选择 [2]

Q: 模式映射只想重新生成向量？
A: python openalex_auto_workflow.py --steps schema_mapping --mapping-steps 10 11

Q: 如何只处理部分数据类型？
A: 修改 download_settings.data_types，删除不需要的类型。

Q: 去重后没有新数据输出？
A: 增量与全量完全重叠时正常，请检查 full_data_dir 路径是否正确。

Q: 下载中断了怎么办？
A: 直接重新运行，skip_existing_downloads: true 会自动跳过已下载的文件。

Q: 指定的日期在 OpenAlex 上不存在？
A: 脚本会自动选取最接近的可用日期，无需手动调整。

