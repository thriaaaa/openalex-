import csv
import json
import argparse
import time
from sentence_transformers import SentenceTransformer

def reconstruct_abstract(abstract_inverted_index_str):
    """根据 inverted index 还原摘要"""
    if not abstract_inverted_index_str or abstract_inverted_index_str.strip() == "":
        return None
    try:
        abstract_inverted_index = json.loads(abstract_inverted_index_str)
    except json.JSONDecodeError:
        print(f"[警告] 无法解析 JSON 字符�? {abstract_inverted_index_str}")
        return None

    abstract_index = {}
    for word, positions in abstract_inverted_index.items():
        for position in positions:
            abstract_index[position] = word

    return ' '.join(abstract_index[pos] for pos in sorted(abstract_index.keys()))

def process_data(input_csv, output_csv, bert_model_path):
    print(f"[INFO] 加载 BERT 模型: {bert_model_path}")
    model = SentenceTransformer(bert_model_path)
    print(f"[INFO] BERT 模型加载完成")

    print(f"[INFO] 开始处理文�? {input_csv}")
    start_time = time.time()

    with open(input_csv, mode='r', encoding='utf-8') as infile, \
         open(output_csv, mode='w', encoding='utf-8', newline='') as outfile:

        reader = csv.DictReader(infile)
        fieldnames = ["work_id", "doi", "vec"]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        processed_count = 0
        for row in reader:
            try:
                work_id = row["id"].strip()
                doi = row["doi"].strip()
                title = row["title"].strip() if "title" in row else ""
                abstract = reconstruct_abstract(row["abstract_inverted_index"]) if "abstract_inverted_index" in row else ""
                combined_text = f"{title} {abstract}".strip()

                # 生成文本嵌入向量128维度向量
                embedding = model.encode(combined_text)[:128].tolist()

                # 写入�?CSV
                writer.writerow({
                    "work_id": work_id,
                    "doi": doi,
                    "vec": json.dumps(embedding, ensure_ascii=False)
                })

                processed_count += 1
                if processed_count % 500 == 0:
                    elapsed_time = time.time() - start_time
                    print(f"[INFO] 已处�?{processed_count} 行数据，耗时 {elapsed_time:.2f} �?)

            except Exception as e:
                print(f"[ERROR] 处理�?{processed_count + 1} 行时出错: {e}")

    total_time = time.time() - start_time
    print(f"[INFO] 任务完成，文件已保存�?{output_csv}")
    print(f"[INFO] 总处理行�? {processed_count}, 总耗时: {total_time:.2f} �?)
    print("[INFO] 处理完成，程序退出�?)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="处理 works.csv 生成 works_vector.csv")
    parser.add_argument("input_csv", help="输入 CSV 文件路径")
    parser.add_argument("output_csv", help="输出 CSV 文件路径")
    parser.add_argument("bert_model_path", help="本地 BERT 模型路径")
    # 推荐使用sentence-transformers/all-MiniLM-L6-v2的模型（384维度），但应项目需求，需要裁剪为128维度的向量网址如下�?    # https://public.ukp.informatik.tu-darmstadt.de/reimers/sentence-transformers/v0.2/all-MiniLM-L6-v2.zip
    
    args = parser.parse_args()
    process_data(args.input_csv, args.output_csv, args.bert_model_path)


# 示例命令：python generate_vector.py <输入works.csv文件路径> <输出works_vector.csv文件路径> <本地向量模型路径>
# python generate_vector.py /path/to/input.csv /path/to/output.csv /path/to/all-MiniLM-L6-v2/
