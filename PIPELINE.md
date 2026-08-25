# 数据处理模块化流程方案

## 设计原则

- 一级先筛选 `军事异常检测`、`工业设备异常检测`、`飞机螺丝及结构异常检测`、`其它异常`；一级为前三类时，再执行对应的二级分类。
- 二级严格基于一级结果选择候选集：军事包含集结、爆炸、烟雾、越界移动，工业设备包含发热、泄漏、结构变形，飞机包含飞机螺丝缺失、飞机锈蚀；每个目标领域另有 `其它任务类型` 兜底，二级不能跨一级领域选择标签。
- 两级均由当前可访问且返回有效结果的模型等票投票；访问失败、超时、被禁用或响应不可解析的模型不计票。至少需要 `min_valid_provider_count` 个有效模型，且这些模型标签一致、分别超过当前级阈值才接受；有效模型分歧、数量不足或任一有效模型未过阈值时执行对应级兜底。
- 模型分类时不向模型传入图片名称、路径名称或目录名称，只传入图片内容和当前级分类定义。
- 文件名只作为本地文件定位索引，用于读取原图、写结果、复制到分类目录。
- 每个阶段可以单独执行，后续细粒度分类、清洗、质检、导出等处理应继续扩展为独立 stage。

## 代码结构

```text
config.py
pipeline/
  core/
    image_io.py          # 图片发现、文件头 MIME 识别、base64 编码
    model_client.py      # OpenAI-compatible chat/completions 调用
  fusion/
    unanimous.py         # 全模型一致投票
  stages/
    task_type/
      prompts.py         # 一级与二级分类提示词
      classifier.py      # 两级分类执行模块
      organizer.py       # 按一级/二级标签整理图片
  utils/
    json_utils.py        # 模型 JSON 输出解析与规范化
run_pipeline.py          # 统一阶段入口
```

## 当前可运行阶段

默认完整流程先执行两级分类，再按分类结果整理目录：

```bash
python run_pipeline.py
```

等价于：

```bash
python run_pipeline.py --run-stages all
```

可用以下进行执行

```bash
python run_pipeline.py --input-dir ./data --classification-output-dir ./outputs/task_type_classification --organization-output-dir ./outputs/data_by_task
```

通过参数选择只执行某一阶段：

```bash
python run_pipeline.py --run-stages classify
python run_pipeline.py --run-stages organize
```

数据集分散在多个子文件夹（比如 `cvdata_clean001/`、`cvdata_clean002/`、...）时，需要递归扫描：

```bash
python run_pipeline.py --input-dir /path/to/dataset_root --recursive
```

`DATA_CONFIG["recursive"]` 默认已经是 `True`（会递归扫描 `input_dir` 下所有子目录）；只有当图片明确都在 `input_dir` 顶层、且子目录里有其它不想被扫描的内容时才需要显式传 `--no-recursive` 关掉。

扫描时按固定后缀名（`.jpg/.jpeg/.png/.webp/.gif/.bmp`，大小写不敏感）预过滤，目录里混杂的 json/tar.gz/脚本等非图片文件不会被读取和计算哈希，也不会计入统计总数。

数据量大、想用多个进程/多台机器并行处理时，用 `--shard-index`/`--shard-count` 把数据集切成 N 份（对已排序的完整路径列表按下标取模，纯本地计算，不需要真的搬动/复制文件，各分片之间也不用互相协调）：

```bash
# 起 4 个进程各处理 1/4 的数据，各自独立的 output-dir/checkpoint/results.csv/manifest.jsonl
python run_pipeline.py task-classify --input-dir /path/to/dataset_root --shard-index 0 --shard-count 4 &
python run_pipeline.py task-classify --input-dir /path/to/dataset_root --shard-index 1 --shard-count 4 &
python run_pipeline.py task-classify --input-dir /path/to/dataset_root --shard-index 2 --shard-count 4 &
python run_pipeline.py task-classify --input-dir /path/to/dataset_root --shard-index 3 --shard-count 4 &
```

每个分片会落在 `<output-dir>/shard_{i}_of_{N}/` 下，互相隔离。**分片数量不是越多越快**——各分片进程里的 provider 并发限流（`max_concurrency`）是进程内独立的，不会互相感知，多个分片同时压向同一个并发受限的 provider（比如本项目里 `fx_q3_235` 被设成 `max_concurrency=1`）反而更容易把它打挂、触发更多失败。分片前先确认模型服务端能扛住的总并发，再决定分几片、每片里各 provider 的 `max_concurrency` 设多少。跑完之后如果需要一份合并的 `results.csv`，各分片 schema 一致，直接 `cat` 起来去掉多余表头即可；`manifest.jsonl` 没有表头，直接 `cat` 拼接。

从外部 JSON 动态加载模型池：

```bash
python run_pipeline.py --provider-config providers.json
```

只启用部分模型：

```bash
python run_pipeline.py --providers InternVL3_5-38B,Qwen3.6-27B
```

关闭断点续跑并重建分类输出：

```bash
python run_pipeline.py --run-stages classify --no-resume
```

当前结果结构版本为 `two_level_available_provider_vote_csv_v1`。首次使用有效模型投票和 CSV 主输出，或修改标签/提示词定义后，必须增加 `--no-resume` 重新生成结果，避免新旧结果混用。

历史子命令仍然保留，适合明确单独运行某个模块：

```bash
python run_pipeline.py task-classify
python run_pipeline.py task-organize
```

检查图片读取和输出写入，不调用模型：

```bash
python run_pipeline.py task-classify --dry-run
python run_pipeline.py --run-stages all --dry-run
```

调用模型执行两级分类：

```bash
python run_pipeline.py task-classify
```

把图片按一级/二级标签复制到不同文件夹：

```bash
python run_pipeline.py task-organize
```

默认整理输出：

```text
data_by_task/
  军事异常检测/
    集结/
    爆炸/
    烟雾/
    越界移动/
    其它任务类型/
  工业设备异常检测/
    发热/
    泄漏/
    结构变形/
    其它任务类型/
  飞机螺丝及结构异常检测/
    飞机螺丝缺失/
    飞机锈蚀/
    其它任务类型/
  其它异常/
  _未分类/
  manifest.jsonl
  summary.json
```

## 配置

模型端点、模型名、API Key、超时、并发、标签映射和输入输出目录都集中在 `config.py`。

`config.py` 只保存通用常数和处理参数，不保存任何样本文件名到分类标签的映射。百万级数据必须通过模型分类结果文件、外部标注文件或数据库记录驱动后续处理。

当前投票规则：

```text
每个有效模型一票；访问失败的模型不计票
第一条件：有效模型数量 >= min_valid_provider_count，且所有有效模型标签一致
第二条件：一级所有有效模型分数均 > level1_score_threshold；二级所有有效模型分数均 > level2_score_threshold
同时满足两个条件：接受目标标签
标签不一致、任一模型无有效结果或任一分数未超过当前级阈值：一级归入其它异常，二级归入其它任务类型
模型分数等权平均后保存，仅用于审计，不改变投票结果
```

## 大规模处理能力

当前两级分类模块面向百万级图片做了以下处理：

- 断点续跑：使用 `checkpoint.sqlite3` 记录已完成图片，重复运行时自动跳过。
- 流式输入：逐个遍历图片路径，不把所有图片数据一次性读入内存。
- 流式 CSV 输出：每完成一张图片就直接追加写入 `results.csv`，不生成 `results.jsonl`，也不在内存中保存全量 records。
- 精简审计：CSV 保留两级分数、独立阈值、阈值状态及压缩后的逐模型投票/分数字段；默认不保存模型原始长响应。
- 图片级并发：`image_max_workers` 控制同时处理的图片数。
- 模型级并发：每个 provider 使用自身 `max_concurrency` 限流，避免压垮单个模型服务。
- 动态模型池：可用 `--provider-config` 从外部 JSON 加载模型配置，或用 `--providers` 选择子集。
- 模型故障隔离：单个模型调用失败不会阻断批处理，也不会参与本次投票；只要其余有效模型达到最小数量、标签一致并通过阈值，仍可接受分类。某模型连续失败达到阈值后会被临时禁用，禁用满 `provider_reenable_cooldown_seconds` 后自动放行一次探测请求；探测成功则恢复参与投票，失败则重新计时继续禁用，不会在整轮任务中永久失效。
- 日志监控：每个阶段在输出目录写入 `<stage>.log`，同时输出关键进度到控制台。

相关配置项在 `config.py`：

```text
TASK_CLASSIFICATION_CONFIG["image_max_workers"]
TASK_CLASSIFICATION_CONFIG["max_provider_workers_per_image"]
TASK_CLASSIFICATION_CONFIG["require_all_selected_providers"]
TASK_CLASSIFICATION_CONFIG["min_valid_provider_count"]
TASK_CLASSIFICATION_CONFIG["level1_score_threshold"]
TASK_CLASSIFICATION_CONFIG["level2_score_threshold"]
TASK_CLASSIFICATION_CONFIG["disable_provider_after_consecutive_failures"]
TASK_CLASSIFICATION_CONFIG["provider_reenable_cooldown_seconds"]
TASK_CLASSIFICATION_CONFIG["resume"]
TASK_CLASSIFICATION_CONFIG["checkpoint_file"]
TASK_CLASSIFICATION_CONFIG["progress_log_interval"]
TASK_ORGANIZATION_CONFIG["progress_log_interval"]
```

外部模型池 JSON 可以是列表，也可以是包含 `providers` 字段的对象：

```json
{
  "providers": [
    {
      "name": "InternVL3_5-38B",
      "enabled": true,
      "url": "http://192.168.78.35:8879/intern-vl/v1/chat/completions",
      "model": "InternVL3_5-38B",
      "api_key": "",
      "stream": false,
      "temperature": 0.6,
      "max_tokens": 8192,
      "timeout": 2400,
      "chat_template_kwargs": {},
      "capabilities": ["text", "image"],
      "task_types": ["军事异常检测", "工业设备异常检测", "飞机螺丝及结构异常检测", "其它异常"],
      "weight": 3,
      "max_concurrency": 6
    }
  ]
}
```

外部模型配置中的 `weight` 字段为兼容旧配置而保留；当前一致投票不会用它改变票数或最终标签。

## 分类整理说明

`task-organize` 流式读取 `outputs/task_type_classification/results.csv` 中每张图片的 `best_task_type` 和 `best_anomaly_type`，按两级标签复制图片。该阶段不重新判断图片内容，也不读取文件名前缀作为类别；历史 JSONL 仅保留只读兼容能力。

`TASK_ORGANIZATION_CONFIG["materialize_files"]` 默认为 `True`（按 `copy_files` 拷贝或移动原图）。数据量很大时可设为 `False`：不再拷贝/移动任何文件，只把 `relative_path -> best_task_type/best_anomaly_type` 的映射写入 `manifest.jsonl`（每条记录的 `action` 为 `manifest_only`），下游按 `relative_path` 回原始目录取图即可，避免整份数据集重复落盘。

**`materialize_files=False` 时不需要再手动跑 `task-organize`**：`task-classify` 会在每张图片分类完成的同时，直接把这条映射写进 `<classification_output_dir>/manifest.jsonl`（跟 `results.csv` 同目录、同样逐条 flush，断点续跑时也是追加而不是重写），字段和独立跑 `task-organize` 产出的完全一致。默认全流程（`python run_pipeline.py`，即 `--run-stages all`）检测到这个配置时会自动跳过 organize 阶段（`summaries.task_organize.reason` 是 `manifest_already_written_inline_by_classify`），不会重复读一遍 `results.csv` 白跑一次。分片模式（见上面 `--shard-index`/`--shard-count`）下每个分片各自产出自己的 `manifest.jsonl`，合并方式同 `results.csv`，直接 `cat` 拼接。

如果 `materialize_files=True`（要真的复制/移动文件），仍然需要单独的 `task-organize` 阶段来做这部分 IO——真实文件搬运不适合塞进分类阶段的网络并发线程池里，行为跟以前一样不变。

标准流程是直接运行 `python run_pipeline.py`。该默认流程会先运行 `task-classify` 生成模型分类结果，`materialize_files=True` 时再运行 `task-organize` 做目录整理；`materialize_files=False` 时 organize 会被自动跳过（见上）。

如果使用 `--dry-run`，分类阶段只检查输入输出链路，不产生可用标签、也不写 `manifest.jsonl`；默认完整流程会自动跳过整理阶段。

整理阶段输出：

```text
data_by_task/
  军事异常检测/
    集结/
    爆炸/
    烟雾/
    越界移动/
    其它任务类型/
  工业设备异常检测/
    发热/
    泄漏/
    结构变形/
    其它任务类型/
  飞机螺丝及结构异常检测/
    飞机螺丝缺失/
    飞机锈蚀/
    其它任务类型/
  其它异常/
  _未分类/
  manifest.jsonl
  summary.json
  task_type_organization.log
```
