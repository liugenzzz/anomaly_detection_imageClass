# 数据分桶并行处理指南

数据量大时用 `--shard-index`/`--shard-count` 把数据集切成 N 份，起 N 个独立进程并行跑 `task-classify`。原理和限制见 `PIPELINE.md`；本文件只讲怎么落地执行、怎么盯着跑、怎么收尾。

## 分片怎么分的（先明确一下，避免误解）

`iter_image_paths` 对同一个 `input_dir` 每次都返回同样顺序的排好序的文件列表；分片就是对这个列表按下标取模切片：`image_paths[shard_index::shard_count]`。同一个下标只会落进唯一一个分片，N 个分片的并集是全部文件、交集为空——不会重复，也不会漏。每个分片会自动落在 `<output-dir>/shard_{i}_of_{N}/` 下，`results.csv`/`checkpoint.sqlite3`/`manifest.jsonl`/日志都各自独立，互不干扰，分片之间不需要任何协调。

**分片数不是越多越快**：每个进程里 provider 的并发限流（`max_concurrency`）是进程内独立的，互相不知道对方也在压同一个模型服务。分片前先确认模型服务端能扛住的总并发，别把不稳定的 provider（比如 `max_concurrency=1` 那种）压垮。

## 启动命令

以 4 个分片为例，把 `<你的数据集根目录>` 换成实际路径（比如 `/data/cvdataset/cvdataset-all-labelme/output/layer/final_output`）：

```bash
python run_pipeline.py task-classify --input-dir <你的数据集根目录> --shard-index 0 --shard-count 4 > shard0.out 2>&1 &
python run_pipeline.py task-classify --input-dir <你的数据集根目录> --shard-index 1 --shard-count 4 > shard1.out 2>&1 &
python run_pipeline.py task-classify --input-dir <你的数据集根目录> --shard-index 2 --shard-count 4 > shard2.out 2>&1 &
python run_pipeline.py task-classify --input-dir <你的数据集根目录> --shard-index 3 --shard-count 4 > shard3.out 2>&1 &
```

不用额外传 `--output-dir`，用默认的就行（`outputs/task_type_classification/`），分片会自动在它下面建 `shard_000_of_004/`、`shard_001_of_004/`……四个独立子目录。不用传 `--recursive`，`config.py` 里默认已经是 `True`。第一次跑不用传 `--no-resume`，反正每个分片的输出目录都是全新的，resume 与否没区别。

`> shardN.out 2>&1` 把这个进程的标准输出和标准错误都重定向到文件，`&` 丢到后台，四条命令敲完当前终端就能关掉/断开 SSH 也没事，进程照样在服务器上跑。

## 输出重定向之后，进度条会自动变成普通日志行

`ProgressBar` 内部检测目标流是不是真终端（`isatty()`）。命令行输出被重定向到文件之后，`isatty()` 返回 False，动态刷新的那种单行进度条会自动关闭，退回到原来"每处理 100 张打一行"的普通日志格式：

```
2026-08-26 10:00:00 INFO [task_type_classification] progress processed=1200 skipped_checkpoint=0 failed=3
```

不会有任何 `\r` 乱码问题，这是自动判断的，不用你额外配置。

## 怎么盯着跑

```bash
tail -f shard0.out    # 看某一个分片的实时日志
tail -f shard0.out shard1.out shard2.out shard3.out   # 同时看4个（会交替输出，加个文件名前缀好分辨）
```

重点盯两类日志行：
- `progress processed=... skipped_checkpoint=... failed=...`：进度和失败数。
- `provider disabled after N consecutive failures ...` / `provider cooldown elapsed, probing for recovery` / `provider recovered and re-enabled`：某个 provider（尤其是不稳定的那个）掉线/恢复的记录，四个分片各自独立判断，值得留意是不是同时集中掉线（可能是模型服务本身出问题，而不是单个分片的偶发情况）。

## 数据是流式落盘的，不用等进程结束才有结果

不管有没有重定向到文件，每个分片自己目录下的 `results.csv` 和 `manifest.jsonl`（`materialize_files=False` 时才会生成后者）都是**逐张图片处理完就立刻写入并 `flush()`**，`checkpoint.sqlite3` 也是逐张 `commit()`。这一点跟输出有没有被重定向、进度条是不是打开完全无关——重定向只影响你看到的"人类可读日志"长什么样，不影响背后这两份数据文件的写入方式。

也就是说：
- 想中途看已经跑出多少结果，直接 `wc -l outputs/task_type_classification/shard_000_of_004/results.csv` 或者拿这份文件去分析都行，不用等这个分片跑完。
- 中途 `kill` 掉某个分片进程，已经处理完的图片不会丢，重新用同样的命令跑起来会从它自己的 checkpoint 接着跑，不会重复处理已完成的部分。

## 中断某个分片

后台跑的进程 Ctrl+C 打不到，得用：

```bash
jobs                      # 看后台任务编号
kill %1                   # 按 job 号杀
# 或者
ps aux | grep run_pipeline.py   # 找 PID
kill <PID>
```

杀掉某一个分片不影响其它三个，重新跑同样的 `--shard-index i --shard-count 4` 命令即可继续。

## 跑完之后怎么合并

各分片的 `results.csv`/`manifest.jsonl` 字段结构完全一致，正常情况下直接拼接：

```bash
# results.csv：只保留第一份的表头
head -n 1 outputs/task_type_classification/shard_000_of_004/results.csv > merged_results.csv
for d in outputs/task_type_classification/shard_*_of_004; do
    tail -n +2 "$d/results.csv" >> merged_results.csv
done

# manifest.jsonl 没有表头，直接拼接
cat outputs/task_type_classification/shard_*_of_004/manifest.jsonl > merged_manifest.jsonl
```

**如果你之前已经用旧的单进程版本跑过一部分数据**（比如已经处理过的 23000 条，存在没有分片后缀的 `outputs/task_type_classification/results.csv` 里），这部分图片会在某个分片里被重新处理一遍，合并的时候会出现同一个 `relative_path` 在旧结果和新分片结果里各出现一次的情况，上面这种直接拼接的方式不会自动去重。这种情况在合并前找我，按 `relative_path` 去重一下再合并，避免同一张图片重复计入统计。
