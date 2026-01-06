Word Count with Hadoop Streaming in Python
===

A word count MapReduce implementation in Python for (unprocessed) large English text using Hadoop Streaming API. This project consists of two core Python scripts:
- A `mapper` that tokenizes raw English text into words and emits key-value pairs (`<word, 1>`).
- A `reducer` that aggregates the counts for each word and outputs the final frequency (`<word, total_count>`).

Quick Start
---

### Test in Local Pipeline

```bash
# Navigate to project directory
cd pyWordCount

# Single-process
cat example/WordCountExample.txt | mapper.py | sort -k1,1 | reducer.py

# Multi-thread
cat example/WordCountExample.txt | mapper_threading.py | sort -k1,1 | reducer_threading.py

# Multi-process
cat example/WordCountExample.txt | mapper_multiprocessing.py | sort -k1,1 | reducer_multiprocessing.py
```

### Run on Hadoop Cluster
1. Upload Input File to HDFS
```bash
# Create HDFS input directory (replace with your preferred path)
hdfs dfs -mkdir -p /test/pyWordCount/input

# Upload local text file to HDFS
hdfs dfs -put example/wordCountExample.txt /test/pyWordCount/input/

# Verify upload
hdfs dfs -ls /test/pyWordCount/input/
```

2. Submit MapReduce Job

```bash
mapred streaming \
  -files example/mapper_commit.py,example/reducer_commit.py\
  -input /test/pyWordCount/input/wordCountExample.txt \
  -output /test/pyWordCount/output \
  -mapper mapper_commit.py \
  -reducer reducer_commit.py
```

Notes
---
Due to the fact that Hadoop Streaming **does not guarantee** that the file structure of the uploaded scripts may not be consistent with the local file structure. This inconsistency can cause the internal reference relationships (e.g., imports of local modules, relative path calls) in the `mapper/reducer` scripts to fail. So it is necessary to **restore all the code** referenced by the scripts back before passing them to Hadoop Streaming (e.g. `example/mapper_commit.py` and `example/reducer_commit.py`).
