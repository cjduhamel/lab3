#filepaths
INPUT=input/money_data.csv
CLEAN=output/cleaned
MAPOUT=output/amt_by_bank

# Clean stage (map-only with pandas)
echo ">>> Cleaning input..."
hdfs dfs -rm -r -f "$CLEAN" || true
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar" \
  -D mapreduce.job.name="laund-clean" \
  -D mapreduce.job.reduces=0 \
  -files clean.py \
  -mapper "python3 clean.py" \
  -input  "$INPUT" \
  -output "$CLEAN"

# Provide laundered totals by bank
echo ">>> Running mapper/reducer..."
hdfs dfs -rm -r -f "$MAPOUT" || true
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar" \
  -D mapreduce.job.name="laund-laundered-by-bank" \
  -files map.py,reduce.py \
  -mapper  "python3 map.py" \
  -reducer "python3 reduce.py" \
  -input  "$CLEAN" \
  -output "$MAPOUT"

# 3. Preview results
echo ">>> Top 20 results:"
hdfs dfs -cat "$MAPOUT/part-00000" | head -20