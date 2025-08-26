#filepaths
INPUT=input/money_data.csv
CLEAN=output/cleaned
MAPOUT=output/amt_by_bank

PYBIN="$(command -v python3)"
PYUSER="$($PYBIN -c 'import site; print(site.getusersitepackages())')"
echo "$PYBIN"
echo "$PYUSER"

# Clean stage (map-only with pandas)
echo ">>> Cleaning input..."
hdfs dfs -rm -r -f output/cleaned || true
hadoop jar "$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-3.4.1.jar" \
  -D mapreduce.job.name="laund-clean" \
  -D mapreduce.job.reduces=0 \
  -files clean.py \
  -cmdenv PYTHONPATH="$PYUSER" \
  -mapper "$PYBIN clean.py" \
  -input  input/money_data.csv \
  -output output/cleaned

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