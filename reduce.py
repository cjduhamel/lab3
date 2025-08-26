import sys

cur_key = None
laundered_sum = 0.0

def produce(k, total):
    if k is not None and total > 0.0:
        print(f"{k}\t{total}")

for line in sys.stdin:
    try: #skip if error (again, dont have time for that)
        key, amt, lab = line.rstrip("\n").split("\t")
        amt = float(amt)
        lab = int(lab)
    except:
        continue

    # mapreduce conveniently gives keys in sorted order, if new key, swap everything
    if key != cur_key and cur_key is not None:
        produce(cur_key, laundered_sum)
        laundered_sum = 0.0

    cur_key = key

    # only add amount if laundering label == 1
    if lab == 1:
        laundered_sum += amt

# flush last key
produce(cur_key, laundered_sum)