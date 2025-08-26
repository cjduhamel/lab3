import sys
header = None
for line in sys.stdin:
    if header is None: #if its the first line, grab the header
        header = line.rstrip("\n").split("\t")
        idx = {col:i for i,col in enumerate(header)} #get index for each column
        continue
    parts = line.rstrip("\n").split("\t") #split into the columns

    #use try except (if error, skip, no time for crashing :) )
    try:
        # key is the receiving bank, if mismatch use GLOBAL
        key = parts[idx["To Bank"]] if "To Bank" in idx and parts[idx["To Bank"]] else "GLOBAL"
        amt = float(parts[idx["Amount Paid"]] ) if "Amount Paid" in idx and parts[idx["Amount Paid"]]  else 0.0
        lab = int(parts[idx["Is Laundering"]] ) if "Is Laundering" in idx and parts[idx["Is Laundering"]]  else 0
        print(f"{key}\t{amt}\t{lab}") #print to stdout
    except Exception:
        continue