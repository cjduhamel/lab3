#!/usr/bin/env python3
import sys, io
import pandas as pd

BATCH_SIZE = 20000  # be generous; batching by 1000 lines is tiny

COL_AP = "Amount Paid"
COL_AR = "Amount Received"
COL_TS = "Timestamp"  # optional; we drop it

def process_batch(df: pd.DataFrame, write_header: bool) -> bool:
    # drop duplicates and reset index
    df = df.drop_duplicates().reset_index(drop=True)

    # drop optional timestamp if present
    df = df.drop(columns=[COL_TS], errors="ignore")

    # make sure required columns exist
    for c in (COL_AP, COL_AR):
        if c not in df.columns:
            # nothing to do for this chunk
            return write_header

    # coerce to numeric safely
    df[COL_AP] = pd.to_numeric(df[COL_AP], errors="coerce")
    df[COL_AR] = pd.to_numeric(df[COL_AR], errors="coerce")

    # drop rows missing amounts
    df = df.dropna(subset=[COL_AP, COL_AR])

    # per-batch z-score with zero-std guard
    for col in (COL_AP, COL_AR):
        std = df[col].std()
        if std and std != 0:
            df[col] = (df[col] - df[col].mean()) / std
        else:
            df[col] = 0.0

    # write TSV to stdout; header only once per mapper
    df.to_csv(sys.stdout, sep="\t", index=False, header=write_header)
    sys.stdout.flush()
    return False  # after first successful write, don't print header again

def from_csv(lines, columns):
    text = "".join(lines)
    if not text.strip():
        return None, columns
    if columns is None:
        # first chunk: infer header
        df = pd.read_csv(io.StringIO(text), on_bad_lines="skip", low_memory=False)
        columns = df.columns.tolist()
    else:
        # subsequent chunks: reuse header
        df = pd.read_csv(io.StringIO(text), header=None, names=columns,
                         on_bad_lines="skip", low_memory=False)
    return df, columns

def main():
    buf = []
    n = 0
    write_header = True
    columns = None

    for line in sys.stdin:
        buf.append(line)
        n += 1
        if n % BATCH_SIZE == 0:
            df, columns = from_csv(buf, columns)
            buf = []
            if df is not None:
                write_header = process_batch(df, write_header)

    if buf:
        df, columns = from_csv(buf, columns)
        if df is not None:
            write_header = process_batch(df, write_header)

if __name__ == "__main__":
    main()