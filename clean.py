import sys, io
import pandas as pd

BATCH_SIZE = 20000 

COLS = [
    "From Bank", "Account", "To Bank", "Account.1",
    "Amount Received", "Receiving Currency",
    "Amount Paid", "Payment Currency",
    "Payment Format", "Is Laundering"
]

COL_AP = "Amount Paid"
COL_AR = "Amount Received"
COL_TS = "Timestamp"  # optional; we drop it if present

def process_batch(df: pd.DataFrame, write_header: bool) -> bool:
    if not df.empty and df.columns.size >= 2:
        mask_header = (df.iloc[:, 0].astype(str) == COLS[0]) & (df.iloc[:, 1].astype(str) == COLS[1])
        if mask_header.any():
            df = df[~mask_header]

    # drop duplicates and reset index
    df = df.drop_duplicates().reset_index(drop=True)

    # drop optional timestamp if present
    df = df.drop(columns=[COL_TS], errors="ignore")

    # make sure required columns exist
    for c in (COL_AP, COL_AR):
        if c not in df.columns:
            return write_header  # nothing to do for this chunk

    # coerce to numeric safely
    df[COL_AP] = pd.to_numeric(df[COL_AP], errors="coerce")
    df[COL_AR] = pd.to_numeric(df[COL_AR], errors="coerce")

    # drop rows missing amounts
    df = df.dropna(subset=[COL_AP, COL_AR])

    # write TSV to stdout; header only once per mapper
    df.to_csv(sys.stdout, sep="\t", index=False, header=write_header)
    sys.stdout.flush()
    return False  # after first successful write, don't print header again

def from_csv(lines, columns):
    text = "".join(lines)
    if not text.strip():
        return None, COLS
    df = pd.read_csv(
        io.StringIO(text),
        header=None,            
        names=COLS,             
        on_bad_lines="skip",
        low_memory=False
    )
    return df, COLS

def main():
    buf = []
    n = 0
    write_header = True
    columns = COLS 

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