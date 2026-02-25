import re, csv, glob
from pathlib import Path

TS_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.(\d{6})\]")

RECONCILER_RE = re.compile(r"etcd reconciler started", re.IGNORECASE)
PROCESSING_RE = re.compile(r"processing distributed query", re.IGNORECASE)
REGISTERED_RE = re.compile(r"registered distributed query", re.IGNORECASE)
STARTED_RE    = re.compile(r"started local query", re.IGNORECASE)

def ts_ms(line: str):
    m = TS_RE.search(line)
    if not m:
        return None
    hh, mm, ss, us = map(int, m.groups())
    return ((hh*3600 + mm*60 + ss) * 1000) + (us // 1000)

def first_ts(lines, pattern):
    for ln in lines:
        if pattern.search(ln):
            t = ts_ms(ln)
            if t is not None:
                return t
    return None

def diff(a, b):
    if a is None or b is None:
        return None
    if b < a:  # por si cruza medianoche (raro)
        b += 24*3600*1000
    return b - a

def main():
    files = sorted(glob.glob("results/worker_iter*_after.log"))
    if not files:
        raise SystemExit("No files found: results/worker_iter*_after.log")

    out_rows = []
    for f in files:
        p = Path(f)
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()

        t_reconc = first_ts(lines, RECONCILER_RE)
        t_proc   = first_ts(lines, PROCESSING_RE)
        t_reg    = first_ts(lines, REGISTERED_RE)
        t_start  = first_ts(lines, STARTED_RE)

        out_rows.append({
            "file": p.name,
            "t_reconciler_ms": t_reconc,
            "t_processing_ms": t_proc,
            "t_registered_ms": t_reg,
            "t_started_ms": t_start,
            "reconciler_to_processing_ms": diff(t_reconc, t_proc),
            "processing_to_registered_ms": diff(t_proc, t_reg),
            "registered_to_started_ms": diff(t_reg, t_start),
            "reconciler_to_started_ms": diff(t_reconc, t_start),
            "queries_started_count": sum(1 for ln in lines if STARTED_RE.search(ln)),
        })

    out = Path("results/breakdown_full.csv")
    with out.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    print(f"Saved: {out} ({len(out_rows)} rows)")

if __name__ == "__main__":
    main()
