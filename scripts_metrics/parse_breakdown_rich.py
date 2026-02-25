import re
import csv
import glob
from pathlib import Path

# Timestamp en tus logs: [16:26:43.215756]
TS_RE = re.compile(r"\[(\d{2}):(\d{2}):(\d{2})\.(\d{6})\]")

def ts_ms(line: str):
    m = TS_RE.search(line)
    if not m:
        return None
    hh, mm, ss, us = map(int, m.groups())
    return ((hh * 3600 + mm * 60 + ss) * 1000) + (us // 1000)

def first_ts(lines, pattern, start_index=0):
    for i in range(start_index, len(lines)):
        if pattern.search(lines[i]):
            t = ts_ms(lines[i])
            if t is not None:
                return i, t
    return None, None

def diff(a, b):
    if a is None or b is None:
        return None
    if b < a:  # cruza medianoche (raro)
        b += 24 * 3600 * 1000
    return b - a

# Patrones (elige solo líneas que sí llevan timestamp)
BOOT_RE        = re.compile(r"BufferManager\.cpp:110.*initialize", re.IGNORECASE)
RECONCILER_RE  = re.compile(r"etcd reconciler started", re.IGNORECASE)
PROCESSING_RE  = re.compile(r"processing distributed query", re.IGNORECASE)
PIPELINE_RE    = re.compile(r"Constructed pipeline plan", re.IGNORECASE)
REGISTERED_RE  = re.compile(r"registered distributed query", re.IGNORECASE)
SINKDESC_RE    = re.compile(r"The sinkDescriptor is", re.IGNORECASE)
STARTED_RE     = re.compile(r"started local query", re.IGNORECASE)
FILESINK_RE    = re.compile(r"FileSink\.cpp:73.*Setting up file sink", re.IGNORECASE)

def parse_file(path: Path):
    lines = path.read_text(encoding="utf-8", errors="ignore").splitlines()

    # 1) Boot (primer BufferManager initialize)
    i_boot, t_boot = first_ts(lines, BOOT_RE)

    # 2) Reconciler started
    i_rec, t_rec = first_ts(lines, RECONCILER_RE)

    # 3) First processing AFTER reconciler started (para no pillar basura)
    start_from = i_rec if i_rec is not None else 0
    i_proc, t_proc = first_ts(lines, PROCESSING_RE, start_index=start_from)

    # 4) Pipeline constructed (después de processing)
    start_from = i_proc if i_proc is not None else start_from
    i_pipe, t_pipe = first_ts(lines, PIPELINE_RE, start_index=start_from)

    # 5) Registered
    start_from = i_pipe if i_pipe is not None else start_from
    i_reg, t_reg = first_ts(lines, REGISTERED_RE, start_index=start_from)

    # 6) SinkDescriptor (opcional pero útil)
    start_from_sd = i_reg if i_reg is not None else start_from
    i_sd, t_sd = first_ts(lines, SINKDESC_RE, start_index=start_from_sd)

    # 7) Started local query
    start_from = i_reg if i_reg is not None else start_from
    i_start, t_start = first_ts(lines, STARTED_RE, start_index=start_from)

    # 8) FileSink setup (después de started)
    start_from = i_start if i_start is not None else start_from
    i_fs, t_fs = first_ts(lines, FILESINK_RE, start_index=start_from)

    # Conteo de queries arrancadas (para tu otra pregunta)
    started_count = sum(1 for ln in lines if STARTED_RE.search(ln))

    return {
        "file": path.name,
        "queries_started_count": started_count,

        "t_boot_ms": t_boot,
        "t_reconciler_ms": t_rec,
        "t_processing_ms": t_proc,
        "t_pipeline_ms": t_pipe,
        "t_registered_ms": t_reg,
        "t_sinkdesc_ms": t_sd,
        "t_started_ms": t_start,
        "t_filesink_ms": t_fs,

        "boot_to_reconciler_ms": diff(t_boot, t_rec),
        "reconciler_to_processing_ms": diff(t_rec, t_proc),
        "processing_to_pipeline_ms": diff(t_proc, t_pipe),
        "pipeline_to_registered_ms": diff(t_pipe, t_reg),
        "registered_to_started_ms": diff(t_reg, t_start),
        "started_to_filesink_ms": diff(t_start, t_fs),

        "internal_total_reconciler_to_started_ms": diff(t_rec, t_start),
    }

def main():
    files = sorted(glob.glob("results/worker_iter*_after.log"))
    if not files:
        raise SystemExit("No files found: results/worker_iter*_after.log")

    rows = [parse_file(Path(f)) for f in files]

    out = Path("results/breakdown_rich.csv")
    with out.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    print(f"Saved: {out} ({len(rows)} rows)")

    # Aviso si faltan eventos
    bad = [r["file"] for r in rows if r["t_processing_ms"] is None or r["t_started_ms"] is None]
    if bad:
        print("WARNING: missing processing/started in:", ", ".join(bad))

if __name__ == "__main__":
    main()
