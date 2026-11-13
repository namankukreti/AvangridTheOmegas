#!/usr/bin/env python3
"""
Convert the raw Avangrid hackathon data to compressed, typed Parquet.

Input  (--src): the folder holding the extracted hackathon data, e.g.
    DataAvangrid/
        2026 Avangrid Hackathon Data_1_10000/part-*.csv
        2026 Avangrid Hackathon Data_10001_20000_v2/part-*.csv
        ...
        Weather_Data_Avangrid_Hackathon.xlsx          (optional)
        EV_Heat Pump_Solar PV Customers.xlsx          (optional)

Output (--out, default data/processed):
    ami/batch=<range>/part-XXXXX.parquet   one file per source CSV
    weather.parquet                        if the weather workbook is found
    labels.parquet                         if the labels workbook is found
    ami_manifest.csv                       row/customer/date counts per file

AMI schema written:
    customer_number  int32
    usage_date       date32
    hour_ending      int8     (1..24; HE1 = 00:00-01:00)
    day_type         dictionary<string>  (Weekday / Weekend)
    hourly_kwh       float32
    interval_count   int8
    interval_type    dictionary<string>
    is_complete_hour int8

Usage:
    python scripts/convert_to_parquet.py --src /path/to/DataAvangrid
    python scripts/convert_to_parquet.py --src ... --workers 3 --force

Read it back:
    import pyarrow.dataset as ds
    tbl = ds.dataset("data/processed/ami", format="parquet", partitioning="hive").to_table()
    # or DuckDB:  SELECT * FROM 'data/processed/ami/*/*.parquet'
"""
from __future__ import annotations

import argparse
import csv
import re
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

REPO_ROOT = Path(__file__).resolve().parents[1]

AMI_SCHEMA = pa.schema(
    [
        ("customer_number", pa.int32()),
        ("usage_date", pa.date32()),
        ("hour_ending", pa.int8()),
        ("day_type", pa.dictionary(pa.int8(), pa.string())),
        ("hourly_kwh", pa.float32()),
        ("interval_count", pa.int8()),
        ("interval_type", pa.dictionary(pa.int8(), pa.string())),
        ("is_complete_hour", pa.int8()),
    ]
)

# Raw CSV column types (dates parsed as timestamps first so that both
# 2025-09-01 and 09/01/2025 formats work, then cast to date32).
RAW_TYPES = {
    "customer_number": pa.int32(),
    "usage_date": pa.timestamp("s"),
    "hour_ending": pa.int8(),
    "day_type": pa.string(),
    "hourly_kwh": pa.float32(),
    "interval_count": pa.int8(),
    "interval_type": pa.string(),
    "is_complete_hour": pa.int8(),
}
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y"]


def batch_name(folder: Path) -> str:
    """'2026 Avangrid Hackathon Data_10001_20000_v2' -> '10001_20000'."""
    m = re.search(r"(\d+)_(\d+)", folder.name)
    return f"{m.group(1)}_{m.group(2)}" if m else re.sub(r"\W+", "_", folder.name)


def part_name(csv_path: Path) -> str:
    m = re.match(r"(part-\d+)", csv_path.name)
    return m.group(1) if m else csv_path.stem


def normalize(batch: pa.RecordBatch) -> pa.Table:
    t = pa.Table.from_batches([batch])
    t = t.set_column(
        t.schema.get_field_index("usage_date"),
        "usage_date",
        pc.cast(t["usage_date"], pa.date32()),
    )
    for col in ("day_type", "interval_type"):
        t = t.set_column(t.schema.get_field_index(col), col, pc.dictionary_encode(t[col]))
    return t.select(AMI_SCHEMA.names).cast(AMI_SCHEMA)


def convert_csv(src: Path, dst: Path) -> dict:
    """Stream one AMI CSV into one Parquet file. Low memory (~block_size)."""
    with open(src, newline="") as f:
        header = next(csv.reader(f))
    names = [h.strip().lower() for h in header]
    missing = set(RAW_TYPES) - set(names)
    if missing:
        raise ValueError(f"{src.name}: missing columns {sorted(missing)}")

    reader = pacsv.open_csv(
        src,
        read_options=pacsv.ReadOptions(
            column_names=names, skip_rows=1, block_size=64 << 20
        ),
        convert_options=pacsv.ConvertOptions(
            column_types=RAW_TYPES, timestamp_parsers=DATE_FORMATS
        ),
    )

    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(".parquet.tmp")
    rows = incomplete = 0
    custs: set[int] = set()
    dmin = dmax = None
    with pq.ParquetWriter(tmp, AMI_SCHEMA, compression="zstd", compression_level=6) as w:
        for batch in reader:
            t = normalize(batch)
            w.write_table(t, row_group_size=1_000_000)
            rows += t.num_rows
            incomplete += t.num_rows - pc.sum(t["is_complete_hour"]).as_py()
            custs.update(pc.unique(t["customer_number"]).to_pylist())
            mm = pc.min_max(t["usage_date"])
            lo, hi = mm["min"].as_py(), mm["max"].as_py()
            dmin = lo if dmin is None or lo < dmin else dmin
            dmax = hi if dmax is None or hi > dmax else dmax
    tmp.replace(dst)  # atomic: a crash never leaves a half-written .parquet

    return {
        "source": f"{src.parent.name}/{src.name}",
        "output": f"{dst.parent.name}/{dst.name}",
        "rows": rows,
        "customers": len(custs),
        "cust_min": min(custs) if custs else "",
        "cust_max": max(custs) if custs else "",
        "date_min": dmin,
        "date_max": dmax,
        "incomplete_hours": incomplete,
        "csv_mb": round(src.stat().st_size / 1e6, 1),
        "parquet_mb": round(dst.stat().st_size / 1e6, 1),
    }


def convert_excel(src_dir: Path, out_dir: Path, force: bool) -> None:
    """Weather + label workbooks -> Parquet (needs pandas + openpyxl)."""
    xlsx = [p for p in src_dir.rglob("*.xlsx") if not p.name.startswith("~$")]
    weather = next((p for p in xlsx if p.name.lower().startswith("weather")), None)
    labels = next((p for p in xlsx if "customers" in p.name.lower()), None)
    if not weather and not labels:
        print("No weather / label workbooks found in --src (skipping).")
        return
    import pandas as pd

    if weather and (force or not (out_dir / "weather.parquet").exists()):
        df = pd.read_excel(weather)
        df.columns = [c.strip().lower() for c in df.columns]
        date = pd.to_datetime(df["date"])
        time_ = pd.to_timedelta(df["time"].astype(str))
        df.insert(0, "timestamp", date + time_)
        df.to_parquet(out_dir / "weather.parquet", index=False)
        print(f"weather.parquet: {len(df):,} rows, "
              f"{df['timestamp'].min()} -> {df['timestamp'].max()}")

    if labels and (force or not (out_dir / "labels.parquet").exists()):
        sheets = pd.read_excel(labels, sheet_name=None)
        frames = []
        for sheet, df in sheets.items():
            df.columns = [str(c).strip().lower() for c in df.columns]
            if "customer_number" not in df.columns:  # e.g. the "Description" sheet
                continue
            tech = sheet.split()[0].upper()  # "EV Customers" -> "EV"
            frames.append(
                df[["customer_number"]].dropna().astype("int32").assign(technology=tech)
            )
        out = pd.concat(frames, ignore_index=True)
        out.to_parquet(out_dir / "labels.parquet", index=False)
        print("labels.parquet:", out["technology"].value_counts().to_dict())


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--src", type=Path, default=REPO_ROOT / "data" / "raw",
                    help="folder with the extracted hackathon data (default: data/raw)")
    ap.add_argument("--out", type=Path, default=REPO_ROOT / "data" / "processed")
    ap.add_argument("--workers", type=int, default=2, help="parallel files (each ~200-400 MB RAM)")
    ap.add_argument("--force", action="store_true", help="re-convert files that already exist")
    args = ap.parse_args()

    src, out = args.src.expanduser().resolve(), args.out.expanduser().resolve()
    csvs = sorted(p for p in src.rglob("*.csv") if not any(s.startswith(".") for s in p.parts))
    if not csvs:
        print(f"No CSV files under {src}", file=sys.stderr)
        return 1

    jobs = []
    for p in csvs:
        dst = out / "ami" / f"batch={batch_name(p.parent)}" / f"{part_name(p)}.parquet"
        if args.force or not dst.exists():
            jobs.append((p, dst))
    print(f"{len(csvs)} CSVs found, {len(jobs)} to convert -> {out / 'ami'}")

    t0, results, failed = time.time(), [], []
    with ProcessPoolExecutor(max_workers=args.workers) as ex:
        futs = {ex.submit(convert_csv, p, d): p for p, d in jobs}
        for i, fut in enumerate(as_completed(futs), 1):
            p = futs[fut]
            try:
                r = fut.result()
                results.append(r)
                print(f"[{i}/{len(jobs)}] {r['source']}: {r['rows']:,} rows, "
                      f"{r['csv_mb']} MB -> {r['parquet_mb']} MB", flush=True)
            except Exception as e:  # keep going; report at the end
                failed.append((p, e))
                print(f"[{i}/{len(jobs)}] FAILED {p.name}: {e}", flush=True)

    if results:
        manifest = out / "ami_manifest.csv"
        new = not manifest.exists()
        with open(manifest, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0]))
            if new:
                w.writeheader()
            w.writerows(sorted(results, key=lambda r: r["source"]))
        csv_mb = sum(r["csv_mb"] for r in results)
        pq_mb = sum(r["parquet_mb"] for r in results)
        print(f"\nDone in {time.time() - t0:.0f}s: {sum(r['rows'] for r in results):,} rows, "
              f"{csv_mb:,.0f} MB CSV -> {pq_mb:,.0f} MB Parquet "
              f"({csv_mb / max(pq_mb, 1e-9):.1f}x smaller)")

    convert_excel(src, out, args.force)

    if failed:
        print(f"\n{len(failed)} file(s) failed:", *[f"  {p}: {e}" for p, e in failed], sep="\n")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
