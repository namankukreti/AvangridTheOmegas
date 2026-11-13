# data/

Nothing in this folder is committed (see `.gitignore`); the hackathon data may
not be shared outside the event. Each teammate keeps their own local copy.

## Layout

```
data/
  raw/          optional: put/symlink the extracted hackathon folders + .xlsx files here
  processed/
    ami/batch=<range>/part-XXXXX.parquet   hourly AMI, one file per source CSV
    weather.parquet                        hourly weather
    labels.parquet                         known EV / HP / PV customers
    ami_manifest.csv                       per-file row, customer and date counts
```

## Build it

```bash
pip install -r requirements.txt
python scripts/convert_to_parquet.py --src /path/to/DataAvangrid
```

Already-converted files are skipped, so you can re-run after adding new folders.
Use `--force` to rebuild everything.

## Load it

```python
import pyarrow.dataset as ds
ami = ds.dataset("data/processed/ami", format="parquet", partitioning="hive")
df = ami.to_table(filter=ds.field("customer_number") == 30001).to_pandas()
```

`hour_ending` 1 = 00:00–01:00, 24 = 23:00–24:00 (local time).
Hour start timestamp: `usage_date + (hour_ending - 1) hours`.
