# Capacity & Backpressure Test Runbook

## Test Performed: Oct 10, 2026

### Configuration
| Parameter | Value |
|-----------|-------|
| Burst size | 50 files |
| Records per file | 200 |
| Total records | 10,000 |
| maxFilesPerTrigger | 2 (backpressure cap) |
| Spark mode | local[*] (8 cores) |
| Driver memory | 4g |

### Result: PASSED - No crash, no data loss, smooth backpressure

All 50 files were dropped **simultaneously** in 0.18s (simulating an instant burst).
The consumer throttled intake to 2 files per trigger, processing the burst as 25 sequential
micro-batches without crashing or losing data.

### Measured Performance

| Metric | Value |
|--------|-------|
| Total batches | 25 |
| Total processing time | 321.03s |
| Avg throughput | 31 records/sec |
| Gold output records | 10,000 (100% of input) |
| Fraud cases detected | 68 |
| DLQ records | 0 |

### Per-Batch Timing (all 25 batches)

| Batch | Rows | Time (s) | Rec/s | Notes |
|-------|------|----------|-------|-------|
| 0 | 400 | 15.99 | 25 | Cold start / JIT compilation |
| 1 | 400 | 4.19 | 96 | Warm-up |
| 2 | 400 | 3.33 | 120 | Steady state |
| 3 | 400 | 3.49 | 115 | Steady state |
| 4 | 400 | 3.45 | 116 | Steady state |
| 5 | 400 | 3.61 | 111 | Steady state |
| 6 | 400 | 4.28 | 94 | |
| 7 | 400 | 5.52 | 72 | Minor GC/IO spike |
| 8 | 400 | 4.98 | 80 | |
| 9 | 400 | 7.98 | 50 | IO contention (many Parquet writes) |
| 10 | 400 | 3.00 | 133 | Best batch |
| 11 | 400 | 6.01 | 67 | |
| 12 | 400 | 8.67 | 46 | Worst steady-state batch |
| 13 | 400 | 3.53 | 113 | Recovery |
| 14 | 400 | 3.93 | 102 | |
| 15 | 400 | 5.56 | 72 | |
| 16 | 400 | 3.97 | 101 | |
| 17 | 400 | 5.16 | 78 | |
| 18 | 400 | 3.31 | 121 | |
| 19 | 400 | 4.13 | 97 | |
| 20 | 400 | 4.03 | 99 | |
| 21 | 400 | 4.32 | 93 | |
| 22 | 400 | 4.88 | 82 | |
| 23 | 400 | 3.91 | 102 | |
| 24 | 400 | 7.74 | 52 | Final batch |

### Lag Analysis (excluding Batch 0 cold start)

| Metric | Value |
|--------|-------|
| Min batch time | 3.00s (Batch 10) |
| Max batch time | 8.67s (Batch 12) |
| Avg batch time | 4.75s |
| Throughput range | 46-133 rec/s |

### Key Observations

1. **Backpressure works**: The consumer throttled intake to exactly 2 files per trigger (400 records/batch), even though all 50 files were available simultaneously. Files queued up in the source directory rather than overwhelming the Spark job.

2. **No crash under burst**: Despite an instant 50-file burst, the consumer processed all 25 batches to completion without errors, OOM, or data loss.

3. **Cold start penalty**: Batch 0 took 15.99s (JIT compilation, class loading). All subsequent batches averaged 4.75s. In production, the consumer would already be warm.

4. **IO contention spikes**: Batches 9, 12, and 24 showed higher latency (7-9s), likely due to Parquet write contention from accumulated Gold partition files. This is expected behavior and self-recovers in the next batch.

5. **100% data integrity**: All 10,000 input records were processed to Gold. Zero records were lost or duplicated.

### How to Reproduce

```powershell
cd C:\Users\bharg\Enterprise_fraud_detection
python scripts/load_test_spike.py --burst-size 50 --records-per-file 200 --max-files-per-trigger 2 --clean
```

### Tuning the Backpressure

To adjust the throttle without code changes:
```powershell
# Via environment variable
$env:EFIP_MAX_FILES_PER_TRIGGER = "5"
python src/ingestion/stream_consumer.py

# Via command-line argument
python src/ingestion/stream_consumer.py --max-files 5
```

Higher values = faster throughput but more memory usage per batch.
Lower values = smoother processing but higher end-to-end latency.

### Interview Talking Points

1. "We ran a burst load test dropping 50 files simultaneously. The consumer processed all 10,000 records across 25 throttled micro-batches with zero data loss. Processing lag stayed between 3-9 seconds per batch."

2. "Backpressure is controlled by maxFilesPerTrigger, which is tunable at runtime via environment variable. During a burst, files queue up in the source directory rather than overwhelming the Spark job."

3. "The cold-start penalty on Batch 0 (16s vs 4.75s average) would not occur in production because the consumer runs continuously. This is a known JVM warm-up behavior."
