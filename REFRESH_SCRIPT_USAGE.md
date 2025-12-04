# Safe Driver Score Refresh Scripts - Usage Guide

## Overview

Both refresh scripts now support a `--force` parameter to force model retraining regardless of schedule or new accidents.

---

## Scripts Available

### 1. **refresh_safe_driver_scores.sh** (Standard)
Uses the standard `vehicle_telemetry_data_v2` external table.

```bash
# Normal run (retrains only on Sunday with new accidents)
./refresh_safe_driver_scores.sh

# Force model retraining immediately
./refresh_safe_driver_scores.sh --force
```

### 2. **refresh_safe_driver_scores_fixed.sh** (Corrupted File Workaround)
Uses `vehicle_telemetry_data_v2_working` to skip corrupted partitions.

```bash
# Normal run (retrains only on Sunday with new accidents)
./refresh_safe_driver_scores_fixed.sh

# Force model retraining immediately
./refresh_safe_driver_scores_fixed.sh --force
```

---

## What Each Script Does

### Without `--force` (Default Behavior)

1. ✅ **Extract features** from telemetry data
2. ✅ **Update training data** with accident history
3. ✅ **Calculate scores** using existing model
4. ⏱️ **Retrain model** ONLY if:
   - Today is Sunday AND
   - New accidents added in past 7 days

### With `--force` Parameter

1. ✅ **Extract features** from telemetry data
2. ✅ **Update training data** with accident history
3. ✅ **Train NEW model** (ignoring schedule/accident checks)
4. ✅ **Calculate scores** with new model
5. ✅ **Recalculate scores** again with the retrained model

---

## When to Use `--force`

Use the `--force` flag when:

- 🔧 **Testing model changes** - you modified feature engineering
- 📊 **Major data updates** - bulk accident data imported
- 🐛 **Bug fixes** - fixed issues in training data preparation
- 🎯 **Initial deployment** - first time setup or model baseline
- 📈 **Model drift** - scores seem inaccurate, need recalibration

---

## Complete ML Pipeline Steps

Both scripts execute this full pipeline:

```
Step 1: Verify database connection
Step 2: Check for new telemetry data
Step 3: Execute score recalculation
        ├─ Extract features from telemetry
        ├─ Update training data with accidents
        └─ Calculate scores (using existing model)
Step 4: Verify recalculation results
Step 5: Update model (if conditions met or --force)
        ├─ Drop existing model
        ├─ Train new logistic regression model
        └─ Recalculate all scores with new model
Step 6: Send alerts for high-risk drivers
Step 7: Performance summary
```

---

## Alternative: Quick Model Training

If you ONLY want to retrain the model (without full feature extraction):

```bash
./train_model_now.sh
```

This assumes `driver_ml_training_data` already exists and is current.

---

## SQL File Changes

**Note:** The `recalculate_safe_driver_scores.sql` file now has Step 3 (model training) **uncommented** by default.

- To disable automatic training in SQL: Comment out lines 89-104
- To enable: Uncomment lines 89-104 (currently enabled)

---

## Logs

All script runs create timestamped logs:

```bash
./logs/safe_driver_refresh_YYYYMMDD_HHMMSS.log
```

Check logs for:
- Training iterations
- Model coefficients
- Score distributions
- High-risk driver alerts

---

## Examples

### Example 1: Weekly Scheduled Run
```bash
# Cron: Every Sunday at 2 AM
0 2 * * 0 /path/to/refresh_safe_driver_scores.sh
```

### Example 2: Force Retrain After Data Import
```bash
# After importing new accident data
./refresh_safe_driver_scores.sh --force
```

### Example 3: Test Model Changes
```bash
# After modifying feature engineering SQL
./refresh_safe_driver_scores.sh --force
```

---

## Troubleshooting

### "No new data found. Exiting."
- First-time run: This is expected behavior - run anyway
- Subsequent runs: No new telemetry since last update

### Corrupted Parquet Errors
Use the `_fixed.sh` version instead:
```bash
./refresh_safe_driver_scores_fixed.sh --force
```

### Model Training Fails
Check logs for:
- MADlib extension installed
- Sufficient training data (need labeled accidents)
- Feature columns match model expectations

---

## Performance

Typical execution times:

| Operation | Duration |
|-----------|----------|
| Feature Extraction | 30-60 seconds |
| Model Training | 5-10 seconds |
| Score Calculation | 10-20 seconds |
| **Total (with retrain)** | **~2 minutes** |
| **Total (without retrain)** | **~1 minute** |

---

## Questions?

- See: [SAFE_DRIVER_ML_SYSTEM.md](SAFE_DRIVER_ML_SYSTEM.md) for full ML documentation
- Logs: `./logs/` directory
- Issues: Check PXF logs for external table errors
