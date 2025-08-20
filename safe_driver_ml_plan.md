# Safe Driver Score ML Plan 🚗📊

## 📊 **Data Analysis Summary**

Based on analysis of the telemetry data, we have:
- **10 drivers** (400001-400011+) with varying behavior patterns
- **~2,400 total telemetry events** with rich sensor data
- **10 accidents** as ground truth (driver 400001 has 2 accidents)
- **33 telemetry features** including speed, g-force, GPS, accelerometer, etc.

## 🎯 **Safe Driver Score Definition**

**Safe Driver Score (0-100)**:
- **100**: Perfect driver (always follows speed limits, smooth driving, no accidents)
- **80-99**: Excellent driver (minor violations, very smooth driving)
- **60-79**: Good driver (occasional violations, mostly safe)
- **40-59**: Average driver (regular violations, some risky behavior)
- **20-39**: Poor driver (frequent violations, aggressive driving)
- **0-19**: Dangerous driver (reckless behavior, accidents)

## 🧮 **Proposed ML Approaches**

### **Option 1: Composite Score Model (Recommended)**
**Type**: Feature Engineering + Weighted Scoring
**MADlib Functions**: Statistical analysis, percentile calculations

**Features to Extract**:
```sql
-- Speed-related safety metrics
speed_limit_compliance_rate = (total_events - violations) / total_events * 100
avg_speed_over_limit = AVG(speed_mph - speed_limit_mph) WHERE speed_mph > speed_limit_mph
excessive_speeding_events = COUNT(*) WHERE speed_mph > speed_limit_mph + 10

-- Driving smoothness metrics  
avg_g_force = AVG(g_force)
max_g_force = MAX(g_force)
harsh_acceleration_events = COUNT(*) WHERE g_force > 1.5
extreme_gforce_events = COUNT(*) WHERE g_force > 3.0

-- Consistency metrics
speed_variance = VARIANCE(speed_mph)
acceleration_variance = VARIANCE(accelerometer_x^2 + accelerometer_y^2 + accelerometer_z^2)

-- Distraction indicators
phone_usage_rate = COUNT(*) WHERE device_screen_on = true AND speed_mph > 5
low_battery_driving = COUNT(*) WHERE device_battery_level < 20 AND speed_mph > 0
```

**Scoring Formula**:
```
Safe Score = 100 - (
  speed_violation_penalty * 0.4 +
  harsh_driving_penalty * 0.3 + 
  distraction_penalty * 0.2 +
  accident_penalty * 0.1
)
```

### **Option 2: Supervised Learning Model**
**Type**: Logistic Regression with Accident Prediction
**MADlib Function**: `madlib.logregr_train()`

**Target Variable**: `has_accident` (binary: 0/1)
**Features**: Aggregated driver metrics from telemetry data
**Output**: Probability of accident → Convert to safety score (100 - accident_probability * 100)

### **Option 3: Clustering + Anomaly Detection**
**Type**: K-Means Clustering for Driver Behavior Types
**MADlib Function**: `madlib.kmeans()`

**Approach**:
1. Cluster drivers into behavior groups (Safe, Average, Risky)
2. Assign base scores per cluster
3. Fine-tune with individual metrics

### **Option 4: Ensemble Scoring**
**Type**: Multiple Models Combined
**MADlib Functions**: Multiple

**Models**:
1. Speed compliance model (Linear Regression)
2. Aggressive driving model (SVM)
3. Accident risk model (Logistic Regression)
4. Combine with weighted average

## 🚀 **Recommended Implementation: Option 1 (Composite Score)**

### **Phase 1: Feature Engineering** 
Create aggregated driver metrics table:

```sql
CREATE TABLE driver_behavior_metrics AS
SELECT 
    driver_id,
    
    -- Volume metrics
    COUNT(*) as total_events,
    
    -- Speed compliance (40% weight)
    ROUND((COUNT(*) - COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph))::NUMERIC / COUNT(*) * 100, 2) as speed_compliance_rate,
    ROUND(AVG(CASE WHEN speed_mph > speed_limit_mph THEN speed_mph - speed_limit_mph ELSE 0 END), 2) as avg_speed_violation,
    COUNT(*) FILTER (WHERE speed_mph > speed_limit_mph + 10) as excessive_speeding_count,
    
    -- Driving smoothness (30% weight)
    ROUND(AVG(g_force), 4) as avg_g_force,
    ROUND(MAX(g_force), 4) as max_g_force,
    COUNT(*) FILTER (WHERE g_force > 1.5) as harsh_driving_events,
    COUNT(*) FILTER (WHERE g_force > 3.0) as extreme_events,
    
    -- Distraction indicators (20% weight)
    ROUND(COUNT(*) FILTER (WHERE device_screen_on = true AND speed_mph > 5)::NUMERIC / COUNT(*) * 100, 2) as phone_usage_rate,
    COUNT(*) FILTER (WHERE device_battery_level < 20 AND speed_mph > 0) as low_battery_driving_events,
    
    -- Speed consistency
    ROUND(STDDEV(speed_mph), 2) as speed_variance,
    
    -- Stats for percentile calculations
    ROUND(AVG(speed_mph), 2) as avg_speed,
    ROUND(MAX(speed_mph), 2) as max_speed

FROM vehicle_telemetry_data_v2 
GROUP BY driver_id;
```

### **Phase 2: Accident Integration**
Add accident history:

```sql
CREATE TABLE driver_safety_features AS
SELECT 
    dm.*,
    COALESCE(a.accident_count, 0) as accident_count,
    COALESCE(a.has_accident, false) as has_accident
FROM driver_behavior_metrics dm
LEFT JOIN (
    SELECT driver_id, COUNT(*) as accident_count, true as has_accident
    FROM accidents 
    GROUP BY driver_id
) a ON dm.driver_id = a.driver_id;
```

### **Phase 3: Scoring Algorithm**
Calculate composite safe driver score:

```sql
CREATE TABLE safe_driver_scoring AS
SELECT 
    driver_id,
    
    -- Component scores (0-100 each)
    GREATEST(0, 100 - (100 - speed_compliance_rate) * 2) as speed_score,
    GREATEST(0, 100 - (avg_g_force - 1.0) * 50) as smoothness_score,  
    GREATEST(0, 100 - harsh_driving_events * 2) as harsh_driving_score,
    GREATEST(0, 100 - phone_usage_rate * 3) as distraction_score,
    GREATEST(0, 100 - accident_count * 20) as accident_score,
    
    -- Composite score with weights
    ROUND(
        GREATEST(0, LEAST(100,
            (GREATEST(0, 100 - (100 - speed_compliance_rate) * 2) * 0.4) +
            (GREATEST(0, 100 - (avg_g_force - 1.0) * 50) * 0.15) + 
            (GREATEST(0, 100 - harsh_driving_events * 2) * 0.15) +
            (GREATEST(0, 100 - phone_usage_rate * 3) * 0.2) +
            (GREATEST(0, 100 - accident_count * 20) * 0.1)
        )), 2
    ) as safe_driver_score,
    
    -- Risk level categorization
    CASE 
        WHEN ROUND(
            (GREATEST(0, 100 - (100 - speed_compliance_rate) * 2) * 0.4) +
            (GREATEST(0, 100 - (avg_g_force - 1.0) * 50) * 0.15) + 
            (GREATEST(0, 100 - harsh_driving_events * 2) * 0.15) +
            (GREATEST(0, 100 - phone_usage_rate * 3) * 0.2) +
            (GREATEST(0, 100 - accident_count * 20) * 0.1), 2
        ) >= 90 THEN 'EXCELLENT'
        WHEN ROUND(
            (GREATEST(0, 100 - (100 - speed_compliance_rate) * 2) * 0.4) +
            (GREATEST(0, 100 - (avg_g_force - 1.0) * 50) * 0.15) + 
            (GREATEST(0, 100 - harsh_driving_events * 2) * 0.15) +
            (GREATEST(0, 100 - phone_usage_rate * 3) * 0.2) +
            (GREATEST(0, 100 - accident_count * 20) * 0.1), 2
        ) >= 80 THEN 'GOOD'
        WHEN ROUND(
            (GREATEST(0, 100 - (100 - speed_compliance_rate) * 2) * 0.4) +
            (GREATEST(0, 100 - (avg_g_force - 1.0) * 50) * 0.15) + 
            (GREATEST(0, 100 - harsh_driving_events * 2) * 0.15) +
            (GREATEST(0, 100 - phone_usage_rate * 3) * 0.2) +
            (GREATEST(0, 100 - accident_count * 20) * 0.1), 2
        ) >= 60 THEN 'AVERAGE'
        WHEN ROUND(
            (GREATEST(0, 100 - (100 - speed_compliance_rate) * 2) * 0.4) +
            (GREATEST(0, 100 - (avg_g_force - 1.0) * 50) * 0.15) + 
            (GREATEST(0, 100 - harsh_driving_events * 2) * 0.15) +
            (GREATEST(0, 100 - phone_usage_rate * 3) * 0.2) +
            (GREATEST(0, 100 - accident_count * 20) * 0.1), 2
        ) >= 40 THEN 'POOR'
        ELSE 'HIGH_RISK'
    END as risk_category,
    
    total_events,
    speed_compliance_rate,
    avg_g_force,
    harsh_driving_events,
    phone_usage_rate,
    accident_count

FROM driver_safety_features;
```

### **Phase 4: Populate Safe Driver Scores Table**
Insert results into the renamed table:

```sql
INSERT INTO safe_driver_scores (driver_id, score, calculation_date, notes)
SELECT 
    driver_id,
    safe_driver_score,
    NOW(),
    CONCAT('Risk Level: ', risk_category, 
           ' | Events: ', total_events,
           ' | Speed Compliance: ', speed_compliance_rate, '%',
           ' | Avg G-Force: ', avg_g_force,
           ' | Accidents: ', accident_count)
FROM safe_driver_scoring;
```

## 🧪 **Alternative: MADlib Supervised Learning Model**

If you prefer a more advanced ML approach:

```sql
-- Create training dataset
CREATE TABLE driver_training_data AS
SELECT 
    driver_id,
    speed_compliance_rate,
    avg_g_force,
    harsh_driving_events,
    phone_usage_rate,
    speed_variance,
    has_accident::INTEGER as target
FROM driver_safety_features;

-- Train logistic regression model
SELECT madlib.logregr_train(
    'driver_training_data',        -- source table
    'driver_accident_model',       -- output table
    'target',                      -- dependent variable
    'ARRAY[speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]' -- features
);

-- Generate probability scores
CREATE TABLE driver_risk_predictions AS
SELECT 
    driver_id,
    madlib.logregr_predict(
        (SELECT coef FROM driver_accident_model), 
        ARRAY[speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
    ) as accident_probability,
    ROUND(100 - madlib.logregr_predict(
        (SELECT coef FROM driver_accident_model), 
        ARRAY[speed_compliance_rate, avg_g_force, harsh_driving_events, phone_usage_rate, speed_variance]
    ) * 100, 2) as ml_safety_score
FROM driver_safety_features;
```

## 🎯 **Recommendations**

1. **Start with Option 1 (Composite Score)** - It's interpretable and actionable
2. **Validate with Option 2 (ML Model)** - Compare results and refine
3. **Implement monthly recalculation** - Scores should update as new data arrives
4. **Add score history tracking** - Track improvement/decline over time

Which approach would you like to implement first? I recommend starting with the Composite Score model since it's more transparent and easier to explain to stakeholders.
