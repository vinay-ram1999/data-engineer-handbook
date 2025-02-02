# Data Engineering Pipeline Management

## Ownership of Pipelines

| **Pipeline** | **Primary Owner** | **Secondary Owner** |
|-------------|------------------|------------------|
| **Profit - Unit-level profit for experiments** | Engineer A | Engineer C |
| **Profit - Aggregate profit for investors** | Engineer B | Engineer A |
| **Growth - Aggregate growth for investors** | Engineer C | Engineer D |
| **Growth - Daily growth for experiments** | Engineer D | Engineer B |
| **Engagement - Aggregate engagement for investors** | Engineer A | Engineer D |

## On-Call Schedule

| **Week** | **On-Call Engineer** | **Backup Engineer** |
|---------|--------------------|--------------------|
| Week 1  | Engineer A         | Engineer C         |
| Week 2  | Engineer B         | Engineer D         |
| Week 3  | Engineer C         | Engineer A         |
| Week 4  | Engineer D         | Engineer B         |

### Holiday Considerations
- Swap shifts in advance if a major holiday falls during an engineer’s on-call week.
- Reduce alert thresholds or have two people on-call during end-of-year holidays.

## Runbooks for Investor-Reported Metrics Pipelines

### Key Components of a Runbook
1. **Data Sources:** List of tables, streams, or APIs feeding the pipeline.
2. **Critical SLAs:** Expected latency and uptime requirements.
3. **Failure Scenarios:** Common failure cases and potential impact.
4. **Debugging Steps:**
   - Check data freshness.
   - Validate transformations.
   - Restart jobs if needed.
5. **Escalation Path:** Steps to notify relevant stakeholders if the issue isn't resolved.

### Example Runbook: "Aggregate Profit Reported to Investors"
- **Pipeline:** Profit Aggregation
- **Source:** PostgreSQL (raw transactions), BigQuery (marketing spend)
- **SLA:** Data must be available by **8 AM ET daily**
- **Failure Scenarios:**
  - **Late Data:** Check marketing spend data availability.
  - **Broken Transformation:** Review last code changes and rollback if necessary.
  - **High Latency:** Restart Spark jobs with increased resources.
- **Escalation Path:**
  - First 30 min: Attempt debugging.
  - 1-hour delay: Notify secondary owner.
  - 2-hour delay: Escalate to Data Lead.

## Potential Risks & What Could Go Wrong

1. **Data Delays**
   - Upstream sources fail to refresh on time.
   - Schema drift causes ETL failures.
2. **Schema Changes / Data Quality Issues**
   - Unexpected column additions or removals break transformations.
   - Anomalies like negative profit or duplicate records.
3. **Infrastructure Failures**
   - Spark cluster crashes due to insufficient resources.
   - Kafka consumer lags, causing stale data.
4. **Incorrect Aggregations**
   - Bugs in deployment result in miscalculations.
   - Joins on incorrect keys skew metrics.
5. **Holiday Traffic Spikes**
   - Engagement pipelines overload due to high traffic.
   - Incomplete investor reports due to system failures.

## Summary
By establishing **clear ownership, a fair on-call schedule, detailed runbooks, and proactive risk management**, the data team can ensure smooth pipeline operations and accurate investor reporting.
