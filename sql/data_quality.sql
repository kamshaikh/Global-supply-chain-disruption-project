-- Data-quality results logged by src/quality.py
SELECT layer, table_name, check_name, severity, failed_rows, passed, run_ts
FROM delta.`lakehouse/gold/dq_results`
ORDER BY run_ts DESC, layer, table_name;

-- Failed checks only
SELECT layer, table_name, check_name, severity, failed_rows
FROM delta.`lakehouse/gold/dq_results`
WHERE passed = false;
