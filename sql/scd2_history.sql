-- Supplier history (SCD Type 2)

-- Full version history for suppliers that changed
SELECT supplier_id, reliability_score, lead_time_days, status, effective_from, effective_to, is_current
FROM delta.`lakehouse/silver/dim_supplier_scd2`
WHERE supplier_id IN ('S003', 'S007', 'S011', 'S015', 'S020', 'S021')
ORDER BY supplier_id, effective_from;

-- Current versions only
SELECT * FROM delta.`lakehouse/silver/dim_supplier_scd2` WHERE is_current = true;

-- Point-in-time view as of 2025-04-01
SELECT supplier_id, reliability_score, status
FROM delta.`lakehouse/silver/dim_supplier_scd2`
WHERE effective_from <= TIMESTAMP '2025-04-01 00:00:00'
  AND (effective_to IS NULL OR effective_to > TIMESTAMP '2025-04-01 00:00:00');

-- Delta time travel on a silver table
DESCRIBE HISTORY delta.`lakehouse/silver/suppliers`;
