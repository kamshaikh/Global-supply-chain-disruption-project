-- Spark SQL over local Delta tables (run from project root after: python -m src.run_pipeline)

-- 1. Highest-risk suppliers
SELECT supplier_id, supplier_name, country, delay_rate, max_disruption_severity, risk_score, risk_tier
FROM delta.`lakehouse/gold/gold_supplier_risk`
ORDER BY risk_score DESC
LIMIT 10;

-- 2. Risk tier distribution
SELECT risk_tier, COUNT(*) AS suppliers, ROUND(AVG(risk_score), 1) AS avg_risk_score
FROM delta.`lakehouse/gold/gold_supplier_risk`
GROUP BY risk_tier;

-- 3. Critical inventory: below reorder point AND supplied by a HIGH-risk supplier
SELECT warehouse_id, product_id, product_name, supplier_id, quantity_on_hand, reorder_point, risk_score
FROM delta.`lakehouse/gold/gold_inventory_risk`
WHERE priority = 'CRITICAL'
ORDER BY risk_score DESC, coverage_ratio ASC;

-- 4. Disruption impact on shipments
SELECT disruption_id, disruption_type, country, severity, shipments_affected, delayed_shipments, avg_delay_days
FROM delta.`lakehouse/gold/gold_disruption_impact`
ORDER BY shipments_affected DESC;

-- 5. Monthly on-time rate by destination
SELECT destination_country, ship_month, total_shipments, on_time_rate
FROM delta.`lakehouse/gold/gold_shipment_performance`
ORDER BY ship_month, destination_country;

-- 6. Streaming event metrics
SELECT window_start, event_type, event_count, total_delay_hours
FROM delta.`lakehouse/gold/gold_event_metrics`
ORDER BY window_start, event_type;
