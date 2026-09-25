-- CONSULTAS ANALÍTICAS — MVP
-- A análise deve partir das tabelas Delta persistidas no Databricks.
-- Ajuste catalog/schema se necessário.

USE CATALOG workspace;
USE SCHEMA mvp_ecological_footprint;

-- 1. Inventário
SHOW TABLES;

-- 2. Correlações persistidas
SELECT variable_x, variable_y, ROUND(correlation,3) AS pearson_correlation
FROM analysis_correlations
ORDER BY ABS(correlation) DESC;

-- 3. Análise regional
SELECT * FROM analysis_by_region ORDER BY avg_hdi DESC;

-- 4. Faixas de HDI
SELECT * FROM analysis_by_hdi_band ORDER BY avg_hdi;

-- 5. Faixas de PIB
SELECT * FROM analysis_by_gdp_band ORDER BY avg_gdp_per_capita;

-- 6. Top Carbon
SELECT country, region, hdi, gdp_per_capita, carbon_footprint
FROM countries_environmental_analysis
ORDER BY carbon_footprint DESC LIMIT 20;

-- 7. Outliers
SELECT metric, COUNT(*) AS outlier_count
FROM analysis_outliers
GROUP BY metric
ORDER BY outlier_count DESC;

SELECT * FROM analysis_outliers
ORDER BY metric, metric_value DESC;

-- 8. Bases para visualizações de dispersão
SELECT hdi, carbon_footprint, country, region
FROM countries_environmental_analysis
ORDER BY hdi;

SELECT gdp_per_capita, carbon_footprint, country, region
FROM countries_environmental_analysis
ORDER BY gdp_per_capita;
