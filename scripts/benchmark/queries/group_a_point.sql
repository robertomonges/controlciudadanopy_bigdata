-- Q-A1: Lookup por documento en Hacienda
SELECT *
FROM staging.hacienda_funcionarios
-- FROM local.controlciudadano.hacienda_funcionarios  -- Spark/Iceberg
WHERE documento = :documento;

-- Q-A2: Búsqueda en tabla consolidada (join pre-materializado)
SELECT *
FROM analysis.full_data
-- FROM local.controlciudadano.full_data  -- Spark/Iceberg
WHERE documento = :documento;

-- Q-A3: Lookup empresa por RUC en SET
SELECT *
FROM staging.set
-- FROM local.controlciudadano.set_ruc  -- Spark/Iceberg
WHERE ruc = :ruc;
