-- Q-B1: Snapshot mensual completo de Hacienda
SELECT *
FROM staging.hacienda_funcionarios
-- FROM local.controlciudadano.hacienda_funcionarios  -- Spark/Iceberg
WHERE anio = :anio
  AND mes  = :mes;

-- Q-B2: Rango temporal en DJBR (declaraciones juradas)
SELECT *
FROM staging.djbr_raw_data
-- FROM local.controlciudadano.djbr_raw_data  -- Spark/Iceberg
WHERE periodo_id BETWEEN :periodo_desde AND :periodo_hasta;

-- Q-B3: Búsqueda por nombre parcial (ILIKE / LIKE con wildcard al inicio)
SELECT *
FROM staging.set
-- FROM local.controlciudadano.set_ruc  -- Spark/Iceberg
WHERE nombre ILIKE :nombre_patron;
