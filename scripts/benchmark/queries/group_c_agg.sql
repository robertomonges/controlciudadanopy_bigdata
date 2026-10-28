-- Q-C1: Masa salarial total por institución y período
SELECT
    descripcionentidad  AS institucion,
    anio,
    mes,
    SUM(montodevengado) AS masa_salarial
FROM staging.hacienda_funcionarios
-- FROM local.controlciudadano.hacienda_funcionarios  -- Spark/Iceberg
GROUP BY descripcionentidad, anio, mes
ORDER BY anio, mes;

-- Q-C2: Evolución mensual de cantidad de empleados públicos (SFP)
SELECT
    anio,
    mes,
    COUNT(DISTINCT documento) AS empleados_distintos
FROM staging.sfp
-- FROM local.controlciudadano.sfp  -- Spark/Iceberg
GROUP BY anio, mes
ORDER BY anio, mes;

-- Q-C3: Ranking de proveedores por monto adjudicado (OCDS/Contrataciones)
SELECT
    proveedor_nombre,
    COUNT(*)            AS num_contratos,
    SUM(monto_adjudicado_gs) AS monto_total_gs
FROM staging.ocds_awards
-- FROM local.controlciudadano.ocds_awards  -- Spark/Iceberg
WHERE monto_adjudicado_gs IS NOT NULL
GROUP BY proveedor_nombre
ORDER BY monto_total_gs DESC
LIMIT 100;

-- Q-C4: Comparación salarial SFP vs Hacienda para el mismo documento
SELECT
    h.documento,
    h.descripcionentidad    AS institucion_hacienda,
    h.anio,
    h.mes,
    h.montodevengado        AS salario_hacienda,
    s.cargo                 AS cargo_sfp,
    s.salario               AS salario_sfp,
    (h.montodevengado - s.salario::bigint) AS diferencia
FROM staging.hacienda_funcionarios h
-- FROM local.controlciudadano.hacienda_funcionarios h  -- Spark/Iceberg
JOIN staging.sfp s
-- JOIN local.controlciudadano.sfp s  -- Spark/Iceberg
  ON h.documento = s.documento
 AND h.anio      = s.anio
 AND h.mes       = s.mes
WHERE h.anio = 2024
  AND h.mes  = 10;
