-- Q-D1: Reconstrucción de analysis.full_data desde staging

SELECT
    COALESCE(h.documento, s.documento, p.cedula) AS documento,

    COALESCE(h.nombres || ' ' || h.apellidos,
             s.nombres_apellidos)                 AS nombre_completo,

    h.descripcionentidad    AS institucion_hacienda,
    h.cargo                 AS cargo_hacienda,
    h.montodevengado        AS salario_hacienda,
    h.anio                  AS anio_hacienda,
    h.mes                   AS mes_hacienda,

    s.entidad               AS institucion_sfp,
    s.cargo                 AS cargo_sfp,
    s.salario               AS salario_sfp,

    p.cargo                 AS cargo_policia,
    p.salario               AS salario_policia,
    p.anio                  AS anio_policia,
    p.mes                   AS mes_policia,

    py.name                 AS nombre_pytyvo,
    py.department           AS departamento_pytyvo,

    r.razon_social          AS razon_social_ruc,
    r.ruc                   AS ruc

FROM staging.hacienda_funcionarios h
-- FROM local.controlciudadano.hacienda_funcionarios h  -- Spark
FULL OUTER JOIN staging.sfp s
    ON h.documento = s.documento
   AND h.anio = s.anio AND h.mes = s.mes
FULL OUTER JOIN staging.policia p
    ON COALESCE(h.documento, s.documento) = p.cedula
   AND COALESCE(h.anio, s.anio) = p.anio
   AND COALESCE(h.mes, s.mes) = p.mes
LEFT JOIN staging.pytyvo py
    ON COALESCE(h.documento, s.documento, p.cedula) = py.cid
LEFT JOIN staging.set r
    ON COALESCE(h.documento, s.documento) = r.contribuyente_ci
WHERE COALESCE(h.anio, s.anio) = 2024
  AND COALESCE(h.mes, s.mes)   = 10;

-- Q-D2: Evolución patrimonial de un funcionario (window function + cross-source)

SELECT
    d.documento,
    d.periodo_id            AS periodo_djbr,
    d.activos_total,
    d.pasivos_total,
    d.patrimonio_neto,
    LAG(d.patrimonio_neto) OVER (
        PARTITION BY d.documento
        ORDER BY d.periodo_id
    )                       AS patrimonio_periodo_anterior,
    (d.patrimonio_neto - LAG(d.patrimonio_neto) OVER (
        PARTITION BY d.documento ORDER BY d.periodo_id
    ))                      AS variacion_patrimonio,
    h.descripcionentidad    AS institucion,
    h.cargo,
    h.montodevengado        AS salario_anual_aprox
FROM staging.djbr_raw_data d
-- FROM local.controlciudadano.djbr_raw_data d  -- Spark
LEFT JOIN (
    SELECT documento, anio, descripcionentidad, cargo,
           SUM(montodevengado) AS montodevengado
    FROM staging.hacienda_funcionarios
    -- FROM local.controlciudadano.hacienda_funcionarios
    GROUP BY documento, anio, descripcionentidad, cargo
) h ON d.documento = h.documento
   AND d.periodo_id = h.anio
WHERE d.documento = :documento
ORDER BY d.periodo_id;

-- Q-D3: Funcionarios que aparecen en nómina Hacienda + SFP + Policía simultáneamente

SELECT
    h.documento,
    h.nombres || ' ' || h.apellidos    AS nombre,
    h.descripcionentidad               AS institucion_hacienda,
    h.montodevengado                   AS salario_hacienda,
    s.entidad                          AS institucion_sfp,
    s.salario                          AS salario_sfp,
    p.dependencia                      AS dependencia_policia,
    p.salario                          AS salario_policia,
    h.montodevengado + COALESCE(s.salario::bigint, 0) + COALESCE(p.salario::bigint, 0)
                                       AS ingreso_publico_total
FROM staging.hacienda_funcionarios h
-- FROM local.controlciudadano.hacienda_funcionarios h  -- Spark
JOIN staging.sfp s
-- JOIN local.controlciudadano.sfp s
    ON h.documento = s.documento
   AND h.anio = s.anio
   AND h.mes  = s.mes
JOIN staging.policia p
-- JOIN local.controlciudadano.policia p
    ON h.documento = p.cedula
   AND h.anio = p.anio
   AND h.mes  = p.mes
WHERE h.anio = :anio
  AND h.mes  = :mes
ORDER BY ingreso_publico_total DESC;
