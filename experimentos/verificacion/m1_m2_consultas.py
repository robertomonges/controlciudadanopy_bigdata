import os
import json
import statistics
import sys
import time
from datetime import datetime
from pathlib import Path

CODIGO = Path(os.environ.get("CC_ROOT", "."), "scripts/benchmark")
sys.path.insert(0, str(CODIGO))
import run_benchmark_djbr as b

import psycopg2

N = 5
SALIDA = Path(__file__).with_name('resultados_m1_m2.json')
T = b.PG_TABLE
UNIQUE = 'djbr_official_data_raw_data_id_key'
GIN = 'djbr_official_parsed_gin_verif'


def resumen(tiempos, conteo=None):
    s = sorted(tiempos)
    return {'corridas_s': [round(x, 4) for x in tiempos], 'min_s': round(s[0], 4),
            'p50_s': round(statistics.median(s), 4), 'max_s': round(s[-1], 4),
            'fria_s': round(tiempos[0], 4), 'conteo': conteo}


def pg_medir(conn, sql, params=None):
    tiempos = b.run_pg_query(conn, sql, params or [], N)
    with conn.cursor() as cur:
        cur.execute(sql, params or None)
        filas = cur.fetchall()
    conteo = filas[0][0] if filas and len(filas[0]) == 1 else len(filas)
    return resumen(tiempos, conteo)


def ice_medir(spark, sql):
    tiempos = []
    for _ in range(N):
        t0 = time.perf_counter()
        spark.sql(sql).count()
        tiempos.append(time.perf_counter() - t0)
    filas = spark.sql(sql).collect()
    conteo = filas[0][0] if len(filas) == 1 and len(filas[0]) == 1 else len(filas)
    return resumen(tiempos, conteo)


def main():
    res = {'fecha': datetime.now().isoformat(timespec='seconds'), 'repeticiones': N, 'tabla_pg': T,
           'tabla_iceberg': b.ICE_TABLE}
    conn = psycopg2.connect(**b.PG_DEFAULTS)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM {T}')
        res['filas_pg'] = cur.fetchone()[0]
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname='benchmark' "
                    "AND tablename='djbr_official_data'")
        res['indices_antes'] = sorted(r[0] for r in cur.fetchall())
    secundarios_antes = b.secondary_indexes_exist(conn)

    q_e1 = b.QUERIES['Q-E1']
    q_g3 = b.QUERIES['Q-G3']
    try:
        b.drop_secondary_indexes(conn)
        with conn.cursor() as cur:
            cur.execute(f'ALTER TABLE {T} DROP CONSTRAINT {UNIQUE}')
        res['M1_QE1_pg_sin_ningun_indice'] = pg_medir(conn, q_e1['pg_sql'], q_e1['pg_params'])

        res['M2_QG3_pg_contencion_sin_gin'] = pg_medir(conn, q_g3['pg_sql'])
        res['M2_QG3_pg_extraccion_sin_gin'] = pg_medir(
            conn, f"SELECT COUNT(*) FROM {T} WHERE parsed->>'tipoPresentacion' = 'DIGITAL'")

        with conn.cursor() as cur:
            t0 = time.perf_counter()
            cur.execute(f'CREATE INDEX {GIN} ON {T} USING gin (parsed jsonb_path_ops)')
            res['M2_gin_creacion_s'] = round(time.perf_counter() - t0, 2)
        res['M2_QG3_pg_contencion_con_gin'] = pg_medir(conn, q_g3['pg_sql'])
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP INDEX IF EXISTS benchmark.{GIN}')
            cur.execute("SELECT 1 FROM pg_constraint WHERE conname = %s", (UNIQUE,))
            if not cur.fetchone():
                cur.execute(f'ALTER TABLE {T} ADD CONSTRAINT {UNIQUE} UNIQUE (raw_data_id)')
        if secundarios_antes:
            b.create_secondary_indexes(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname='benchmark' "
                        "AND tablename='djbr_official_data'")
            res['indices_despues'] = sorted(r[0] for r in cur.fetchall())
        res['base_restaurada'] = res['indices_despues'] == res['indices_antes']
        conn.close()

    spark = b._build_spark()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        res['filas_iceberg'] = spark.sql(f'SELECT COUNT(*) FROM {b.ICE_TABLE}').collect()[0][0]
        res['M2_QG3_iceberg_like_original'] = ice_medir(spark, q_g3['ice_sql'])
        res['M2_QG3_iceberg_get_json_object'] = ice_medir(
            spark, f"SELECT COUNT(*) FROM {b.ICE_TABLE} "
                   f"WHERE get_json_object(parsed, '$.tipoPresentacion') = 'DIGITAL'")
    finally:
        spark.stop()

    SALIDA.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
