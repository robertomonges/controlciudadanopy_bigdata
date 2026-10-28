import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import m1_m2_consultas as v

b = v.b
T = b.PG_TABLE
CLAVE, VALOR = 'descTipoPresentacion', 'BAJA DEL CARGO'
SALIDA = Path(__file__).with_name('resultados_m2b.json')


def main():
    res = {'fecha': datetime.now().isoformat(timespec='seconds'), 'repeticiones': v.N,
           'filtro': f'{CLAVE} = {VALOR}'}
    conn = v.psycopg2.connect(**b.PG_DEFAULTS)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname='benchmark' "
                    "AND tablename='djbr_official_data'")
        res['indices_antes'] = sorted(r[0] for r in cur.fetchall())
    contencion = f"SELECT COUNT(*) FROM {T} WHERE parsed @> '{{\"{CLAVE}\": \"{VALOR}\"}}'"
    extraccion = f"SELECT COUNT(*) FROM {T} WHERE parsed->>'{CLAVE}' = '{VALOR}'"
    try:
        res['pg_contencion_sin_gin'] = v.pg_medir(conn, contencion)
        res['pg_extraccion_sin_gin'] = v.pg_medir(conn, extraccion)
        with conn.cursor() as cur:
            t0 = v.time.perf_counter()
            cur.execute(f'CREATE INDEX {v.GIN} ON {T} USING gin (parsed jsonb_path_ops)')
            res['gin_creacion_s'] = round(v.time.perf_counter() - t0, 2)
        res['pg_contencion_con_gin'] = v.pg_medir(conn, contencion)
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP INDEX IF EXISTS benchmark.{v.GIN}')
            cur.execute("SELECT indexname FROM pg_indexes WHERE schemaname='benchmark' "
                        "AND tablename='djbr_official_data'")
            res['indices_despues'] = sorted(r[0] for r in cur.fetchall())
        res['base_restaurada'] = res['indices_despues'] == res['indices_antes']
        conn.close()

    spark = b._build_spark()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        res['iceberg_like'] = v.ice_medir(
            spark, f"SELECT COUNT(*) FROM {b.ICE_TABLE} WHERE parsed LIKE '%\"{CLAVE}\":\"{VALOR}\"%' "
                   f"OR parsed LIKE '%\"{CLAVE}\": \"{VALOR}\"%'")
        res['iceberg_get_json_object'] = v.ice_medir(
            spark, f"SELECT COUNT(*) FROM {b.ICE_TABLE} WHERE get_json_object(parsed, '$.{CLAVE}') = '{VALOR}'")
    finally:
        spark.stop()

    SALIDA.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding='utf-8')
    print(json.dumps(res, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
