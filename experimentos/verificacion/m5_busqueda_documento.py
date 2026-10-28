import os
import hashlib
import json
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path

CODIGO = Path(os.environ.get("CC_ROOT", "."), "scripts/benchmark")
sys.path.insert(0, str(CODIGO))
import run_benchmark_djbr as b

import psycopg2

N = 5
DOCUMENTOS = 5
SALIDA = Path(__file__).with_name('resultados_m5.json')

TABLAS = {
    'pytyvo': ("cid", "cid"),
    'nangareko': ("cid", "cid"),
    'nangareko_transparencia': ("numero_documento", "numero_documento"),
    'hacienda_funcionarios': ("codigopersona", "codigopersona"),
    'sfp': ("documento", "documento"),
    'policia': ("cedula", "cedula"),
    'ande_exonerados': ("regexp_replace(documento, '[^0-9]+', '', 'g')",
                        "regexp_replace(documento, '[^0-9]+', '')"),
    'a_quien_elegimos': ("regexp_replace(identifier, '\\.', '', 'g')",
                         "regexp_replace(identifier, '\\\\.', '')"),
}


def huella(doc):
    return hashlib.sha256(doc.encode()).hexdigest()[:10]


def resumen(tiempos):
    s = sorted(tiempos)
    return {'corridas_s': [round(x, 4) for x in tiempos], 'min_s': round(s[0], 4),
            'p50_s': round(statistics.median(s), 4), 'max_s': round(s[-1], 4),
            'fria_s': round(tiempos[0], 4)}


def elegir_documentos(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT setseed(0.42)")
        cur.execute("""
            SELECT documento FROM (
                SELECT DISTINCT documento FROM staging.sfp TABLESAMPLE SYSTEM (0.05) REPEATABLE (42)
                WHERE documento ~ '^[0-9]{5,9}$') d
            ORDER BY random() LIMIT %s""", (DOCUMENTOS,))
        return [r[0] for r in cur.fetchall()]


def pg_una(conn, tabla, doc):
    expr = TABLAS[tabla][0]
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM staging.{tabla} WHERE {expr} = %s", (doc,))
        return len(cur.fetchall())


def pg_medir(conns, doc):
    tablas = list(TABLAS)
    sec, par, filas = [], [], {}
    with ThreadPoolExecutor(len(tablas)) as ex:
        for _ in range(N):
            t0 = time.perf_counter()
            for t in tablas:
                filas[t] = pg_una(conns[0], t, doc)
            sec.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            list(ex.map(lambda tc: pg_una(tc[1], tc[0], doc), zip(tablas, conns)))
            par.append(time.perf_counter() - t0)
    return {'secuencial': resumen(sec), 'paralelo': resumen(par), 'filas': filas}


def ice_una(spark, tabla, doc, por_tabla=None):
    expr = TABLAS[tabla][1]
    t0 = time.perf_counter()
    n = len(spark.sql(f"SELECT * FROM {b.CATALOG}.controlciudadano.{tabla} "
                      f"WHERE {expr} = '{doc}'").collect())
    if por_tabla is not None:
        por_tabla.setdefault(tabla, []).append(time.perf_counter() - t0)
    return n


def ice_medir(spark, doc):
    tablas = list(TABLAS)
    sec, par, filas, por_tabla = [], [], {}, {}
    with ThreadPoolExecutor(len(tablas)) as ex:
        for _ in range(N):
            t0 = time.perf_counter()
            for t in tablas:
                filas[t] = ice_una(spark, t, doc, por_tabla)
            sec.append(time.perf_counter() - t0)
            t0 = time.perf_counter()
            list(ex.map(lambda t: ice_una(spark, t, doc), tablas))
            par.append(time.perf_counter() - t0)
    return {'secuencial': resumen(sec), 'paralelo': resumen(par), 'filas': filas,
            'p50_por_tabla_s': {t: round(statistics.median(v), 4) for t, v in por_tabla.items()}}


def main():
    res = {'fecha': datetime.now().isoformat(timespec='seconds'), 'repeticiones': N,
           'tablas': list(TABLAS), 'documentos': []}
    conns = [psycopg2.connect(**b.PG_DEFAULTS) for _ in TABLAS]
    for c in conns:
        c.autocommit = True
    try:
        docs = elegir_documentos(conns[0])
        if len(docs) != DOCUMENTOS:
            raise SystemExit(f'Se eligieron {len(docs)} documentos, se esperaban {DOCUMENTOS}')
        for doc in docs:
            res['documentos'].append({'id': huella(doc), 'pg': pg_medir(conns, doc)})
            print(f"PG {huella(doc)}: listo", flush=True)
    finally:
        for c in conns:
            c.close()
    SALIDA.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding='utf-8')

    spark = b._build_spark()
    spark.sparkContext.setLogLevel('ERROR')
    try:
        for doc, d in zip(docs, res['documentos']):
            d['iceberg'] = ice_medir(spark, doc)
            d['filas_iguales'] = d['pg']['filas'] == d['iceberg']['filas']
            SALIDA.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding='utf-8')
            print(f"Iceberg {d['id']}: listo (filas iguales: {d['filas_iguales']})", flush=True)
    finally:
        spark.stop()
    print('M5 terminado.')


if __name__ == '__main__':
    main()
