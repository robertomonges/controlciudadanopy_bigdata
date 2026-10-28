import os
import argparse
import contextlib
import io
import json
import shutil
import sys
import time
from datetime import datetime
from pathlib import Path

AQUI = Path(__file__).resolve().parent
sys.path.insert(0, str(AQUI))
sys.path.insert(0, os.path.join(os.environ.get("CC_ROOT", "."), "scripts/benchmark"))
sys.path.insert(0, os.path.join(os.environ.get("CC_ROOT", "."), "scripts/python/local"))

import m3_cargas_iceberg as m3
with contextlib.redirect_stdout(io.StringIO()):
    import run_benchmark_djbr as b
import hacienda_employees_etl as etl_hacienda
import sfp_etl as etl_sfp
import psycopg2

DATOS_M6 = m3.CODIGO / '.local/data_verif_m6'
SALIDA = AQUI / 'resultados_m6.json'
ESQUEMA = 'verif_m6'
MIN_LIBRE_GB = 12
TABLA_REAL = {'sfp': 'staging.sfp', 'hacienda': 'staging.hacienda_funcionarios'}
LOTE = {'sfp': 3000, 'hacienda': 10000}
MUESTRA = {'sfp': 10, 'hacienda': 12}
PUBLICADO = {'sfp': {'filas': 129_445_835, 'segundos': 39_484},
             'hacienda': {'filas': 138_393_256, 'segundos': 16_953}}


def log(msg):
    print(f'[{datetime.now():%H:%M:%S}] {msg}', flush=True)


def libre_gb():
    return shutil.disk_usage('C:/').free / 1e9


def archivos(fuente):
    if fuente == 'hacienda':
        lista = []
        for anio, mes in m3.meses_hacienda():
            zp = m3.DATOS / 'hacienda' / f'nomina-{anio}-{mes:02d}.zip'
            if not zp.exists():
                zp = m3.DATOS / 'hacienda' / f'nomina_{anio}-{mes:02d}.zip'
            lista.append((zp, f'nomina_{anio}-{mes:02d}.csv'))
        lista += [(m3.DATOS / 'hacienda' / f'nomina_2020-{mes:02d}.zip',
                   f'nomina_2020-{mes:02d}_segunda_copia.csv') for mes in range(4, 9)]
        return lista
    lista = [(zp, None) for zp in sorted((m3.DATOS / 'sfp').glob('sfp_*.zip'))]
    lista.append((m3.DATOS / 'sfp_2024_10.zip', 'funcionarios_2024_10_segunda_copia.csv'))
    return lista


def extraer(zp, nombre):
    DATOS_M6.mkdir(parents=True, exist_ok=True)
    try:
        return m3.extraer(zp, DATOS_M6, nombre)
    except Exception as exc:
        log(f'SKIP {zp.name}: {exc}')
        return None


def crear_tabla(cur, fuente, sufijo):
    tabla = f'{ESQUEMA}.{fuente}_{sufijo}'
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS {ESQUEMA}')
    cur.execute(f'DROP TABLE IF EXISTS {tabla}')
    cur.execute(f'DROP SEQUENCE IF EXISTS {tabla}_id_seq')
    cur.execute(f'CREATE TABLE {tabla} (LIKE {TABLA_REAL[fuente]} INCLUDING CONSTRAINTS INCLUDING INDEXES)')
    cur.execute(f'CREATE SEQUENCE {tabla}_id_seq OWNED BY {tabla}.id')
    cur.execute(f"ALTER TABLE {tabla} ALTER COLUMN id SET DEFAULT nextval('{tabla}_id_seq')")
    return tabla


def columnas_copy(fuente, csv_path):
    with open(csv_path, 'r', encoding='utf-8', errors='replace') as f:
        cab = f.readline().strip()
    cols = [c.lower().strip('"').strip() for c in cab.split(',')]
    return cols


def cargar_copy(conn, tabla, fuente, csv_path):
    cols = columnas_copy(fuente, csv_path)
    sql = f"COPY {tabla} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, DELIMITER ',', NULL '')"
    with open(csv_path, 'r', encoding='utf-8', errors='replace', newline='') as f:
        f.readline()
        with conn.cursor() as cur:
            cur.copy_expert(sql, f)
    conn.commit()


def cargar_insert(conn, tabla, fuente, csv_path):
    lote = LOTE[fuente]
    if fuente == 'sfp':
        cols = etl_sfp.SFP_COLUMNS
        import csv
        with open(csv_path, 'r', encoding='latin1', newline='') as f:
            filas = [etl_sfp.coerce_row(r) for r in csv.DictReader(f)]
    else:
        cols = etl_hacienda.CSV_COLUMNS
        with contextlib.redirect_stdout(io.StringIO()):
            filas = [etl_hacienda.normalize_row(r) for r in etl_hacienda.batch_rows(csv_path, lote)]
    sql = f"INSERT INTO {tabla} ({', '.join(cols)}) VALUES ({', '.join(['%s'] * len(cols))})"
    with conn.cursor() as cur:
        for i in range(0, len(filas), lote):
            cur.executemany(sql, filas[i:i + lote])
            conn.commit()


CARGADORES = {'copy': cargar_copy, 'insert_lotes': cargar_insert}


def filas_tabla(cur, tabla):
    cur.execute(f'SELECT last_value, is_called FROM {tabla}_id_seq')
    v, llamado = cur.fetchone()
    return int(v) if llamado else 0


def medir(fuente, mecanismo, n_archivos):
    lista = archivos(fuente)[:n_archivos] if n_archivos else archivos(fuente)
    conn = psycopg2.connect(**b.PG_DEFAULTS)
    conn.autocommit = False
    with conn.cursor() as cur:
        tabla = crear_tabla(cur, fuente, mecanismo)
    conn.commit()
    detalle, total, previo = [], 0.0, 0
    try:
        for zp, nombre in lista:
            if libre_gb() < MIN_LIBRE_GB:
                raise RuntimeError(f'disco con menos de {MIN_LIBRE_GB} GB libres; se aborta')
            csv_path = extraer(zp, nombre)
            if csv_path is None:
                continue
            t0 = time.perf_counter()
            CARGADORES[mecanismo](conn, tabla, fuente, csv_path)
            dt = time.perf_counter() - t0
            csv_path.unlink()
            with conn.cursor() as cur:
                acum = filas_tabla(cur, tabla)
            total += dt
            detalle.append({'archivo': csv_path.name, 'filas': acum - previo, 'segundos': round(dt, 2)})
            log(f'{fuente} {mecanismo} {csv_path.name}: {acum - previo:,} filas en {dt:.1f} s '
                f'(acumulado {acum:,} filas, {total:.0f} s, {acum / total:,.0f} filas/s)')
            previo = acum
        with conn.cursor() as cur:
            cur.execute(f"SELECT pg_total_relation_size('{tabla}')")
            tam = cur.fetchone()[0]
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS {tabla}')
        conn.commit()
        conn.close()
    return {'fuente': fuente, 'mecanismo': mecanismo, 'archivos': len(detalle), 'filas': previo,
            'segundos': round(total, 1), 'filas_s': round(previo / total, 1) if total else None,
            'bytes_tabla': tam, 'detalle': detalle, 'fecha': datetime.now().isoformat(timespec='seconds')}


def guardar(r):
    res = json.loads(SALIDA.read_text(encoding='utf-8')) if SALIDA.exists() else {
        'publicado': PUBLICADO, 'mediciones': []}
    res['mediciones'].append(r)
    SALIDA.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding='utf-8')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('fase', choices=['muestra', 'completa'])
    ap.add_argument('--fuente', choices=['sfp', 'hacienda'])
    ap.add_argument('--mecanismo', choices=list(CARGADORES), default='copy')
    a = ap.parse_args()
    log(f'disco libre: {libre_gb():.0f} GB')
    if a.fase == 'muestra':
        for fuente in ([a.fuente] if a.fuente else ['sfp', 'hacienda']):
            for mec in CARGADORES:
                r = medir(fuente, mec, MUESTRA[fuente])
                r['fase'] = 'muestra'
                guardar(r)
                log(f'== {fuente} {mec}: {r["filas"]:,} filas en {r["segundos"]} s ({r["filas_s"]:,} filas/s)')
    else:
        r = medir(a.fuente, a.mecanismo, None)
        r['fase'] = 'completa'
        guardar(r)
        p = PUBLICADO[a.fuente]
        log(f'== {a.fuente} {a.mecanismo} completa: {r["filas"]:,} filas en {r["segundos"]} s; '
            f'publicado {p["filas"]:,} filas en {p["segundos"]:,} s')
    shutil.rmtree(DATOS_M6, ignore_errors=True)


if __name__ == '__main__':
    main()
