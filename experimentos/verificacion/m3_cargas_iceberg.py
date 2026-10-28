import json
import os
import re
import shutil
import subprocess
import time
import zipfile
from datetime import datetime
from pathlib import Path

CODIGO = Path(os.environ.get("CC_ROOT", "."))
PY = CODIGO / '.venv/Scripts/python.exe'
ORIGINAL = CODIGO / '.local/iceberg_warehouse'
VERIF = CODIGO / '.local/iceberg_warehouse_verif_m3'
DATOS = CODIGO / '.local/data'
DATOS_VERIF = CODIGO / '.local/data_verif_m3'
SPARK_TMP = CODIGO / '.local/spark_tmp_m3'
N = 4
CORRIDAS = 3
SALIDA = Path(__file__).with_name('resultados_m3.json')
LOGS = Path(__file__).with_name('logs_m3')
FILAS_ESPERADAS = {'hacienda': 138_393_256, 'sfp': 129_445_835}

CARGAS = {
    'nangareko_transparencia': (['scripts/benchmark/setup/load_iceberg.py', '--source',
                                 'nangareko_transparencia'], 'nangareko_transparencia'),
    'pytyvo': (['scripts/benchmark/setup/load_iceberg.py', '--source', 'pytyvo'], 'pytyvo'),
    'policia': (['scripts/benchmark/setup/load_policia_iceberg.py'], 'policia'),
    'sfp': (['scripts/benchmark/setup/load_iceberg.py', '--source', 'sfp',
             '--data-dir', str(DATOS_VERIF)], 'sfp'),
    'hacienda': (['scripts/benchmark/setup/load_iceberg.py', '--source', 'hacienda',
                  '--data-dir', str(DATOS_VERIF)], 'hacienda_funcionarios'),
    'full_data': (['scripts/benchmark/run_full_data_iceberg.py'], 'full_data'),
}


def meses_hacienda():
    for anio in range(2013, 2027):
        ultimo = 13 if anio <= 2018 else (2 if anio == 2026 else 12)
        for mes in range(1, ultimo + 1):
            yield anio, mes


def extraer(zip_path, destino, nombre=None):
    with zipfile.ZipFile(zip_path) as zf:
        csvs = [m for m in zf.infolist() if m.filename.lower().endswith('.csv')]
        if len(csvs) != 1:
            raise RuntimeError(f'{zip_path.name}: {len(csvs)} CSV, se esperaba 1')
        salida = destino / (nombre or Path(csvs[0].filename).name)
        if not salida.exists():
            with zf.open(csvs[0]) as src, open(salida, 'wb') as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
    return salida


def preparar_csv(fuente):
    destino = DATOS_VERIF / fuente
    destino.mkdir(parents=True, exist_ok=True)
    for viejo in destino.glob('*.zip'):
        viejo.unlink()
    if fuente == 'hacienda':
        for anio, mes in meses_hacienda():
            zp = DATOS / 'hacienda' / f'nomina-{anio}-{mes:02d}.zip'
            if not zp.exists():
                zp = DATOS / 'hacienda' / f'nomina_{anio}-{mes:02d}.zip'
            extraer(zp, destino, f'nomina_{anio}-{mes:02d}.csv')
        for mes in range(4, 9):
            extraer(DATOS / 'hacienda' / f'nomina_2020-{mes:02d}.zip', destino,
                    f'nomina_2020-{mes:02d}_segunda_copia.csv')
        return len(list(destino.glob('nomina_*.csv')))
    for zp in sorted((DATOS / 'sfp').glob('sfp_*.zip')):
        try:
            extraer(zp, destino)
        except zipfile.BadZipFile:
            print(f'SKIP {zp.name}: no es un ZIP (el cargador original tambien lo salta)')
    extraer(DATOS / 'sfp_2024_10.zip', destino, 'funcionarios_2024_10_segunda_copia.csv')
    return len(list(destino.glob('funcionarios_*.csv')))


def cargar_resultados():
    if SALIDA.exists():
        return json.loads(SALIDA.read_text(encoding='utf-8'))
    return {'N': N, 'corridas_previstas': CORRIDAS, 'warehouse': str(VERIF), 'mediciones': []}


def guardar(res):
    SALIDA.write_text(json.dumps(res, indent=2, ensure_ascii=False), encoding='utf-8')


def medir(fuente, corrida, args, tabla):
    shutil.rmtree(VERIF / 'controlciudadano' / tabla, ignore_errors=True)
    cmd = [str(PY), *args, '--warehouse', str(VERIF), '--parallelism', str(N)]
    log = LOGS / f'{fuente}_{corrida}.log'
    SPARK_TMP.mkdir(exist_ok=True)
    env = dict(os.environ, PYTHONUNBUFFERED='1', SPARK_LOCAL_DIRS=str(SPARK_TMP))
    inicio = datetime.now().isoformat(timespec='seconds')
    t0 = time.perf_counter()
    with open(log, 'w', encoding='utf-8') as fh:
        proc = subprocess.run(cmd, cwd=CODIGO, stdout=fh, stderr=subprocess.STDOUT, text=True, env=env)
    seg = time.perf_counter() - t0
    texto = log.read_text(encoding='utf-8', errors='replace')
    filas = [int(x.replace(',', '')) for x in re.findall(r'([\d,]+) filas', texto)]
    m = {'fuente': fuente, 'corrida': corrida, 'inicio': inicio, 'segundos': round(seg, 2),
         'ok': proc.returncode == 0, 'codigo_salida': proc.returncode,
         'filas_reportadas': filas[-1] if filas else None, 'procedimiento': 'csv_extraidos'}
    if fuente in FILAS_ESPERADAS and m['filas_reportadas'] != FILAS_ESPERADAS[fuente]:
        m['invalida'] = f'filas distintas de la tabla publicada ({FILAS_ESPERADAS[fuente]:,})'
    return m


def main():
    LOGS.mkdir(exist_ok=True)
    res = cargar_resultados()
    if not VERIF.exists():
        t0 = time.perf_counter()
        shutil.copytree(ORIGINAL, VERIF)
        res['copia_warehouse_s'] = round(time.perf_counter() - t0, 1)
        guardar(res)
    hechas = {(m['fuente'], m['corrida']) for m in res['mediciones']
              if m['ok'] and not m.get('invalida')}
    for fuente, (args, tabla) in CARGAS.items():
        pendientes = [c for c in range(1, CORRIDAS + 1) if (fuente, c) not in hechas]
        if not pendientes:
            continue
        if fuente in FILAS_ESPERADAS:
            t0 = time.perf_counter()
            n = preparar_csv(fuente)
            res.setdefault('preparacion', {})[fuente] = {
                'csv': n, 'segundos_fuera_del_cronometro': round(time.perf_counter() - t0, 1)}
            guardar(res)
            print(f'{fuente}: {n} CSV extraidos', flush=True)
        for corrida in pendientes:
            m = medir(fuente, corrida, args, tabla)
            res['mediciones'].append(m)
            guardar(res)
            print(f"{fuente} corrida {corrida}: {m['segundos']:.1f} s (salida {m['codigo_salida']})", flush=True)
            if m.get('invalida') or not m['ok']:
                raise SystemExit(f"{fuente} corrida {corrida}: {m.get('invalida', 'fallo')}")
        if fuente in FILAS_ESPERADAS:
            shutil.rmtree(DATOS_VERIF / fuente)
    print('M3 terminado.')


if __name__ == '__main__':
    main()
