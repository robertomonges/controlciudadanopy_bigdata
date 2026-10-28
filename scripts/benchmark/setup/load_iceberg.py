from __future__ import annotations

import os
import argparse
import zipfile
import io
import tempfile
from pathlib import Path

import sys as _sys
import os as _os
_sys.path.insert(0, str(Path(__file__).parent.parent))
import _spark_windows

import psycopg2
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, LongType, StringType, StructField, StructType, DateType,
)


LOCAL_DATA_ROOT = Path(os.environ.get("CC_ROOT", "."), ".local/data")
DEFAULT_WAREHOUSE = Path(os.environ.get("CC_ROOT", "."), ".local/iceberg_warehouse")
CATALOG = "local"
PG = dict(host="localhost", port=5432, dbname="controlciudadano", user="postgres", password=os.environ.get("PGPASSWORD", ""))


def build_spark(warehouse: Path, codec: str = "zstd", parallelism: int = 4) -> SparkSession:
    return (
        SparkSession.builder
        .appName("controlciudadano-iceberg-load")
        .master(f"local[{parallelism}]")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "8")
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", warehouse.as_posix())
        .config("spark.sql.defaultCatalog", CATALOG)
        .config("spark.sql.iceberg.write.parquet.compression-codec", codec)
        .config("spark.driver.extraJavaOptions", "-Dhadoop.home.dir=C:/hadoop")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


HACIENDA_SCHEMA = StructType([
    StructField("anio",                       IntegerType(), True),
    StructField("mes",                        IntegerType(), True),
    StructField("codigonivel",                IntegerType(), True),
    StructField("descripcionnivel",           StringType(),  True),
    StructField("codigoentidad",              IntegerType(), True),
    StructField("descripcionentidad",         StringType(),  True),
    StructField("codigoprograma",             IntegerType(), True),
    StructField("descripcionprograma",        StringType(),  True),
    StructField("codigosubprograma",          IntegerType(), True),
    StructField("descripcionsubprograma",     StringType(),  True),
    StructField("codigoproyecto",             IntegerType(), True),
    StructField("descripcionproyecto",        StringType(),  True),
    StructField("codigounidadresponsable",    IntegerType(), True),
    StructField("descripcionunidadresponsable", StringType(), True),
    StructField("codigoobjetogasto",          IntegerType(), True),
    StructField("conceptogasto",              StringType(),  True),
    StructField("fuentefinanciamiento",       StringType(),  True),
    StructField("linea",                      IntegerType(), True),
    StructField("codigopersona",              StringType(),  True),
    StructField("nombres",                    StringType(),  True),
    StructField("apellidos",                  StringType(),  True),
    StructField("sexo",                       StringType(),  True),
    StructField("discapacidad",               StringType(),  True),
    StructField("codigocategoria",            StringType(),  True),
    StructField("cargo",                      StringType(),  True),
    StructField("horascatedra",               StringType(),  True),
    StructField("fechaingreso",               StringType(),  True),
    StructField("tipopersonal",               StringType(),  True),
    StructField("lugar",                      StringType(),  True),
    StructField("montopresupuestado",         LongType(),    True),
    StructField("montodevengado",             LongType(),    True),
    StructField("mescorte",                   IntegerType(), True),
    StructField("aniocorte",                  IntegerType(), True),
    StructField("fechacorte",                 StringType(),  True),
    StructField("nivelabr",                   StringType(),  True),
    StructField("entidadabr",                 StringType(),  True),
    StructField("programaabr",                StringType(),  True),
    StructField("subprogramaabr",             StringType(),  True),
    StructField("proyectoabr",                StringType(),  True),
    StructField("unidadabr",                  StringType(),  True),
    StructField("documento",                  StringType(),  True),
])


def load_hacienda(spark: SparkSession, data_dir: Path) -> None:
    import tempfile, zipfile, shutil

    csv_paths: list[str] = []
    tmp_dirs: list[str] = []

    for zip_path in sorted(data_dir.glob("nomina_*.zip")):
        tmp_dir = tempfile.mkdtemp(prefix="hacienda_")
        tmp_dirs.append(tmp_dir)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                for member in zf.infolist():
                    try:
                        zf.extract(member, tmp_dir)
                    except Exception as e:
                        print(f"[hacienda] SKIP entry {member.filename} en {zip_path.name}: {e}")
        except (zipfile.BadZipFile, OSError) as e:
            print(f"[hacienda] SKIP {zip_path.name}: {e}")
            continue
        extracted_csvs = list(Path(tmp_dir).glob("**/*.csv"))
        for extracted in extracted_csvs:
            csv_paths.append(extracted.as_posix())
        if extracted_csvs:
            print(f"[hacienda] Extraído: {len(extracted_csvs)} CSV(s) de {zip_path.name}")
        else:
            print(f"[hacienda] SKIP {zip_path.name}: no contiene CSVs")

    for csv_path in sorted(data_dir.glob("nomina_*.csv")):
        csv_paths.append(csv_path.as_posix())

    if not csv_paths:
        print(f"[hacienda] No se encontraron archivos en {data_dir}")
        return

    print(f"[hacienda] Procesando {len(csv_paths)} archivo(s)...")

    df_raw = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("nullValue", "")
        .csv(csv_paths)
    )

    rename_map = {c: c.lower() for c in df_raw.columns}
    df = df_raw.toDF(*[rename_map[c] for c in df_raw.columns])

    from pyspark.sql.functions import col as _col
    for field in HACIENDA_SCHEMA.fields:
        if field.name in df.columns:
            df = df.withColumn(field.name, _col(field.name).cast(field.dataType))

    df = df.withColumn(
        "documento",
        F.regexp_replace(F.col("codigopersona"), r"[^0-9]", ""),
    )

    row_count = df.count()
    print(f"[hacienda] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.hacienda_funcionarios",
        partition_by=["anio", "mes"],
        description="hacienda",
    )

    for tmp_dir in tmp_dirs:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def load_sfp(spark: SparkSession, data_dir: Path) -> None:
    import tempfile, zipfile, shutil
    sfp_dir = data_dir.parent / "sfp" if data_dir.name != "sfp" else data_dir
    data_root = data_dir.parent if data_dir.name == "sfp" else data_dir

    csv_paths: list[str] = []
    tmp_dirs: list[str] = []

    if sfp_dir.exists():
        csv_paths += [p.as_posix() for p in sorted(sfp_dir.glob("funcionarios_*.csv"))]

    zip_candidates: list[Path] = []
    seen_zip_paths: set[str] = set()
    for zip_root in (sfp_dir, data_root):
        if not zip_root.exists():
            continue
        for zip_path in sorted(zip_root.glob("sfp_*.zip")):
            normalized = str(zip_path.resolve())
            if normalized in seen_zip_paths:
                continue
            seen_zip_paths.add(normalized)
            zip_candidates.append(zip_path)

    for zip_path in zip_candidates:
        tmp_dir = tempfile.mkdtemp(prefix="sfp_")
        tmp_dirs.append(tmp_dir)
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(tmp_dir)
            for extracted in Path(tmp_dir).glob("**/*.csv"):
                csv_paths.append(extracted.as_posix())
                print(f"[sfp] Extraído: {extracted.name} de {zip_path.name}")
        except Exception as e:
            print(f"[sfp] SKIP {zip_path.name}: {e}")

    if not csv_paths:
        print(f"[sfp] No se encontraron archivos CSV en {sfp_dir} ni ZIPs en {data_root}")
        for tmp_dir in tmp_dirs:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        return

    print(f"[sfp] Procesando {len(csv_paths)} archivo(s)...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("nullValue", "")
        .csv(csv_paths)
    )

    df = df.toDF(*[c.lower() for c in df.columns])

    row_count = df.count()
    print(f"[sfp] {row_count:,} filas leídas")

    partition_cols = ["anio", "mes"] if "anio" in df.columns else []
    _write_iceberg(
        spark, df, "controlciudadano.sfp",
        partition_by=partition_cols,
        description="sfp",
    )
    for tmp_dir in tmp_dirs:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def load_set(spark: SparkSession, data_dir: Path) -> None:
    set_dir = data_dir.parent / "set" if data_dir.name != "set" else data_dir
    if not set_dir.exists():
        print(f"[set] Directorio no encontrado: {set_dir}")
        return

    csv_paths = [p.as_posix() for p in sorted(set_dir.glob("**/*.csv"))]
    csv_paths += [p.as_posix() for p in sorted(set_dir.glob("**/*.txt"))]
    if not csv_paths:
        print(f"[set] No se encontraron CSVs/TXTs en {set_dir}")
        return

    print(f"[set] Procesando {len(csv_paths)} archivo(s)...")
    df = (
        spark.read
        .option("header", "false")
        .option("encoding", "UTF-8")
        .option("sep", "|")
        .option("nullValue", "")
        .csv(csv_paths)
    )
    set_columns = ["ruc", "razon_social", "tipo_contribuyente", "ruc_viejo", "estado", "extra"]
    if len(df.columns) == len(set_columns):
        df = df.toDF(*set_columns)
    else:
        df = df.toDF(*[f"col{i}" for i in range(len(df.columns))])

    row_count = df.count()
    print(f"[set] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.set_ruc",
        partition_by=[],
        description="set",
    )


def load_pytyvo(spark: SparkSession, data_dir: Path) -> None:
    pytyvo_dir = data_dir.parent / "pytyvo" if data_dir.name != "pytyvo" else data_dir
    if not pytyvo_dir.exists():
        print(f"[pytyvo] Directorio no encontrado: {pytyvo_dir}")
        return

    csv_paths = [p.as_posix() for p in sorted(pytyvo_dir.glob("**/*.csv"))]
    if not csv_paths:
        print(f"[pytyvo] No se encontraron CSVs en {pytyvo_dir}")
        return

    print(f"[pytyvo] Procesando {len(csv_paths)} archivo(s)...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ";")
        .option("nullValue", "")
        .option("escape", "\"")
        .csv(csv_paths)
    )
    col_map = {
        c.lower(): c.lower() for c in df.columns
    }
    rename = {}
    for c in df.columns:
        cl = c.lower().strip()
        if "cic" in cl:
            rename[c] = "cid"
        elif "nombre" in cl:
            rename[c] = "name"
        elif "departamento" in cl:
            rename[c] = "department"
        elif "distrito" in cl:
            rename[c] = "district"
        else:
            rename[c] = cl
    df = df.toDF(*[rename.get(c, c.lower()) for c in df.columns])

    row_count = df.count()
    print(f"[pytyvo] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.pytyvo",
        partition_by=[],
        description="pytyvo",
    )


def load_policia(spark: SparkSession, data_dir: Path) -> None:
    policia_dir = data_dir.parent / "policia" if data_dir.name != "policia" else data_dir
    if not policia_dir.exists():
        print(f"[policia] Directorio no encontrado: {policia_dir}")
        return

    csv_paths = [p.as_posix() for p in sorted(policia_dir.glob("**/*.csv"))]
    if not csv_paths:
        print(f"[policia] No se encontraron CSVs en {policia_dir}")
        return

    print(f"[policia] Procesando {len(csv_paths)} archivo(s)...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("nullValue", "")
        .csv(csv_paths)
    )
    df = df.toDF(*[c.lower() for c in df.columns])

    row_count = df.count()
    print(f"[policia] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.policia",
        partition_by=[],
        description="policia",
    )


def load_nangareko(spark: SparkSession, data_dir: Path) -> None:
    del data_dir

    with tempfile.TemporaryDirectory(prefix="nangareko_iceberg_") as tmpdir:
        csv_path = Path(tmpdir) / "staging_nangareko.csv"
        print(f"[nangareko] Exportando staging.nangareko -> {csv_path}")

        with psycopg2.connect(**PG) as conn:
            with conn.cursor() as cur:
                with csv_path.open("w", encoding="utf-8", newline="") as handle:
                    cur.copy_expert(
                        """
                        COPY (
                            SELECT cid, name, department, district
                            FROM staging.nangareko
                            ORDER BY cid, name
                        ) TO STDOUT WITH CSV HEADER
                        """,
                        handle,
                    )

        df = (
            spark.read
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("sep", ",")
            .option("nullValue", "")
            .csv(csv_path.as_posix())
        )
        df = df.toDF(*[c.lower() for c in df.columns])

        row_count = df.count()
        print(f"[nangareko] {row_count:,} filas leídas")

        _write_iceberg(
            spark, df, "controlciudadano.nangareko",
            partition_by=[],
            description="nangareko",
        )


def load_nangareko_transparencia(spark: SparkSession, data_dir: Path) -> None:
    csv_path = data_dir.parent / "nangareko" / "subsidios_zip" / "Subsidios.csv"
    if not csv_path.exists():
        print(f"[nangareko_transparencia] Archivo no encontrado: {csv_path}")
        return

    print(f"[nangareko_transparencia] Leyendo {csv_path.name} ...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ";")
        .option("nullValue", "")
        .csv(csv_path.as_posix())
    )
    rename = {
        "\ufeffNivel": "nivel", "Nivel": "nivel",
        "Código entidad": "codigo_entidad", "CÃ³digo entidad": "codigo_entidad",
        "Entidad": "entidad",
        "Clase programa": "clase_programa",
        "Código programa": "codigo_programa", "CÃ³digo programa": "codigo_programa",
        "Nombre programa": "nombre_programa",
        "Código proyecto actividad": "codigo_proyecto_actividad",
        "CÃ³digo proyecto actividad": "codigo_proyecto_actividad",
        "Nombre actividad proyecto": "nombre_actividad_proyecto",
        "Tipo subsidio programa": "tipo_subsidio_programa",
        "Número documento": "numero_documento", "NÃºmero documento": "numero_documento",
        "Nombre beneficiario": "nombre_beneficiario",
        "Id departamento": "id_departamento",
        "Nombre departamento": "nombre_departamento",
        "Id municipio": "id_municipio",
        "Nombre municipio": "nombre_municipio",
        "Característica subsidio": "caracteristica_subsidio",
        "CaracterÃ­stica subsidio": "caracteristica_subsidio",
        "Nombre subsidio": "nombre_subsidio",
        "Valor subsidio": "valor_subsidio",
        "Cantidad subsidio": "cantidad_subsidio",
        "Fecha cargue archivo": "fecha_cargue_archivo",
        "Fuente datos": "fuente_datos",
    }
    df = df.toDF(*[rename.get(c, c.lower()) for c in df.columns])

    row_count = df.count()
    print(f"[nangareko_transparencia] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.nangareko_transparencia",
        partition_by=[],
        description="nangareko_transparencia",
    )


def load_ande_exonerados(spark: SparkSession, data_dir: Path) -> None:
    csv_path = data_dir.parent / "ande_exonerados.normalized.csv"
    if not csv_path.exists():
        print(f"[ande_exonerados] Archivo no encontrado: {csv_path}")
        return

    print(f"[ande_exonerados] Leyendo {csv_path.name} ...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("nullValue", "")
        .csv(csv_path.as_posix())
    )
    df = df.toDF(*[c.lower() for c in df.columns])

    row_count = df.count()
    print(f"[ande_exonerados] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.ande_exonerados",
        partition_by=[],
        description="ande_exonerados",
    )


def load_tsje_elected(spark: SparkSession, data_dir: Path) -> None:
    csv_path = data_dir.parent / "tsje_elected.csv"
    if not csv_path.exists():
        print(f"[tsje_elected] Archivo no encontrado: {csv_path}")
        return

    print(f"[tsje_elected] Leyendo {csv_path.name} ...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("nullValue", "")
        .csv(csv_path.as_posix())
    )
    df = df.toDF(*[c.lower() for c in df.columns])

    row_count = df.count()
    print(f"[tsje_elected] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.tsje_elected",
        partition_by=[],
        description="tsje_elected",
    )


def load_a_quien_elegimos(spark: SparkSession, data_dir: Path) -> None:
    csv_path = data_dir.parent / "a_quien_elegimos.csv"
    if not csv_path.exists():
        print(f"[a_quien_elegimos] Archivo no encontrado: {csv_path}")
        return

    print(f"[a_quien_elegimos] Leyendo {csv_path.name} ...")
    df = (
        spark.read
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("sep", ",")
        .option("nullValue", "")
        .csv(csv_path.as_posix())
    )
    df = df.toDF(*[c.lower() for c in df.columns])

    row_count = df.count()
    print(f"[a_quien_elegimos] {row_count:,} filas leídas")

    _write_iceberg(
        spark, df, "controlciudadano.a_quien_elegimos",
        partition_by=[],
        description="a_quien_elegimos",
    )


def _write_iceberg(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    partition_by: list[str],
    description: str,
) -> None:
    fq_table = f"{CATALOG}.{table_name}"
    print(f"[{description}] Escribiendo en {fq_table}  partición={partition_by or 'ninguna'}")

    writer = df.writeTo(fq_table).using("iceberg")

    if partition_by:
        from pyspark.sql.functions import col
        writer = writer.partitionedBy(*[col(c) for c in partition_by if c in df.columns])

    writer.createOrReplace()

    print(f"[{description}] OK -> {fq_table}")


SOURCES = {
    "hacienda":                 load_hacienda,
    "sfp":                      load_sfp,
    "set":                      load_set,
    "pytyvo":                   load_pytyvo,
    "policia":                  load_policia,
    "nangareko":                load_nangareko,
    "nangareko_transparencia":  load_nangareko_transparencia,
    "ande_exonerados":          load_ande_exonerados,
    "tsje_elected":             load_tsje_elected,
    "a_quien_elegimos":         load_a_quien_elegimos,
}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Carga fuentes CSV de Control Ciudadano en Apache Iceberg (Spark local)."
    )
    parser.add_argument(
        "--source",
        choices=[*SOURCES.keys(), "all"],
        default="all",
        help="Fuente a cargar (default: all)",
    )
    parser.add_argument(
        "--warehouse",
        default=str(DEFAULT_WAREHOUSE),
        help=f"Ruta del warehouse Iceberg (default: {DEFAULT_WAREHOUSE})",
    )
    parser.add_argument(
        "--data-dir",
        default=str(LOCAL_DATA_ROOT),
        help=f"Directorio raíz de datos locales (default: {LOCAL_DATA_ROOT})",
    )
    parser.add_argument(
        "--codec",
        choices=["zstd", "snappy", "gzip", "none"],
        default="zstd",
        help="Codec de compresión Parquet (default: zstd) — use snappy para E1.3",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=4,
        help="Número de cores Spark local (default: 4)",
    )
    args = parser.parse_args()

    warehouse = Path(args.warehouse)
    data_dir = Path(args.data_dir)
    warehouse.mkdir(parents=True, exist_ok=True)

    spark = build_spark(warehouse, codec=args.codec, parallelism=args.parallelism)
    spark.sparkContext.setLogLevel("WARN")

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.controlciudadano")

    sources_to_load = list(SOURCES.keys()) if args.source == "all" else [args.source]
    for source_name in sources_to_load:
        source_data_dir = data_dir / source_name
        print(f"\n{'='*60}")
        print(f"  Cargando fuente: {source_name}")
        print(f"{'='*60}")
        SOURCES[source_name](spark, source_data_dir)

    print("\nCarga en Iceberg completada.")
    spark.stop()


if __name__ == "__main__":
    main()
