from __future__ import annotations
import sys
import os
from pathlib import Path
import uuid
import pandas as pd
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_timestamp,
    substring, expr, broadcast
)

# =========================
# Config
# =========================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_SOURCE_ROOT = PROJECT_ROOT / "data_source"
CSV_DIR = DATA_SOURCE_ROOT / "csv"
MANIFEST_PATH = CSV_DIR / "_manifest.csv"

BRONZE_ROOT = PROJECT_ROOT / "lakehouse" / "bronze"
TABLE_NAME = "boi_gordo"   # ajuste para ipca etc.
DELTA_PATH = BRONZE_ROOT / TABLE_NAME

# Schema do CSV:
CSV_SCHEMA = StructType([
    StructField("mes_ano", StringType(), nullable=False),  # "MM/YYYY"
    StructField("valor", DoubleType(), nullable=True),
])

def get_spark(app_name: str = "bronze_ingest_delta") -> SparkSession:
    """
    Cria o SparkSession com Delta habilitado e Python local dento do Environment
    """
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")) # Local
        
    spark = configure_spark_with_delta_pip(builder).getOrCreate()  
    return spark
    
def delta_exists() -> bool:
    return (DELTA_PATH / "_delta_log").exists()

def read_manifest_df() -> pd.DataFrame:
    if not MANIFEST_PATH.exists():
        raise FileNotFoundError(f"Manifest não encontrado em: {MANIFEST_PATH}")
    
    df = pd.read_csv(MANIFEST_PATH)
    required = {"file_hash", "source_file", "csv_file", "converted_at_utc"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Manifest sem colunas obrigatórias: {missing}")
    return df

def list_new_manifest_rows(spark: SparkSession, manifest_pd: pd.DataFrame) -> pd.DataFrame:
    """
    Retorna só os itens do manifest cujo file_hash ainda NÃO existe na Delta Bronze.
    Isso garante idempotência sem depender só do CSV.
    """
    # Se a Delta ainda não existe, tudo é novo
    if not DELTA_PATH.exists():
        return manifest_pd

    bronze_df = spark.read.format("delta").load(str(DELTA_PATH))
    ingested_hashes = [r["file_hash"] for r in bronze_df.select("file_hash").distinct().collect()]
    ingested_hashes = set(ingested_hashes)

    return manifest_pd[~manifest_pd["file_hash"].isin(ingested_hashes)]

def read_csvs_with_lineage(spark: SparkSession,manifest_subset: pd.DataFrame,ingest_run_id: str
    ) -> DataFrame:
    """
    Lê os CSVs e enriquece com colunas técnicas a partir do manifest.
    """
    if manifest_subset.empty:
        return spark.createDataFrame([], schema=CSV_SCHEMA)  # vazio

    # Lê todos os CSVs de uma vez (melhor que loop)
    csv_files = [str(CSV_DIR / f) for f in manifest_subset["csv_file"].astype(str).tolist()]

    df = (
        spark.read
        .schema(CSV_SCHEMA)
        .option("header", True)
        .option("mode", "FAILFAST")
        .csv(csv_files)
    )

    # Reconstroi o nome do arquivo de origem (csv_file) para join com manifest
    # input_file_name() seria outra alternativa; aqui usamos isso via expr:
    df = df.withColumn("csv_path", expr("input_file_name()"))
    df = df.withColumn("csv_file", expr("regexp_extract(csv_path, '([^\\\\/]+)$', 1)"))

    # Manifest no Spark para join (pequeno -> broadcast)
    manifest_sdf = spark.createDataFrame(manifest_subset)
    manifest_sdf = manifest_sdf.select(
        col("csv_file").cast("string"),
        col("source_file").cast("string"),
        col("file_hash").cast("string"),
        col("converted_at_utc").cast("string")
    )

    df = (
        df.join(manifest_sdf, on="csv_file", how="left")
          .drop("csv_path")
    )

    # Colunas técnicas
    df = (
        df.withColumn("ingest_run_id", lit(ingest_run_id))
          .withColumn("ingested_at_utc", current_timestamp())
          .withColumn("converted_at_utc", to_timestamp(col("converted_at_utc")))
          .withColumn("mes", substring(col("mes_ano"), 1, 2).cast("int"))
          .withColumn("ano", substring(col("mes_ano"), 4, 4).cast("int"))
           # Deriva ano/mes p/ particionar e facilitar query / # mes_ano = "MM/YYYY"
    )

    return df

def quality_checks(df: DataFrame) -> None:
    """
    Checagens simples (Bronze não é “curada”, mas não pode ser lixo total).
    """
    # mes_ano no formato básico
    invalid_mes_ano = df.filter(
        ~col("mes_ano").rlike(r"^(0[1-9]|1[0-2])/[0-9]{4}$")
    ).count()

    if invalid_mes_ano > 0:
        raise ValueError(f"Falha de qualidade: {invalid_mes_ano} linha(s) com mes_ano inválido.")

def write_bronze_delta(df: DataFrame) -> None:
    if df.rdd.isEmpty():
        return

    # Dedupe de segurança (mes_ano + file_hash costuma ser suficiente)
    df = df.dropDuplicates(["mes_ano", "file_hash"])

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("ano", "mes")
        .save(str(DELTA_PATH))
    )

def main():
    spark = get_spark()
    ingest_run_id = str(uuid.uuid4())

    manifest_pd = read_manifest_df()
    manifest_new = list_new_manifest_rows(spark, manifest_pd)

    if manifest_new.empty:
        print("Nada novo para ingerir (todos os hashes já estão na Bronze).")
        return

    df = read_csvs_with_lineage(spark, manifest_new, ingest_run_id=ingest_run_id)

    # Checagens mínimas
    quality_checks(df)

    # Write
    write_bronze_delta(df)

    print(f"Ingestão Bronze concluída. Registros ingeridos: {df.count()}")
    print(f"Delta path: {DELTA_PATH}")

if __name__ == "__main__":
    main()
