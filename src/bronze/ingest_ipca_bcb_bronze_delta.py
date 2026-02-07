from __future__ import annotations

from pathlib import Path
import uuid
import json
import hashlib
import requests
import sys, os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType, IntegerType
from pyspark.sql.functions import (
    col, lit, current_timestamp, to_date, regexp_replace, substring, broadcast
)

from delta import configure_spark_with_delta_pip


# =========================
# Config (fonte / destino)
# =========================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LAKEHOUSE_ROOT = PROJECT_ROOT / "lakehouse"
BRONZE_ROOT = LAKEHOUSE_ROOT / "bronze"

TABLE_NAME = "ipca"
DELTA_PATH = BRONZE_ROOT / TABLE_NAME

SOURCE = "BCB_SGS"
SERIES_ID = 433
DATA_INICIAL = "01/01/2025"
DATA_FINAL = "31/12/2025"

URL = (
    "https://api.bcb.gov.br/dados/serie/"
    f"bcdata.sgs.{SERIES_ID}/dados"
    f"?dataInicial={DATA_INICIAL}"
    f"&dataFinal={DATA_FINAL}"
    "&formato=json"
)

# Schema esperado do BCB/SGS (JSON)
# Ex.: [{"data":"01/01/2025","valor":"0,42"}, ...]
RAW_SCHEMA = StructType([
    StructField("data", StringType(), nullable=False),   # "dd/MM/yyyy"
    StructField("valor", StringType(), nullable=True),   # "0,42" (pt-BR)
])


# =========================
# Spark (Delta via pip)
# =========================

def get_spark(app_name: str = "bronze_ipca_bcb_ingestion") -> SparkSession:
    
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")  # bom para local
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def delta_exists() -> bool:
    return (DELTA_PATH / "_delta_log").exists()


# =========================
# Fonte (API) + hash
# =========================

def fetch_ipca_json(url: str) -> list[dict]:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError("API do BCB retornou dataset vazio para o período informado.")
    return data


def sha256_of_payload(payload: list[dict]) -> str:
    """
    Hash determinístico do conteúdo retornado (idempotência tipo file_hash).
    """
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def already_ingested_by_hash(spark: SparkSession, payload_hash: str) -> bool:
    if not delta_exists():
        return False
    df = spark.read.format("delta").load(str(DELTA_PATH))
    return df.filter(col("payload_hash") == lit(payload_hash)).limit(1).count() > 0


# =========================
# Transform + lineage
# =========================

def build_bronze_df(
    spark: SparkSession,
    raw: list[dict],
    ingest_run_id: str,
    payload_hash: str
) -> DataFrame:
    df = spark.createDataFrame(raw, schema=RAW_SCHEMA)

    # Converte data e valor (pt-BR -> double)
    # "0,42" -> "0.42"
    df = df.withColumn("data_ref", to_date(col("data"), "dd/MM/yyyy"))

    df = (
        df.withColumn("valor_num", regexp_replace(col("valor"), ",", ".").cast(DoubleType()))
    )

    # Deriva ano/mes (para particionamento)
    # data_ref -> ano/mes
    df = (
        df.withColumn("ano", substring(col("data"), 7, 4).cast(IntegerType()))
          .withColumn("mes", substring(col("data"), 4, 2).cast(IntegerType()))
    )

    # Linhagem / metadados técnicos
    df = (
        df.withColumn("source", lit(SOURCE))
          .withColumn("series_id", lit(int(SERIES_ID)))
          .withColumn("data_inicial", lit(DATA_INICIAL))
          .withColumn("data_final", lit(DATA_FINAL))
          .withColumn("request_url", lit(URL))
          .withColumn("payload_hash", lit(payload_hash))
          .withColumn("ingest_run_id", lit(ingest_run_id))
          .withColumn("ingested_at_utc", current_timestamp())
    )

    # Seleção final (Bronze “quase raw”, mas tipada + auditável)
    out = df.select(
        col("data_ref").cast(DateType()).alias("data_ref"),
        col("valor_num").alias("valor"),
        col("source"),
        col("series_id"),
        col("data_inicial"),
        col("data_final"),
        col("request_url"),
        col("payload_hash"),
        col("ingest_run_id"),
        col("ingested_at_utc"),
        col("ano"),
        col("mes"),
    )

    return out


def quality_checks(df: DataFrame) -> None:
    if df.rdd.isEmpty():
        return

    bad_dates = df.filter(col("data_ref").isNull()).count()
    if bad_dates > 0:
        raise ValueError(f"Qualidade: {bad_dates} linha(s) com data_ref nula (parse de data falhou).")

    # valor pode ser nulo em alguns casos (depende da fonte), mas normalmente não deveria
    # Se quiser endurecer:
    # bad_val = df.filter(col("valor").isNull()).count()
    # if bad_val > 0:
    #     raise ValueError(f"Qualidade: {bad_val} linha(s) com valor nulo.")


def write_bronze_delta(df: DataFrame) -> None:
    if df.rdd.isEmpty():
        return

    # Dedupe defensivo (mesma data/serie dentro do mesmo payload)
    df = df.dropDuplicates(["series_id", "data_ref", "payload_hash"])

    (
        df.write
        .format("delta")
        .mode("append")
        .partitionBy("ano", "mes")
        .save(str(DELTA_PATH))
    )


# =========================
# Main
# =========================

def main():
    spark = get_spark()
    ingest_run_id = str(uuid.uuid4())

    try:
        raw = fetch_ipca_json(URL)
        payload_hash = sha256_of_payload(raw)

        # Idempotência: se o mesmo payload já foi ingerido, não faz nada
        if already_ingested_by_hash(spark, payload_hash):
            print("Nada novo para ingerir (payload_hash já existe na Bronze).")
            return

        df = build_bronze_df(spark, raw, ingest_run_id, payload_hash)
        quality_checks(df)
        write_bronze_delta(df)

        print("Ingestão Bronze IPCA concluída.")
        print(f"- Registros ingeridos: {df.count()}")
        print(f"- Delta path: {DELTA_PATH}")
        print(f"- payload_hash: {payload_hash}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
