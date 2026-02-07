# Ingestão bruta : leitura e conversão CSV 

from __future__ import annotations
import datetime
import pandas as pd
from pathlib import Path
import hashlib

# =========================================================
# Utilitário:
# =========================================================

def _file_sha256(path: Path, chunk_size: int = 8192) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()

def _get_ingested_hashes(data_source_root: Path) -> set[str]:
    manifest = data_source_root / "csv" / "_manifest.csv"
    if not manifest.exists():
        return set()

    df = pd.read_csv(manifest)
    if "file_hash" not in df.columns:
        return set()

    return set(df["file_hash"].astype(str))

# Conversão XLS -> CSV (canônico do data_source)

def convert_xls_to_csv_data_source(xls_path: Path, data_source_root: Path) -> Path:
    df = pd.read_excel(xls_path, sheet_name=0)

    # usa só as duas primeiras colunas
    df = df.iloc[:, :2]
    df.columns = ["data_raw", "valor_raw"]

    # remove linhas lixo
    df = df[
        ~(
            df["data_raw"].astype(str).str.lower().eq("data")
            & df["valor_raw"].astype(str).str.lower().eq("valor")
        )
    ]

    # normaliza valor pt-BR e converte tolerante
    valor_txt = (
        df["valor_raw"].astype(str)
        .str.replace("\u00a0", " ", regex=False)  # NBSP
        .str.strip()
        .str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False)
    )

    df["valor"] = pd.to_numeric(valor_txt, errors="coerce")
    

    # data -> mes/ano
    df["mes_ano"] = pd.to_datetime(
        df["data_raw"].astype(str).str.strip(),
        format="%m/%Y",
        errors="coerce"
    ).dt.strftime("%m/%Y")

    df = df[["mes_ano", "valor"]].dropna()

    csv_path = data_source_root / "csv" / f"{xls_path.stem}.csv"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False,encoding="utf-8")

    return csv_path

# Ingestão controlada (dedupe por hash)

def ingest_data_source_xls(xls_dir: Path,data_source_root: Path) -> list[Path]:

    xls_files = sorted(
        list(xls_dir.glob("*.xls")) + list(xls_dir.glob("*.xlsx"))
    )

    ingested_hashes = _get_ingested_hashes(data_source_root)

    out_csvs: list[Path] = []
    new_manifest_rows = []

    for xls_path in xls_files:
        file_hash = _file_sha256(xls_path)

        # dedupe por conteúdo
        if file_hash in ingested_hashes:
            continue

        csv_path = convert_xls_to_csv_data_source(xls_path, data_source_root)
        out_csvs.append(csv_path)

        new_manifest_rows.append({
            "file_hash": file_hash,
            "source_file": xls_path.name,     # SEMPRE preservado
            "csv_file": csv_path.name,
            "converted_at_utc": pd.Timestamp.utcnow()
                                  .strftime("%Y-%m-%dT%H:%M:%SZ"),
        })

    # atualiza manifest
    if new_manifest_rows:
        manifest_path = data_source_root / "csv" / "_manifest.csv"
        df_new = pd.DataFrame(new_manifest_rows)

        if manifest_path.exists():
            df_old = pd.read_csv(manifest_path)
            df_all = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_all = df_new

        df_all.to_csv(manifest_path, index=False, encoding="utf-8")

    return out_csvs

# =========================================================
# Execução direta (opcional)
# =========================================================

if __name__ == "__main__":
    xls_dir = Path("data_source/xls")
    data_source_root = Path("data_source")

    csvs = ingest_data_source_xls(xls_dir, data_source_root)
    print(f"{len(csvs)} CSV(s) gerado(s):")
    for c in csvs:
        print(" -", c)

# Obs:
# O script usa caminhos relativos:

# xls_dir = Path("data_source/xls")
# data_source_root = Path("data_source")
# Então ele precisa ser executado da raiz do projeto.        
