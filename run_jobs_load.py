# run_jobs_load.py
import os
import argparse
import yaml
import pandas as pd
from dotenv import load_dotenv
from cleaners import coerce_decimal_column

from storage import get_filesystem, get_root, iter_xlsx, read_file_bytes
from excel_reader import read_excel_bytes, iter_excel_chunks
from transform import add_fecha_operacion
from mysql_loader import load_batch_to_mysql
from ddl_utils import ensure_table_from_file, truncate_table, ensure_index_exists



def process_job(fs, base_prefix: str, job: dict, defaults: dict):
    name        = job["name"]
    subpath     = job["path"]
    table       = job["table"]
    ddl_file    = job.get("ddl_file")
    replace     = job.get("replace_mode", "none")   # "truncate" | "none"
    sheet_name  = job.get("sheet_name", defaults["sheet_name"])
    skiprows    = job.get("skiprows",    defaults["skiprows"])
    allowed     = {e.lower() for e in job.get("allowed_exts", defaults["allowed_exts"])}
    batch_rows  = int(job.get("batch_rows", defaults["batch_rows"]))
    drop_cols   = list(job.get("drop_columns", []))
    select_cols = job.get("select_columns")  # opcional: orden exacto de columnas

    # Flags de control adicionales
    extract_fecha = job.get("extract_fecha_operacion", True)
    stream_excel  = bool(job.get("stream_excel", False))
    chunk_rows    = int(job.get("chunk_rows", batch_rows))  # por defecto usa tamaño de lote
    use_ldli      = bool(job.get("use_load_data_infile", False))
    create_idx_end = bool(job.get("create_indexes_at_end", False))

    root = get_root(base_prefix, subpath)
    print(f"\n==> Job: {name}")
    print(f"    Path:  {fs.file_system_name}/{root}")
    print(f"    Tabla: {table}")
    print(f"    DDL:   {ddl_file or '(none)'} | Replace: {replace}")
    print(f"    Lote:  {batch_rows} | Exts: {sorted(allowed)}")
    print(f"    Drop:  {drop_cols or '(ninguna)'}")
    if select_cols:
        print(f"    Orden forzado (select_columns): {select_cols}")
    print(f"    Stream Excel: {stream_excel} | chunk_rows={chunk_rows} | extract_fecha={extract_fecha} | LDLI={use_ldli} | idx_end={create_idx_end}")

    # 1) Asegurar tabla y política de reemplazo
    if ddl_file:
        ensure_table_from_file(ddl_file)
    if replace == "truncate":
        truncate_table(table)

    # Índices al inicio SOLO si no se pidió moverlos al final
    if not create_idx_end:
        for idx in job.get("indexes", []):
            ensure_index_exists(
                table=table,
                index_name=idx["name"],
                columns=idx["columns"],
                unique=idx.get("unique", False)
            )

    # 2) Validación ruta
    if not fs.get_directory_client(root).exists():
        print("    ⚠ La ruta no existe. Se omite.")
        return

    # 3) Stream por lotes (buffer en memoria)
    buffer, buffered_rows = [], 0
    found = processed = errs = inserted_total = 0
    column_list_for_loader = select_cols  # se usa si definiste select_columns

    for path, _etag in iter_xlsx(fs, root, recursive=True):
        basefile = path.rsplit("/", 1)[-1]
        ext = basefile.rsplit(".", 1)[-1].lower() if "." in basefile else ""
        if ext not in allowed:
            continue

        found += 1
        try:
            xls = read_file_bytes(fs, path)

            if stream_excel:
                # === Lectura por chunks (openpyxl read_only) ===
                for df in iter_excel_chunks(
                    xls_bytes=xls,
                    sheet_name=sheet_name,
                    skiprows=skiprows,
                    chunk_rows=chunk_rows
                ):
                    # Añadir fecha si el job lo requiere
                    if extract_fecha:
                        try:
                            df = add_fecha_operacion(df, path)
                        except Exception:
                            # Si el job EXIGE (True por defecto) y falla, propaga
                            if job.get("extract_fecha_operacion", True):
                                raise
                            # Si no lo exige, ignora y continúa

                    # Drops por job (si existen)
                    if drop_cols:
                        present = [c for c in drop_cols if c in df.columns]
                        if present:
                            df = df.drop(columns=present)

                    # Orden/selección exacta (si defines select_columns)
                    if select_cols:
                        missing = [c for c in select_cols if c not in df.columns]
                        if missing:
                            raise ValueError(f"Faltan columnas requeridas {missing} en {basefile}")
                        df = df[select_cols]

                    buffer.append(df)
                    buffered_rows += len(df)
                    processed += 1

                    # Descargar buffer a MySQL cuando alcance el lote
                    if buffered_rows >= batch_rows:
                        df_batch = pd.concat(buffer, ignore_index=True, sort=False)
                        inserted = load_batch_to_mysql(
                            df_batch,
                            table,
                            column_list=column_list_for_loader,
                            use_load_data_infile=use_ldli
                        )
                        inserted_total += inserted
                        print(f"    ✔ Lote cargado: {inserted} filas (archivos acumulados: {processed})")
                        buffer.clear(); buffered_rows = 0

            else:
                # === Camino original (leer el archivo completo a memoria) ===
                df = read_excel_bytes(xls, sheet_name=sheet_name, skiprows=skiprows)

                if extract_fecha:
                    try:
                        df = add_fecha_operacion(df, path)
                    except Exception:
                        if job.get("extract_fecha_operacion", True):
                            raise

                if drop_cols:
                    present = [c for c in drop_cols if c in df.columns]
                    if present:
                        df = df.drop(columns=present)

                if select_cols:
                    missing = [c for c in select_cols if c not in df.columns]
                    if missing:
                        raise ValueError(f"Faltan columnas requeridas {missing} en {basefile}")
                    df = df[select_cols]

                buffer.append(df)
                buffered_rows += len(df)
                processed += 1

                if buffered_rows >= batch_rows:
                    df_batch = pd.concat(buffer, ignore_index=True, sort=False)
                    inserted = load_batch_to_mysql(
                        df_batch,
                        table,
                        column_list=column_list_for_loader,
                        use_load_data_infile=use_ldli
                    )
                    inserted_total += inserted
                    print(f"    ✔ Lote cargado: {inserted} filas (archivos acumulados: {processed})")
                    buffer.clear(); buffered_rows = 0

        except Exception as e:
            errs += 1
            print(f"    ⚠ Error en '{path}': {e}")

    # Último lote pendiente
    if buffer:
        df_batch = pd.concat(buffer, ignore_index=True, sort=False)
        inserted = load_batch_to_mysql(
            df_batch,
            table,
            column_list=column_list_for_loader,
            use_load_data_infile=use_ldli
        )
        inserted_total += inserted
        print(f"    ✔ Lote final: {inserted} filas")

    # Crear índices al final si se solicitó
    if create_idx_end:
        for idx in job.get("indexes", []):
            ensure_index_exists(
                table=table,
                index_name=idx["name"],
                columns=idx["columns"],
                unique=idx.get("unique", False)
            )

    print(f"    Resumen: encontrados={found}  procesados={processed}  errores={errs}  insertadas={inserted_total}")


def main():
    load_dotenv()

    # defaults desde .env
    sheet_raw = (os.getenv("SHEET_NAME", "0") or "0").strip()
    defaults = {
        "sheet_name": int(sheet_raw) if sheet_raw.isdigit() else sheet_raw,
        "skiprows": int(os.getenv("SKIPROWS", "0")),
        "allowed_exts": [e.strip().lower() for e in os.getenv("ALLOWED_EXTENSIONS","xlsx").split(",") if e.strip()],
        "batch_rows": int(os.getenv("BATCH_ROWS", "100000")),
    }

    # lee jobs.yaml
    with open("jobs.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    base_prefix = (cfg.get("base_prefix") or "").strip().strip("/")
    jobs = cfg.get("jobs", [])
    if not jobs:
        print("No hay jobs en jobs.yaml"); return

    # filtros CLI
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", help="Ejecuta solo estos jobs (por name)")
    parser.add_argument("--skip", nargs="*", help="Omite estos jobs (por name)")
    args = parser.parse_args()

    selected = []
    for j in jobs:
        if args.only and j["name"] not in args.only: continue
        if args.skip and j["name"] in args.skip:     continue
        selected.append(j)
    if not selected:
        print("No hay jobs seleccionados."); return

    fs = get_filesystem()

    for job in selected:
        process_job(fs, base_prefix, job, defaults)

if __name__ == "__main__":
    main()
