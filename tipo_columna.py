import io
import re
import pandas as pd
from collections import Counter
from storage import get_filesystem, get_root, iter_xlsx, read_file_bytes


def _norm(s: str) -> str:
    """Normaliza: minúsculas + colapsa espacios."""
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def _match_column(target: str, cols) -> str | None:
    """Retorna el nombre real de la columna que coincide con target (insensible a mayúsculas/espacios)."""
    tnorm = _norm(target)
    norm_map = { _norm(c): c for c in cols }
    return norm_map.get(tnorm)


def _sheet_list(xls: pd.ExcelFile, sheet_selector="ALL"):
    """Devuelve las hojas a procesar."""
    if isinstance(sheet_selector, str) and sheet_selector.upper() == "ALL":
        return xls.sheet_names
    try:
        idx = int(sheet_selector)
        return [xls.sheet_names[idx]]
    except Exception:
        return [sheet_selector]


def _type_counter(series: pd.Series, max_items: int = 2000) -> Counter:
    """Cuenta tipos Python en una muestra de la serie."""
    vals = series.head(max_items).tolist()
    return Counter(type(v).__name__ for v in vals)


def inspect_column_dtype(
    column_name: str,
    base_prefix: str,
    job_subpath: str,
    sheet_selector: str = "ALL",
    skiprows_try: list[int] = list(range(0, 9)),
    head_rows: int = 1000,
    show_sample: int = 8,
):
    """
    Analiza el tipo de dato de una columna en todos los archivos XLSX de una ruta ADLS.

    Parámetros:
        column_name: nombre (o parte normalizada) de la columna a buscar.
        base_prefix: prefijo base, ej. 'busint/safetti/input'
        job_subpath: subcarpeta del job, ej. 'Compañia 1/...'
        sheet_selector: 'ALL', índice o nombre de hoja.
        skiprows_try: lista de posibles filas a saltar antes del encabezado.
        head_rows: número de filas a leer por archivo para inferir dtype.
        show_sample: número de valores a mostrar de muestra.

    Devuelve:
        Lista de dicts con detalles por archivo/hoja/skiprows donde se encontró la columna.
    """
    fs = get_filesystem()
    root = get_root(base_prefix, job_subpath)
    print(f"[INFO] Root ADLS: {root}")
    print(f"[INFO] Columna objetivo: {column_name!r}")
    print(f"[INFO] Hojas: {sheet_selector} | skiprows a probar: {skiprows_try}")

    candidates = list(iter_xlsx(fs, root, recursive=True))
    if not candidates:
        print("[WARN] No se encontraron .xlsx bajo esa ruta.")
        return []

    results = []
    for path, _ in candidates:
        data = read_file_bytes(fs, path)
        bio = io.BytesIO(data)
        try:
            xls = pd.ExcelFile(bio, engine="openpyxl")
            sheets = _sheet_list(xls, sheet_selector)
        except Exception as e:
            print(f"[ERROR] No pude abrir {path}: {e}")
            continue

        for sh in sheets:
            for sk in skiprows_try:
                try:
                    df = pd.read_excel(
                        io.BytesIO(data),
                        sheet_name=sh,
                        nrows=head_rows,
                        engine="openpyxl",
                        skiprows=sk,
                        header=0,
                    )
                except Exception:
                    continue

                real_col = _match_column(column_name, df.columns)
                if not real_col:
                    continue

                col = df[real_col]
                dtype = str(col.dtype)
                non_null = int(col.notna().sum())
                n_null = int(col.isna().sum())
                total = int(len(col))

                py_types = dict(_type_counter(col, max_items=head_rows))
                sample_vals = [str(v).replace("\n", " ") for v in col.head(show_sample).tolist()]

                result = {
                    "file": path,
                    "sheet": sh,
                    "skiprows": sk,
                    "column": real_col,
                    "dtype": dtype,
                    "types_python": py_types,
                    "non_null": non_null,
                    "nulls": n_null,
                    "total": total,
                    "sample": sample_vals,
                }
                results.append(result)
                # ya la encontramos en este skiprows, pasamos a la siguiente hoja
                break

    if not results:
        print("\n[RESULT] No se encontró la columna en ningún archivo.")
    else:
        print(f"\n[RESULT] Encontrada en {len(results)} archivo(s):")
        for r in results:
            print(f"- {r['file']} | hoja='{r['sheet']}' | skiprows={r['skiprows']} | dtype={r['dtype']}")
            print(f"  tipos_python={r['types_python']} | sample={r['sample']}")

    return results
