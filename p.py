# find_bad_headers_multi.py
import os, io, re
import pandas as pd
from collections import defaultdict
from storage import get_filesystem, get_root, iter_xlsx, read_file_bytes

# --- TARGETS ---
DEFAULT_TARGETS = [
    "CODCONCILI",
    # Los 'Unnamed' los detectamos con regex, no es necesario listarlos todos
]
# Permite override por variable de entorno (separado por comas)
ENV_TARGETS = os.getenv("TARGET_HEADERS", "")
TARGETS = [t.strip() for t in ENV_TARGETS.split(",") if t.strip()] or DEFAULT_TARGETS

# Detecta columnas tipo "Unnamed: N" (N 1..18 por defecto)
UNNAMED_MAX = int(os.getenv("UNNAMED_MAX", "18"))
UNNAMED_RE = re.compile(rf"^unnamed:\s*([0-9]+)\s*$", re.I)

# Normalizador: minimiza diferencias por mayúsculas/espacios
def norm(s: str) -> str:
    s = str(s)
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

TARGETS_NORM = set(norm(t) for t in TARGETS)

# --- Params de ruta/lectura ---
BASE_PREFIX = os.getenv("BASE_PREFIX", "busint/safetti/input")
JOB_SUBPATH = os.getenv("JOB_SUBPATH", "Compañia 1/3. Cuentas por Cobrar/03. Notas contables gerencia")

# Hojas: ALL para recorrer todas; o pasa un nombre/índice concreto
SHEET_SELECTOR = os.getenv("SHEET_NAME", "ALL")  # "ALL" | "0" | "Sheet1" ...
SKIPROWS_TRY = [int(x) for x in os.getenv("SKIPROWS_TRY", "0,1,2,3,4,5,6,7,8").split(",")]

def sheet_list(xls: pd.ExcelFile):
    if isinstance(SHEET_SELECTOR, str) and SHEET_SELECTOR.upper() == "ALL":
        return xls.sheet_names
    # si es número como string, intenta convertir a índice
    try:
        idx = int(SHEET_SELECTOR)
        return [xls.sheet_names[idx]]
    except Exception:
        # asume nombre de hoja
        return [SHEET_SELECTOR]

def find_matches_in_cols(cols):
    """
    Devuelve conjunto de nombres problemáticos encontrados en 'cols'.
    Se hace matching normalizado para TARGETS y regex para 'Unnamed:N' (1..UNNAMED_MAX)
    """
    found = set()
    cols_norm = [norm(c) for c in cols]

    # Targets explícitos
    for tnorm in TARGETS_NORM:
        if tnorm in cols_norm:
            # recuperar la versión original tal cual aparece
            original = cols[cols_norm.index(tnorm)]
            found.add(str(original))

    # Unnamed:N
    for c in cols:
        m = UNNAMED_RE.match(str(c))
        if m:
            try:
                n = int(m.group(1))
                if 1 <= n <= UNNAMED_MAX:
                    found.add(str(c))
            except ValueError:
                pass

    return found

def main():
    fs = get_filesystem()
    root = get_root(BASE_PREFIX, JOB_SUBPATH)
    print(f"[INFO] Root ADLS: {root}")
    print(f"[INFO] Buscando cualquiera de: {TARGETS}")
    print(f"[INFO] Detectando también Unnamed:1..{UNNAMED_MAX}")
    print(f"[INFO] Hojas: {SHEET_SELECTOR} | skiprows a probar: {SKIPROWS_TRY}")

    candidates = list(iter_xlsx(fs, root, recursive=True))
    if not candidates:
        print("[WARN] No se encontraron .xlsx bajo esa ruta.")
        return

    results = defaultdict(list)  # path -> list of dicts {sheet, skiprows, columns_found}
    for path, _ in candidates:
        data = read_file_bytes(fs, path)
        bio  = io.BytesIO(data)

        try:
            xls = pd.ExcelFile(bio, engine="openpyxl")
            sheets = sheet_list(xls)
        except Exception as e:
            print(f"[ERROR] No pude abrir {path}: {e}")
            continue

        # vuelve a crear BytesIO para lecturas sucesivas
        for sh in sheets:
            for sk in SKIPROWS_TRY:
                try:
                    df = pd.read_excel(
                        io.BytesIO(data),
                        sheet_name=sh,
                        nrows=5,
                        engine="openpyxl",
                        skiprows=sk,
                        header=0,
                    )
                except Exception:
                    continue
                found = find_matches_in_cols(df.columns)
                if found:
                    results[path].append({
                        "sheet": sh,
                        "skiprows": sk,
                        "columns_found": sorted(found),
                    })
                    # si con este skiprows ya encontró, puedes romper para esta hoja
                    # break

    if not results:
        print("\n[RESULT] No se encontraron esas columnas en ningún archivo con los parámetros actuales.")
        return

    print("\n[RESULT] Archivos con columnas problemáticas:")
    for p, hits in results.items():
        print(f"\n- {p}")
        for h in hits:
            print(f"    hoja='{h['sheet']}' | skiprows={h['skiprows']} | cols={h['columns_found']}")

if __name__ == "__main__":
    main()
