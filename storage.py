# storage.py
import os
from typing import Iterator, Tuple
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient

# Carga variables de entorno una vez
load_dotenv()

def get_filesystem() -> FileSystemClient:
    """
    Crea y devuelve el cliente del File System (container) de ADLS Gen2.
    NO depende de ROOT_PATH; las rutas se pasan por job (jobs.yaml).
    Requiere en .env:
      - STORAGE_ACCOUNT_NAME
      - STORAGE_ACCOUNT_KEY
      - FILE_SYSTEM
    """
    account = os.getenv("STORAGE_ACCOUNT_NAME")
    key     = os.getenv("STORAGE_ACCOUNT_KEY")
    fs_name = os.getenv("FILE_SYSTEM")

    if not all([account, key, fs_name]):
        raise RuntimeError("Faltan vars .env: STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY, FILE_SYSTEM")

    service = DataLakeServiceClient(
        account_url=f"https://{account}.dfs.core.windows.net",
        credential=key
    )
    return service.get_file_system_client(fs_name)

def get_root(base_prefix: str, subpath: str) -> str:
    """
    Une el prefijo común (del jobs.yaml) con el subpath del job y normaliza barras.
    Ej: base_prefix='busint/safetti/input', subpath='Compañía 1/10 Costos/01. Inventario por bodega'
        -> 'busint/safetti/input/Compañía 1/10 Costos/01. Inventario por bodega'
    """
    base = (base_prefix or "").strip().strip("/")
    sub  = (subpath or "").strip().strip("/")
    return f"{base}/{sub}".strip("/") if base or sub else ""

def iter_xlsx(fs: FileSystemClient, root: str, recursive: bool = True) -> Iterator[Tuple[str, str | None]]:
    """
    Itera rutas de archivos .xlsx bajo 'root'.
    Omite temporales (~$...). Retorna (ruta_completa, etag|None).
    """
    for p in fs.get_paths(path=root, recursive=recursive):
        if p.is_directory:
            continue
        base = p.name.rsplit("/", 1)[-1]
        if base.startswith("~$"):
            continue
        if p.name.lower().endswith(".xlsx"):
            yield p.name, getattr(p, "etag", None)

def read_file_bytes(fs: FileSystemClient, path: str) -> bytes:
    """
    Descarga un archivo del Data Lake y retorna su contenido en bytes.
    """
    return fs.get_file_client(path).download_file().readall()
