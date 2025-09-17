# test_ddl.py
from dotenv import load_dotenv
load_dotenv()

from ddl_utils import ensure_table_from_file, truncate_table

ensure_table_from_file("ddl/inventario_bodega.sql")
truncate_table("inventario_bodega")
print("DDL y TRUNCATE OK")
