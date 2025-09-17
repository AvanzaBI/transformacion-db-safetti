CREATE TABLE IF NOT EXISTS inventario_bodega (
    Ubicacion VARCHAR(255),
    Ref VARCHAR(30),
    RefExt VARCHAR(30),
    Pinta VARCHAR(5),
    Colores VARCHAR(20),
    Coleccion VARCHAR(20),
    Linea VARCHAR(20),
    Cuento VARCHAR(20),
    Descripcion VARCHAR(50),
    CANT INT,
    Talla VARCHAR(10),
    `Descripcion Larga` VARCHAR(100),
    SubCategoria VARCHAR(50),
    UltFechaEntradaProd DATE,
    UltFechaEntradaProv DATE,
    UltFechaAjuste DATE,
    fecha_operacion DATE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
