CREATE TABLE IF NOT EXISTS inventario_bodega (
    Ubicacion VARCHAR(255),
    Ref VARCHAR(30),
<<<<<<< HEAD
    RefExt VARCHAR(30),
=======
    RefExt VARCHAR(50),
>>>>>>> f116724 (init: estructura Safetti ETL)
    Pinta VARCHAR(5),
    Colores VARCHAR(20),
    Coleccion VARCHAR(20),
    Linea VARCHAR(20),
    Cuento VARCHAR(20),
    Descripcion VARCHAR(50),
    CANT INT,
    Talla VARCHAR(10),
<<<<<<< HEAD
    `Descripcion Larga` VARCHAR(100),
=======
    CostoEst INT,
    `CostoEst Total` INT,
    `Descripcion Larga` VARCHAR(100),
    TipoConf VARCHAR(50),
>>>>>>> f116724 (init: estructura Safetti ETL)
    SubCategoria VARCHAR(50),
    UltFechaEntradaProd DATE,
    UltFechaEntradaProv DATE,
    UltFechaAjuste DATE,
<<<<<<< HEAD
=======
    InvMax INT,
    InvMin INT,
    MinMax INT,
>>>>>>> f116724 (init: estructura Safetti ETL)
    fecha_operacion DATE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
