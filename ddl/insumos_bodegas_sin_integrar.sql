CREATE TABLE IF NOT EXISTS insumos_bodegas_sin_integrar (
    Cod VARCHAR(20),
    Generico VARCHAR(50),
    Especifico VARCHAR(50),
<<<<<<< HEAD
=======
    Color VARCHAR(50),
>>>>>>> f116724 (init: estructura Safetti ETL)
    `Talla/Med` VARCHAR(10),
    Cant INT,
    Precio DECIMAL(10,2),
    Nombre VARCHAR(100),
    BodInt VARCHAR(10),
    Activo INT,
    Talla VARCHAR(10),
    TallaMedida VARCHAR(10),
    Esp VARCHAR(50),
    `Fecha Inv` DATE,
    FDev DATE,
    FAjuste DATE,
    FEntProv DATE,
    FEntProc DATE,
    `FEntPlan o EntCorte` DATE,
    FSalProv DATE,
    `FSalPlanta o SalCorte` DATE,
    FSalProc DATE,
    FFact DATE,
    fecha_operacion DATE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;