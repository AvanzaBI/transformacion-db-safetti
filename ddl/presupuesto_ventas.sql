CREATE TABLE IF NOT EXISTS presupuesto_ventas (
    Fecha DATE,
    `Vendedor-punto` VARCHAR(150),
    `VENDEDOR SISTEMA` VARCHAR(150),
    `PPTO COP` DECIMAL(15,2),
    `PPTO USD` DECIMAL(15,2),
    `Nacional / Exportacion` VARCHAR(30),
    Tipo VARCHAR(20)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;