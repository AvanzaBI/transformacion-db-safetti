CREATE TABLE IF NOT EXISTS historico_lotes_gerencia (
    NumLote VARCHAR(50),
    Nombre VARCHAR(100),
    Total INT,
    TotalReal INT,
    Ref VARCHAR(30),
<<<<<<< HEAD
    `Costo Unt FT` DECIMAL(10,2),
    CostoTotalFT DECIMAL(10,2),
    CostoReal DECIMAL(10,2),
=======
    `Costo Unt FT` DECIMAL(20,8),
    CostoTotalFT DECIMAL(20,8),
    CostoReal DECIMAL(25,8),
>>>>>>> f116724 (init: estructura Safetti ETL)
    fecha DATE,
    esp VARCHAR(50),
    Coleccion VARCHAR(20),
    Linea VARCHAR(20),
    Descripcion VARCHAR(100),
    Cuento VARCHAR(20),
    PedidoCliente VARCHAR(30),
    cliente VARCHAR(100),
    Componente VARCHAR(50),
<<<<<<< HEAD
    RefExt VARCHAR(30),
=======
    RefExt VARCHAR(100),
>>>>>>> f116724 (init: estructura Safetti ETL)
    `Precio PM` DECIMAL(10,2),
    `Precio PP` DECIMAL(10,2),
    `Precio 2` DECIMAL(10,2),
    `Precio 3` DECIMAL(10,2),
    `Precio 4` DECIMAL(10,2),
<<<<<<< HEAD
    Observacion VARCHAR(100),
=======
    Observacion VARCHAR(255),
>>>>>>> f116724 (init: estructura Safetti ETL)
    fecha_operacion DATE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;