CREATE DATABASE IF NOT EXISTS conorder_db;
USE conorder_db;

CREATE TABLE IF NOT EXISTS orders (
    `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `client_id` CHAR(12) CHARACTER SET ascii COLLATE ascii_bin NOT NULL COMMENT '客户号, 固定12位',  -- 固定 12 位
    `exchange_type` CHAR(1) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL COMMENT '交易市场, 1:SH, 2:SZ',
    `stock_code` CHAR(6) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL COMMENT '股票代码, 固定6位',
    UNIQUE INDEX `idx_unique_order` (`client_id`, `exchange_type`, `stock_code`),
    INDEX `idx_stock` (`stock_code`)  -- 辅助索引, 按股票代码快速检索
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;