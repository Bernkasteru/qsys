CREATE DATABASE IF NOT EXISTS conorder_db;
USE conorder_db;

CREATE TABLE IF NOT EXISTS orders (
    `id` BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    `client_id` CHAR(12) NOT NULL,  -- 固定 12 位
    `exchange_type` CHAR(1) NOT NULL COMMENT '1:SH, 2:SZ',
    `stock_code` CHAR(6) NOT NULL,
    UNIQUE INDEX `idx_unique_order` (`client_id`, `exchange_type`, `stock_code`),
    INDEX `idx_stock` (`stock_code`)  -- 辅助索引, 按股票代码快速检索
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;