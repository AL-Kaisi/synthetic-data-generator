/*
 * Customer Orders Table Schema
 * Example of PostgreSQL/MySQL compatible DDL
 */

CREATE TABLE IF NOT EXISTS customer_orders (
    order_id BIGINT NOT NULL,
    customer_id INT NOT NULL,
    customer_name VARCHAR(100),
    customer_email VARCHAR(255),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    order_status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') NOT NULL,
    order_total DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50),
    shipping_address TEXT,
    shipping_postcode VARCHAR(10),
    items_count SMALLINT,
    is_gift BOOLEAN DEFAULT FALSE,
    tracking_number VARCHAR(50),
    notes TEXT,
    created_at DATETIME,
    updated_at DATETIME
);
