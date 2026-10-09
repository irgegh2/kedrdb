\set ON_ERROR_STOP on
\if :{?product_count}
\else
  \set product_count 100000
\endif
\if :{?user_count}
\else
  \set user_count 10000
\endif
\if :{?payload_bytes}
\else
  \set payload_bytes 128
\endif

-- Execute only in a newly created benchmark database. No existing table is dropped.
CREATE TABLE users (
    id BIGINT PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    active BOOLEAN NOT NULL
);
CREATE TABLE products (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    category_id BIGINT NOT NULL,
    price_cents BIGINT NOT NULL,
    stock INT NOT NULL,
    active BOOLEAN NOT NULL,
    description TEXT
);
CREATE INDEX products_category ON products(category_id);
CREATE TABLE orders (
    id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id),
    status TEXT NOT NULL,
    total_cents BIGINT NOT NULL
);
CREATE INDEX orders_user ON orders(user_id);
CREATE TABLE order_items (
    id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL REFERENCES orders(id),
    product_id BIGINT NOT NULL REFERENCES products(id),
    quantity INT NOT NULL,
    price_cents BIGINT NOT NULL
);
CREATE INDEX order_items_order ON order_items(order_id);
CREATE TABLE inventory_events (
    id BIGINT PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES products(id),
    actor_id BIGINT NOT NULL REFERENCES users(id),
    new_stock INT NOT NULL
);

-- generate_series, concatenation and arithmetic are fixture-generation tools.
-- These expressions are not a promise of initial MiniDB SQL support.
INSERT INTO users
SELECT n, 'user' || n || '@example.test', 'Пользователь ' || n, TRUE
FROM generate_series(1, :user_count) AS n;
INSERT INTO products
SELECT n, 'Товар ' || n, 1 + n % 100, 100 + n % 100000,
       1000, TRUE, repeat('x', :payload_bytes)
FROM generate_series(1, :product_count) AS n;
INSERT INTO orders
SELECT n, n, 'created', 2 * (100 + n % 100000)
FROM generate_series(1, LEAST(:user_count, :product_count)) AS n;
INSERT INTO order_items
SELECT n, n, n, 2, 100 + n % 100000
FROM generate_series(1, LEAST(:user_count, :product_count)) AS n;
ANALYZE;
