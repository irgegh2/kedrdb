-- SPDX-License-Identifier: Apache-2.0
-- Each non-comment line is one statement. Use `kedr sql` or an editor; the
-- line-oriented sql-stream currently expects SQL statements, not comment lines.
CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE);
CREATE TABLE products (id BIGINT PRIMARY KEY, title TEXT NOT NULL, price_kopecks BIGINT NOT NULL, stock INT NOT NULL);
CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES users(id), product_id BIGINT NOT NULL REFERENCES products(id), quantity INT NOT NULL);
CREATE INDEX products_title ON products(title);
INSERT INTO users (id,name,email) VALUES (1,'Анна','anna@example.test');
INSERT INTO products (id,title,price_kopecks,stock) VALUES (1,'Тетрадь',12500,10);
BEGIN;
UPDATE products SET stock=stock-2 WHERE id=1;
INSERT INTO orders (id,user_id,product_id,quantity) VALUES (1,1,1,2);
COMMIT;
SELECT p.title,o.quantity FROM orders AS o INNER JOIN products AS p ON o.product_id=p.id ORDER BY o.id;
EXPLAIN SELECT id FROM products WHERE title='Тетрадь';
