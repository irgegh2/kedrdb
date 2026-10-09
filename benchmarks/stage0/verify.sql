\set ON_ERROR_STOP on
-- This only checks invariants of the fixture; it does not prove database durability.
SELECT COUNT(*) AS product_count FROM products;
SELECT COUNT(*) AS user_count FROM users;
SELECT COUNT(*) AS inventory_event_count FROM inventory_events;
SELECT COUNT(*) AS invalid_products FROM products WHERE stock < 0 OR stock > 1000;
SELECT COUNT(*) AS orphaned_events
FROM inventory_events AS e
LEFT JOIN products AS p ON p.id = e.product_id
LEFT JOIN users AS u ON u.id = e.actor_id
WHERE p.id IS NULL OR u.id IS NULL;

