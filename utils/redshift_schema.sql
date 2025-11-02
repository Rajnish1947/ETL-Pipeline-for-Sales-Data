CREATE TABLE IF NOT EXISTS sales_data (
    order_id INT,
    customer VARCHAR(50),
    product VARCHAR(50),
    price FLOAT,
    quantity INT,
    date DATE,
    total_value FLOAT
);
