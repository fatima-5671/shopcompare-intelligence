#✅ 1. Docker Postgres run command
docker run --name ecommerce-db \
-e POSTGRES_USER=app_user \
-e POSTGRES_PASSWORD=app_password \
-e POSTGRES_DB=ecommerce_db \
-p 5432:5432 \
-d postgres:16
#✅ 2. Database connect command
docker exec -it ecommerce-db psql -U app_user -d ecommerce_db
#✅ 3. Table creation (IMPORTANT)
CREATE TABLE ecommerce_products (
    id SERIAL PRIMARY KEY,
    platform VARCHAR(50),
    keyword VARCHAR(100),
    title TEXT,
    price NUMERIC(12,2),
    currency VARCHAR(10),
    original_price NUMERIC(12,2),
    discount_pct NUMERIC(10,2),
    rating NUMERIC(3,2),
    review_count INTEGER,
    availability VARCHAR(50),
    seller VARCHAR(255),
    product_url TEXT,
    scraped_at TIMESTAMP,
    quality_flag VARCHAR(20),
    usd_price NUMERIC(12,2),
    price_savings NUMERIC(12,2),
    value_score NUMERIC(12,4),
    is_high_discount VARCHAR(10),
    price_tier VARCHAR(50)
);