-- ============================================================
-- ClickHouse 完整建表脚本（SQL 清洗管道专用）
-- 数据库: dataproject
-- ============================================================

-- 0. 日历参考表（节日/促销/工作日标记）
CREATE TABLE IF NOT EXISTS dataproject.calendar (
    date        Date,
    day_type    String,      -- holiday/promotion/weekday/weekend
    year        UInt16,
    month       UInt8,
    day         UInt8,
    weekday     UInt8        -- 0=Mon, 6=Sun
) ENGINE = MergeTree()
ORDER BY date;

-- 1. 分类表（维度表，从本地 CSV 写入）
CREATE TABLE IF NOT EXISTS dataproject.categories (
    category      String,
    category_id   UInt64,
    hierarchy     UInt8,
    weight        Nullable(Float64),
    price         Nullable(Float64),
    parent        Nullable(UInt64)
) ENGINE = MergeTree()
ORDER BY category_id;

-- 2. 产品表（维度表）
CREATE TABLE IF NOT EXISTS dataproject.products (
    product_id      UInt64,
    name            String,
    category_id     UInt64,
    price           Float64,
    weight          Float64,
    status          UInt8,
    effective_date  Date,
    expiration_date Nullable(Date)
) ENGINE = MergeTree()
ORDER BY (product_id, effective_date);

-- 3. 原始销量暂存表（从 OSS S3 读入，清洗后清空）
CREATE TABLE IF NOT EXISTS dataproject.sales_staging (
    product_id    UInt64,
    sale_date     Date,
    sales_volume  Nullable(Int32),
    price         Float64,
    revenue       Nullable(Float64),
    is_missing    UInt8
) ENGINE = MergeTree()
ORDER BY (sale_date, product_id);

-- 4. 清洗后销量表（SQL 清洗结果）
CREATE TABLE IF NOT EXISTS dataproject.sales_clean (
    product_id    UInt64,
    sale_date     Date,
    sales_volume  Nullable(Int32),
    price         Float64,
    revenue       Nullable(Float64),
    is_missing    UInt8,
    event_type    String,   -- normal/weekend/holiday/promotion/anomaly/missing
    day_type      String    -- 日历类型
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY (sale_date, product_id);

-- 5. 每日聚合统计表
CREATE TABLE IF NOT EXISTS dataproject.daily_stats (
    sale_date       Date,
    event_type      String,
    record_count    UInt64,
    active_products UInt64,
    total_sales_volume Nullable(Int64),
    total_revenue   Nullable(Float64),
    missing_count   UInt64,
    anomaly_count   UInt64,
    promotion_count UInt64,
    holiday_count   UInt64
) ENGINE = MergeTree()
ORDER BY (sale_date, event_type);

-- 6. CPI 结果表
CREATE TABLE IF NOT EXISTS dataproject.cpi_trend (
    date          Date,
    laspeyres     Nullable(Float64),
    paasche       Nullable(Float64),
    fisher        Float64,
    product_count UInt32,
    category_id   UInt64 DEFAULT 0,
    granularity   String DEFAULT 'day'
) ENGINE = MergeTree()
ORDER BY (date, category_id);

-- 7. CPI 类目明细表
CREATE TABLE IF NOT EXISTS dataproject.cpi_category (
    date          Date,
    category_id   UInt64,
    category      String,
    hierarchy     UInt8,
    laspeyres     Nullable(Float64),
    paasche       Nullable(Float64),
    fisher        Float64,
    weight        Float64 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY (date, category_id);

-- 8. 产品-分类宽表
CREATE TABLE IF NOT EXISTS dataproject.product_category_map (
    product_id     UInt64,
    category_id    UInt64,
    category       String,
    l1_category    String,
    l2_category    String,
    l3_category    String
) ENGINE = MergeTree()
ORDER BY product_id;

-- 9. 异常事件记录表
CREATE TABLE IF NOT EXISTS dataproject.anomaly_events (
    sale_date       Date,
    product_id      UInt64,
    sales_volume    Nullable(Int32),
    expected_volume Nullable(Float64),
    ratio           Nullable(Float64),
    event_type      String
) ENGINE = MergeTree()
ORDER BY (sale_date, product_id);
