-- =============================================================
-- TPC-H Source Database Schema for PostgreSQL
-- =============================================================
-- Creates 'sourcedb' database with 8 TPC-H tables.
-- Used as the OLTP source for the Medallion pipeline.
-- =============================================================

-- Run this AFTER creating the database:
--   CREATE DATABASE sourcedb;
-- Then connect to sourcedb and run this script.

-- ---------------------------------------------------------
-- DIMENSION TABLES (Small, Static)
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS region (
    r_regionkey  INTEGER PRIMARY KEY,
    r_name       VARCHAR(25) NOT NULL,
    r_comment    VARCHAR(152),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS nation (
    n_nationkey  INTEGER PRIMARY KEY,
    n_name       VARCHAR(25) NOT NULL,
    n_regionkey  INTEGER NOT NULL REFERENCES region(r_regionkey),
    n_comment    VARCHAR(152),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS supplier (
    s_suppkey    INTEGER PRIMARY KEY,
    s_name       VARCHAR(25) NOT NULL,
    s_address    VARCHAR(40) NOT NULL,
    s_nationkey  INTEGER NOT NULL REFERENCES nation(n_nationkey),
    s_phone      VARCHAR(15) NOT NULL,
    s_acctbal    DECIMAL(15,2) NOT NULL,
    s_comment    VARCHAR(101),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS part (
    p_partkey      INTEGER PRIMARY KEY,
    p_name         VARCHAR(55) NOT NULL,
    p_mfgr         VARCHAR(25) NOT NULL,
    p_brand        VARCHAR(10) NOT NULL,
    p_type         VARCHAR(25) NOT NULL,
    p_size         INTEGER NOT NULL,
    p_container    VARCHAR(10) NOT NULL,
    p_retailprice  DECIMAL(15,2) NOT NULL,
    p_comment      VARCHAR(23),
    created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customer (
    c_custkey     INTEGER PRIMARY KEY,
    c_name        VARCHAR(25) NOT NULL,
    c_address     VARCHAR(40) NOT NULL,
    c_nationkey   INTEGER NOT NULL REFERENCES nation(n_nationkey),
    c_phone       VARCHAR(15) NOT NULL,
    c_acctbal     DECIMAL(15,2) NOT NULL,
    c_mktsegment  VARCHAR(10) NOT NULL,
    c_comment     VARCHAR(117),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ---------------------------------------------------------
-- BRIDGE TABLE
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS partsupp (
    ps_partkey    INTEGER NOT NULL REFERENCES part(p_partkey),
    ps_suppkey    INTEGER NOT NULL REFERENCES supplier(s_suppkey),
    ps_availqty   INTEGER NOT NULL,
    ps_supplycost DECIMAL(15,2) NOT NULL,
    ps_comment    VARCHAR(199),
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ps_partkey, ps_suppkey)
);

-- ---------------------------------------------------------
-- FACT TABLES (Large, Transactional)
-- ---------------------------------------------------------

CREATE TABLE IF NOT EXISTS orders (
    o_orderkey      INTEGER PRIMARY KEY,
    o_custkey       INTEGER NOT NULL REFERENCES customer(c_custkey),
    o_orderstatus   CHAR(1) NOT NULL,
    o_totalprice    DECIMAL(15,2) NOT NULL,
    o_orderdate     DATE NOT NULL,
    o_orderpriority VARCHAR(15) NOT NULL,
    o_clerk         VARCHAR(15) NOT NULL,
    o_shippriority  INTEGER NOT NULL,
    o_comment       VARCHAR(79),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS lineitem (
    l_orderkey      INTEGER NOT NULL REFERENCES orders(o_orderkey),
    l_partkey       INTEGER NOT NULL REFERENCES part(p_partkey),
    l_suppkey       INTEGER NOT NULL REFERENCES supplier(s_suppkey),
    l_linenumber    INTEGER NOT NULL,
    l_quantity      DECIMAL(15,2) NOT NULL,
    l_extendedprice DECIMAL(15,2) NOT NULL,
    l_discount      DECIMAL(15,2) NOT NULL,
    l_tax           DECIMAL(15,2) NOT NULL,
    l_returnflag    CHAR(1) NOT NULL,
    l_linestatus    CHAR(1) NOT NULL,
    l_shipdate      DATE NOT NULL,
    l_commitdate    DATE NOT NULL,
    l_receiptdate   DATE NOT NULL,
    l_shipinstruct  VARCHAR(25) NOT NULL,
    l_shipmode      VARCHAR(10) NOT NULL,
    l_comment       VARCHAR(44),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (l_orderkey, l_linenumber)
);

-- ---------------------------------------------------------
-- INDEXES for query performance
-- ---------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_orders_custkey ON orders(o_custkey);
CREATE INDEX IF NOT EXISTS idx_orders_orderdate ON orders(o_orderdate);
CREATE INDEX IF NOT EXISTS idx_lineitem_partkey ON lineitem(l_partkey);
CREATE INDEX IF NOT EXISTS idx_lineitem_suppkey ON lineitem(l_suppkey);
CREATE INDEX IF NOT EXISTS idx_lineitem_shipdate ON lineitem(l_shipdate);
CREATE INDEX IF NOT EXISTS idx_customer_nationkey ON customer(c_nationkey);
CREATE INDEX IF NOT EXISTS idx_supplier_nationkey ON supplier(s_nationkey);
