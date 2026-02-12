"""
TPC-H Data Generator for PostgreSQL sourcedb
=============================================
Generates realistic TPC-H-compatible data and loads it into PostgreSQL.
Scale Factor 0.1 = ~800K total rows.

Usage:
    python3 scripts/generate_tpch_data.py [--scale 0.1]

Tables generated:
    region     (5 rows)          - Static
    nation     (25 rows)         - Static
    supplier   (1,000 rows)      - SF * 10,000
    part       (20,000 rows)     - SF * 200,000
    customer   (15,000 rows)     - SF * 150,000
    partsupp   (80,000 rows)     - SF * 800,000
    orders     (150,000 rows)    - SF * 1,500,000
    lineitem   (~600,000 rows)   - SF * 6,000,000
"""

import psycopg2
from psycopg2.extras import execute_values
import random
import string
import argparse
from datetime import date, timedelta, datetime
import sys
import time

# =============================================================
# CONFIG
# =============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "postgres",
    "password": "Gs+163264128",
}

SOURCE_DB = "sourcedb"

# TPC-H Reference Data
REGIONS = [
    (0, "AFRICA"), (1, "AMERICA"), (2, "ASIA"),
    (3, "EUROPE"), (4, "MIDDLE EAST")
]

NATIONS = [
    (0, "ALGERIA", 0), (1, "ARGENTINA", 1), (2, "BRAZIL", 1),
    (3, "CANADA", 1), (4, "EGYPT", 4), (5, "ETHIOPIA", 0),
    (6, "FRANCE", 3), (7, "GERMANY", 3), (8, "INDIA", 2),
    (9, "INDONESIA", 2), (10, "IRAN", 4), (11, "IRAQ", 4),
    (12, "JAPAN", 2), (13, "JORDAN", 4), (14, "KENYA", 0),
    (15, "MOROCCO", 0), (16, "MOZAMBIQUE", 0), (17, "PERU", 1),
    (18, "CHINA", 2), (19, "ROMANIA", 3), (20, "SAUDI ARABIA", 4),
    (21, "VIETNAM", 2), (22, "RUSSIA", 3), (23, "UNITED KINGDOM", 3),
    (24, "UNITED STATES", 1)
]

MARKET_SEGMENTS = ["AUTOMOBILE", "BUILDING", "FURNITURE", "HOUSEHOLD", "MACHINERY"]
ORDER_PRIORITIES = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"]
SHIP_MODES = ["REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"]
SHIP_INSTRUCTS = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"]
PART_TYPES = ["BRASS", "COPPER", "NICKEL", "STEEL", "TIN"]
PART_CONTAINERS = ["SM CASE", "SM BOX", "SM PACK", "SM PKG", "SM JAR",
                   "MED BAG", "MED BOX", "MED PKG", "MED PACK", "MED CAN",
                   "LG CASE", "LG BOX", "LG PACK", "LG PKG", "LG DRUM"]
BRANDS = [f"Brand#{i}{j}" for i in range(1, 6) for j in range(1, 6)]
MANUFACTURERS = [f"Manufacturer#{i}" for i in range(1, 6)]

START_DATE = date(1992, 1, 1)
END_DATE = date(1998, 8, 2)
DATE_RANGE = (END_DATE - START_DATE).days


# =============================================================
# HELPERS
# =============================================================

def random_string(length):
    return ''.join(random.choices(string.ascii_lowercase + ' ', k=length))

def random_phone(nation_key):
    cc = 10 + nation_key
    return f"{cc}-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}"

def random_date():
    return START_DATE + timedelta(days=random.randint(0, DATE_RANGE))

def progress(current, total, label):
    pct = int(current / total * 100)
    bar = "█" * (pct // 2) + "░" * (50 - pct // 2)
    print(f"\r  {label}: {bar} {pct}% ({current:,}/{total:,})", end="", flush=True)


# =============================================================
# DATABASE SETUP
# =============================================================

def create_database(config):
    """Create sourcedb if it doesn't exist."""
    conn = psycopg2.connect(**config, dbname="postgres")
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (SOURCE_DB,))
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {SOURCE_DB}")
        print(f"✅ Database '{SOURCE_DB}' created")
    else:
        print(f"ℹ️  Database '{SOURCE_DB}' already exists")

    cur.close()
    conn.close()


def create_schema(config):
    """Run the DDL script to create tables."""
    conn = psycopg2.connect(**config, dbname=SOURCE_DB)
    cur = conn.cursor()

    schema_file = "scripts/setup_sourcedb.sql"
    with open(schema_file) as f:
        sql = f.read()

    cur.execute(sql)
    conn.commit()
    print("✅ TPC-H schema created (8 tables)")
    cur.close()
    conn.close()


def truncate_tables(config):
    """Truncate all tables for fresh load."""
    conn = psycopg2.connect(**config, dbname=SOURCE_DB)
    cur = conn.cursor()
    cur.execute("""
        TRUNCATE lineitem, orders, partsupp, customer, part,
                 supplier, nation, region CASCADE
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("🗑️  All tables truncated")


# =============================================================
# DATA GENERATION
# =============================================================

def generate_regions(cur):
    data = [(r[0], r[1], random_string(40)) for r in REGIONS]
    execute_values(cur,
        "INSERT INTO region (r_regionkey, r_name, r_comment) VALUES %s",
        data)
    print(f"  ✅ region: {len(data)} rows")

def generate_nations(cur):
    data = [(n[0], n[1], n[2], random_string(60)) for n in NATIONS]
    execute_values(cur,
        "INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) VALUES %s",
        data)
    print(f"  ✅ nation: {len(data)} rows")

def generate_suppliers(cur, sf):
    count = int(10_000 * sf)
    data = []
    for i in range(1, count + 1):
        data.append((
            i,
            f"Supplier#{i:09d}",
            random_string(25),
            random.randint(0, 24),
            random_phone(random.randint(0, 24)),
            round(random.uniform(-999.99, 9999.99), 2),
            random_string(60)
        ))
    execute_values(cur,
        """INSERT INTO supplier
           (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
           VALUES %s""",
        data, page_size=5000)
    print(f"  ✅ supplier: {count:,} rows")
    return count

def generate_parts(cur, sf):
    count = int(200_000 * sf)
    data = []
    for i in range(1, count + 1):
        words = [random.choice(PART_TYPES) for _ in range(3)]
        data.append((
            i,
            ' '.join(words),
            random.choice(MANUFACTURERS),
            random.choice(BRANDS),
            f"{random.choice(['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY'])} {random.choice(PART_TYPES)}",
            random.randint(1, 50),
            random.choice(PART_CONTAINERS),
            round(random.uniform(1.0, 2000.0), 2),
            random_string(15)
        ))
        if i % 10000 == 0:
            progress(i, count, "part")
    execute_values(cur,
        """INSERT INTO part
           (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
           VALUES %s""",
        data, page_size=5000)
    print(f"\n  ✅ part: {count:,} rows")
    return count

def generate_customers(cur, sf):
    count = int(150_000 * sf)
    data = []
    for i in range(1, count + 1):
        nk = random.randint(0, 24)
        data.append((
            i,
            f"Customer#{i:09d}",
            random_string(30),
            nk,
            random_phone(nk),
            round(random.uniform(-999.99, 9999.99), 2),
            random.choice(MARKET_SEGMENTS),
            random_string(70)
        ))
        if i % 10000 == 0:
            progress(i, count, "customer")
    execute_values(cur,
        """INSERT INTO customer
           (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
           VALUES %s""",
        data, page_size=5000)
    print(f"\n  ✅ customer: {count:,} rows")
    return count

def generate_partsupp(cur, sf, num_parts, num_suppliers):
    count = 0
    data = []
    for pk in range(1, num_parts + 1):
        for j in range(4):  # 4 suppliers per part
            sk = ((pk + j * (num_suppliers // 4)) % num_suppliers) + 1
            data.append((
                pk, sk,
                random.randint(1, 9999),
                round(random.uniform(1.0, 1000.0), 2),
                random_string(120)
            ))
            count += 1
        if pk % 10000 == 0:
            progress(pk, num_parts, "partsupp")
    execute_values(cur,
        """INSERT INTO partsupp
           (ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment)
           VALUES %s""",
        data, page_size=5000)
    print(f"\n  ✅ partsupp: {count:,} rows")
    return count

def generate_orders_and_lineitems(cur, sf, num_customers, num_parts, num_suppliers):
    order_count = int(1_500_000 * sf)
    lineitem_count = 0
    order_batch = []
    lineitem_batch = []
    batch_size = 10000

    for ok in range(1, order_count + 1):
        ck = random.randint(1, num_customers)
        od = random_date()
        num_items = random.randint(1, 7)
        total = 0.0

        for ln in range(1, num_items + 1):
            pk = random.randint(1, num_parts)
            sk = random.randint(1, num_suppliers)
            qty = round(random.uniform(1, 50), 2)
            price = round(random.uniform(1.0, 5000.0), 2)
            disc = round(random.uniform(0.0, 0.10), 2)
            tax = round(random.uniform(0.0, 0.08), 2)
            ext = round(qty * price, 2)
            total += ext * (1 - disc) * (1 + tax)

            ship_d = od + timedelta(days=random.randint(1, 121))
            commit_d = od + timedelta(days=random.randint(30, 90))
            receipt_d = ship_d + timedelta(days=random.randint(1, 30))

            lineitem_batch.append((
                ok, pk, sk, ln, qty, ext, disc, tax,
                random.choice(['N', 'R', 'A']),
                random.choice(['O', 'F']),
                ship_d, commit_d, receipt_d,
                random.choice(SHIP_INSTRUCTS),
                random.choice(SHIP_MODES),
                random_string(30)
            ))
            lineitem_count += 1

        status = random.choice(['O', 'F', 'P'])
        order_batch.append((
            ok, ck, status, round(total, 2), od,
            random.choice(ORDER_PRIORITIES),
            f"Clerk#{random.randint(1, 1000):09d}",
            0,
            random_string(50)
        ))

        if ok % batch_size == 0:
            execute_values(cur,
                """INSERT INTO orders
                   (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                    o_orderpriority, o_clerk, o_shippriority, o_comment)
                   VALUES %s""",
                order_batch, page_size=5000)
            execute_values(cur,
                """INSERT INTO lineitem
                   (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
                    l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
                    l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct,
                    l_shipmode, l_comment)
                   VALUES %s""",
                lineitem_batch, page_size=5000)
            order_batch = []
            lineitem_batch = []
            progress(ok, order_count, "orders+lineitem")

    # Flush remaining
    if order_batch:
        execute_values(cur,
            """INSERT INTO orders
               (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                o_orderpriority, o_clerk, o_shippriority, o_comment)
               VALUES %s""",
            order_batch, page_size=5000)
        execute_values(cur,
            """INSERT INTO lineitem
               (l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
                l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
                l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct,
                l_shipmode, l_comment)
               VALUES %s""",
            lineitem_batch, page_size=5000)

    print(f"\n  ✅ orders: {order_count:,} rows")
    print(f"  ✅ lineitem: {lineitem_count:,} rows")
    return order_count, lineitem_count


# =============================================================
# MAIN
# =============================================================

def main():
    parser = argparse.ArgumentParser(description="TPC-H Data Generator")
    parser.add_argument("--scale", type=float, default=0.1,
                        help="Scale factor (0.01=tiny, 0.1=small, 1.0=standard)")
    parser.add_argument("--fresh", action="store_true",
                        help="Truncate all tables before loading")
    args = parser.parse_args()

    sf = args.scale
    print(f"{'='*60}")
    print(f"  TPC-H Data Generator — Scale Factor {sf}")
    print(f"{'='*60}")

    # Step 1: Create database
    print("\n📦 Step 1: Database setup")
    create_database(DB_CONFIG)
    create_schema(DB_CONFIG)

    if args.fresh:
        truncate_tables(DB_CONFIG)

    # Step 2: Generate data
    print(f"\n🔄 Step 2: Generating data (SF={sf})...")
    conn = psycopg2.connect(**DB_CONFIG, dbname=SOURCE_DB)
    cur = conn.cursor()

    start = time.time()

    # Check if data already exists
    cur.execute("SELECT COUNT(*) FROM region")
    if cur.fetchone()[0] > 0 and not args.fresh:
        print("⚠️  Data already exists. Use --fresh to reload.")
        print("    Skipping generation.")
        cur.close()
        conn.close()
        return

    generate_regions(cur)
    generate_nations(cur)
    num_suppliers = generate_suppliers(cur, sf)
    num_parts = generate_parts(cur, sf)
    num_customers = generate_customers(cur, sf)
    conn.commit()  # Commit dimensions first (FK constraints)

    generate_partsupp(cur, sf, num_parts, num_suppliers)
    conn.commit()

    order_count, lineitem_count = generate_orders_and_lineitems(
        cur, sf, num_customers, num_parts, num_suppliers)
    conn.commit()

    elapsed = time.time() - start

    # Step 3: Summary
    print(f"\n{'='*60}")
    print(f"  ✅ TPC-H Data Generation Complete!")
    print(f"  ⏱  Time: {elapsed:.1f} seconds")
    print(f"{'='*60}")

    cur.execute("""
        SELECT relname AS table, n_live_tup AS rows
        FROM pg_stat_user_tables
        ORDER BY n_live_tup DESC
    """)
    print(f"\n  {'Table':<15} {'Rows':>12}")
    print(f"  {'─'*15} {'─'*12}")
    total = 0
    for row in cur.fetchall():
        print(f"  {row[0]:<15} {row[1]:>12,}")
        total += row[1]
    print(f"  {'─'*15} {'─'*12}")
    print(f"  {'TOTAL':<15} {total:>12,}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
