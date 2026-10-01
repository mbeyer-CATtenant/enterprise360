# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "45e498e1-6aaf-42e0-b0a1-926b4c34cf22",
# META       "default_lakehouse_name": "enterprise360_lh",
# META       "default_lakehouse_workspace_id": "e809baaa-4226-4e06-93a2-26cfb3476f49",
# META       "known_lakehouses": [
# META         {
# META           "id": "45e498e1-6aaf-42e0-b0a1-926b4c34cf22"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Enterprise 360 synthetic data generator
# 
# Run this notebook in Microsoft Fabric with a **schema-enabled lakehouse**
# attached as the default lakehouse. It creates deterministic synthetic data
# for commercial, supply-chain, workforce, asset, and service domains, then
# writes managed Delta tables to `bronze`, `silver`, and `gold` schemas.
# 
# The Gold tables are shaped for three Direct Lake on OneLake semantic models:
# Commercial Analytics, Supply Chain Analytics, and Service Operations.


# PARAMETERS CELL ********************

# Fabric pipeline parameters. Mark this cell as the parameter cell after import.
SCALE = "small"  # small, medium, or large
SEED = 20261001
START_DATE = "2024-01-01"
END_DATE = "2026-09-30"
WRITE_MODE = "overwrite"
OPTIMIZE_GOLD = False


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 1. Runtime validation and configuration


# CELL ********************

from datetime import datetime, timezone
import json
import uuid

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

context = notebookutils.runtime.context
workspace_id = context["currentWorkspaceId"]
lakehouse_id = context["defaultLakehouseId"]
is_for_pipeline = context["isForPipeline"]

if not lakehouse_id:
    raise RuntimeError(
        "Attach a schema-enabled lakehouse as the notebook's default lakehouse before running."
    )

scale_profiles = {
    "small": {
        "customers": 2_500,
        "products": 500,
        "suppliers": 75,
        "facilities": 8,
        "employees": 500,
        "technicians": 200,
        "assets": 10_000,
        "orders": 50_000,
        "order_lines": 150_000,
        "purchase_order_lines": 35_000,
        "shipments": 40_000,
        "inventory_snapshots": 400_000,
        "work_orders": 35_000,
        "work_order_parts": 70_000,
        "asset_daily_status": 300_000,
    },
    "medium": {
        "customers": 25_000,
        "products": 2_000,
        "suppliers": 250,
        "facilities": 15,
        "employees": 2_000,
        "technicians": 1_000,
        "assets": 50_000,
        "orders": 500_000,
        "order_lines": 1_500_000,
        "purchase_order_lines": 300_000,
        "shipments": 400_000,
        "inventory_snapshots": 5_000_000,
        "work_orders": 250_000,
        "work_order_parts": 500_000,
        "asset_daily_status": 2_000_000,
    },
    "large": {
        "customers": 100_000,
        "products": 8_000,
        "suppliers": 1_000,
        "facilities": 40,
        "employees": 8_000,
        "technicians": 3_000,
        "assets": 250_000,
        "orders": 2_000_000,
        "order_lines": 6_000_000,
        "purchase_order_lines": 1_200_000,
        "shipments": 1_600_000,
        "inventory_snapshots": 20_000_000,
        "work_orders": 1_000_000,
        "work_order_parts": 2_000_000,
        "asset_daily_status": 10_000_000,
    },
}

if SCALE not in scale_profiles:
    raise ValueError(f"SCALE must be one of {sorted(scale_profiles)}")

N = scale_profiles[SCALE]
BATCH_ID = str(uuid.uuid4())
LOAD_TS = datetime.now(timezone.utc).replace(tzinfo=None)
DAY_COUNT = (
    datetime.fromisoformat(END_DATE) - datetime.fromisoformat(START_DATE)
).days + 1

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.microsoft.delta.parquet.vorder.enabled", "true")

for schema_name in ("bronze", "silver", "gold"):
    try:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
    except Exception as exc:
        raise RuntimeError(
            "This notebook requires a schema-enabled Fabric lakehouse."
        ) from exc

print(
    json.dumps(
        {
            "workspace_id": workspace_id,
            "lakehouse_id": lakehouse_id,
            "is_for_pipeline": is_for_pipeline,
            "scale": SCALE,
            "batch_id": BATCH_ID,
            "counts": N,
        },
        indent=2,
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 2. Deterministic generation helpers


# CELL ********************

REGIONS = ["NA-East", "NA-Central", "NA-West", "EMEA", "APAC", "LATAM"]
COUNTRIES = ["US", "CA", "GB", "DE", "FR", "JP", "AU", "BR"]
CITIES = [
    "Atlanta",
    "Chicago",
    "Dallas",
    "Denver",
    "London",
    "Munich",
    "Paris",
    "Tokyo",
    "Sydney",
    "Sao Paulo",
]
INDUSTRIES = [
    "Manufacturing",
    "Energy",
    "Healthcare",
    "Retail",
    "Transportation",
    "Food and Beverage",
]
COMPANY_PREFIXES = [
    "Apex",
    "Beacon",
    "Crest",
    "Delta",
    "Evergreen",
    "Frontier",
    "Global",
    "Horizon",
    "Ironwood",
    "Juniper",
]
COMPANY_SUFFIXES = [
    "Industries",
    "Manufacturing",
    "Logistics",
    "Systems",
    "Holdings",
    "Services",
]
FIRST_NAMES = [
    "Alex",
    "Avery",
    "Casey",
    "Devon",
    "Jordan",
    "Morgan",
    "Riley",
    "Sam",
    "Taylor",
    "Quinn",
]
LAST_NAMES = [
    "Anderson",
    "Brown",
    "Chen",
    "Davis",
    "Garcia",
    "Johnson",
    "Khan",
    "Lee",
    "Patel",
    "Wilson",
]
PRODUCT_CATEGORIES = [
    "Industrial Equipment",
    "Vehicles",
    "Controls",
    "Sensors",
    "Replacement Parts",
]
PRODUCT_FAMILIES = [
    "Compressor",
    "Pump",
    "Conveyor",
    "Generator",
    "Controller",
    "Telemetry Sensor",
    "Service Kit",
]
CARRIERS = ["NorthStar Freight", "BlueLine Logistics", "RapidRail", "Global Air Cargo"]


def hash_bucket(column_name: str, salt: int, modulo: int):
    return F.pmod(
        F.xxhash64(F.col(column_name), F.lit(SEED + salt)),
        F.lit(modulo),
    ).cast("int")


def pick(values, index_col):
    return F.element_at(F.array(*[F.lit(value) for value in values]), index_col + 1)


def synthetic_id(prefix: str, numeric_col, width: int = 8):
    return F.concat(F.lit(prefix), F.lpad(numeric_col.cast("string"), width, "0"))


def generated_date(id_column: str, salt: int = 0):
    return F.date_add(
        F.lit(START_DATE).cast("date"),
        hash_bucket(id_column, salt, DAY_COUNT),
    )


def add_lineage(df: DataFrame, source_system: str) -> DataFrame:
    return (
        df.withColumn("ingestion_timestamp", F.lit(LOAD_TS).cast("timestamp"))
        .withColumn("pipeline_run_id", F.lit(BATCH_ID))
        .withColumn("source_system", F.lit(source_system))
    )


def write_delta(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode(WRITE_MODE)
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )


def require_unique(table_name: str, keys) -> None:
    duplicate = (
        spark.table(table_name)
        .groupBy(*keys)
        .count()
        .where(F.col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError(f"{table_name} has duplicate key values for {keys}")


def require_no_orphans(
    child_table: str,
    child_key: str,
    parent_table: str,
    parent_key: str,
) -> None:
    child = spark.table(child_table).select(child_key).where(F.col(child_key).isNotNull())
    parent = spark.table(parent_table).select(parent_key)
    orphan = (
        child.join(parent, child[child_key] == parent[parent_key], "left_anti")
        .limit(1)
        .count()
    )
    if orphan:
        raise ValueError(
            f"{child_table}.{child_key} contains values missing from "
            f"{parent_table}.{parent_key}"
        )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 3. Enterprise dimensions


# CELL ********************

date_dim = (
    spark.sql(
        f"""
        SELECT explode(
            sequence(to_date('{START_DATE}'), to_date('{END_DATE}'), interval 1 day)
        ) AS full_date
        """
    )
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("calendar_year", F.year("full_date"))
    .withColumn("calendar_quarter", F.quarter("full_date"))
    .withColumn("calendar_month", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("week_of_year", F.weekofyear("full_date"))
    .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
    .withColumn("is_weekend", F.dayofweek("full_date").isin(1, 7))
)

facility_seed = spark.range(N["facilities"]).withColumn("n", F.col("id") + 1)
facility = (
    facility_seed.withColumn("facility_key", F.col("n").cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("n"), 4))
    .withColumn("facility_name", F.concat(F.lit("Enterprise Facility "), F.col("n")))
    .withColumn("facility_type", pick(["Plant", "Distribution Center", "Service Hub"], hash_bucket("id", 1, 3)))
    .withColumn("region_code", pick(REGIONS, hash_bucket("id", 2, len(REGIONS))))
    .withColumn("country_code", pick(COUNTRIES, hash_bucket("id", 3, len(COUNTRIES))))
    .withColumn("city", pick(CITIES, hash_bucket("id", 4, len(CITIES))))
    .withColumn("capacity_units", (hash_bucket("id", 5, 9000) + 1000).cast("long"))
    .withColumn("status", F.lit("Active"))
    .withColumn("created_on", F.lit("2018-01-01").cast("date"))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

customer_seed = spark.range(N["customers"]).withColumn("n", F.col("id") + 1)
customer = (
    customer_seed.withColumn("customer_key", F.col("n").cast("long"))
    .withColumn("customer_id", synthetic_id("CUS", F.col("n")))
    .withColumn(
        "customer_name",
        F.concat_ws(
            " ",
            pick(COMPANY_PREFIXES, hash_bucket("id", 10, len(COMPANY_PREFIXES))),
            pick(COMPANY_SUFFIXES, hash_bucket("id", 11, len(COMPANY_SUFFIXES))),
            F.lpad(F.col("n").cast("string"), 5, "0"),
        ),
    )
    .withColumn("customer_segment", pick(["Strategic", "Enterprise", "Commercial", "SMB"], hash_bucket("id", 12, 4)))
    .withColumn("industry", pick(INDUSTRIES, hash_bucket("id", 13, len(INDUSTRIES))))
    .withColumn("region_code", pick(REGIONS, hash_bucket("id", 14, len(REGIONS))))
    .withColumn("country_code", pick(COUNTRIES, hash_bucket("id", 15, len(COUNTRIES))))
    .withColumn("city", pick(CITIES, hash_bucket("id", 16, len(CITIES))))
    .withColumn("annual_revenue", ((hash_bucket("id", 17, 950_000) + 50_000) * 100).cast("decimal(18,2)"))
    .withColumn(
        "risk_tier",
        F.when(hash_bucket("id", 18, 10) < 6, "Low")
        .when(hash_bucket("id", 18, 10) < 9, "Medium")
        .otherwise("High"),
    )
    .withColumn("status", F.when(hash_bucket("id", 19, 20) == 0, "Inactive").otherwise("Active"))
    .withColumn("created_on", F.date_add(F.lit("2016-01-01").cast("date"), hash_bucket("id", 20, 2922)))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

product_seed = spark.range(N["products"]).withColumn("n", F.col("id") + 1)
product = (
    product_seed.withColumn("product_key", F.col("n").cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("n"), 6))
    .withColumn("sku", F.concat(F.lit("SKU-"), F.lpad(F.col("n").cast("string"), 7, "0")))
    .withColumn("product_category", pick(PRODUCT_CATEGORIES, hash_bucket("id", 30, len(PRODUCT_CATEGORIES))))
    .withColumn("product_family", pick(PRODUCT_FAMILIES, hash_bucket("id", 31, len(PRODUCT_FAMILIES))))
    .withColumn(
        "product_name",
        F.concat(
            pick(PRODUCT_FAMILIES, hash_bucket("id", 31, len(PRODUCT_FAMILIES))),
            F.lit(" Model "),
            F.lpad(F.col("n").cast("string"), 5, "0"),
        ),
    )
    .withColumn("standard_cost", (F.lit(50.0) + hash_bucket("id", 32, 50000) / F.lit(10.0)).cast("decimal(18,2)"))
    .withColumn("list_price", (F.col("standard_cost") * (F.lit(1.25) + hash_bucket("id", 33, 55) / F.lit(100.0))).cast("decimal(18,2)"))
    .withColumn("lead_time_days", (hash_bucket("id", 34, 55) + 5).cast("int"))
    .withColumn("weight_kg", (F.lit(0.5) + hash_bucket("id", 35, 25000) / F.lit(10.0)).cast("decimal(12,2)"))
    .withColumn("critical_spare", hash_bucket("id", 36, 10) < 2)
    .withColumn("status", F.when(hash_bucket("id", 37, 25) == 0, "Discontinued").otherwise("Active"))
    .withColumn("created_on", F.date_add(F.lit("2018-01-01").cast("date"), hash_bucket("id", 38, 2191)))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

supplier_seed = spark.range(N["suppliers"]).withColumn("n", F.col("id") + 1)
supplier = (
    supplier_seed.withColumn("supplier_key", F.col("n").cast("long"))
    .withColumn("supplier_id", synthetic_id("SUP", F.col("n"), 5))
    .withColumn(
        "supplier_name",
        F.concat_ws(
            " ",
            pick(COMPANY_PREFIXES, hash_bucket("id", 40, len(COMPANY_PREFIXES))),
            F.lit("Supply"),
            F.lpad(F.col("n").cast("string"), 4, "0"),
        ),
    )
    .withColumn("region_code", pick(REGIONS, hash_bucket("id", 41, len(REGIONS))))
    .withColumn("country_code", pick(COUNTRIES, hash_bucket("id", 42, len(COUNTRIES))))
    .withColumn("supplier_tier", pick(["Preferred", "Approved", "Conditional"], hash_bucket("id", 43, 3)))
    .withColumn("contract_start_date", F.date_add(F.lit("2019-01-01").cast("date"), hash_bucket("id", 44, 1826)))
    .withColumn("status", F.when(hash_bucket("id", 45, 20) == 0, "On Hold").otherwise("Active"))
    .withColumn("created_on", F.lit("2019-01-01").cast("date"))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

employee_seed = spark.range(N["employees"]).withColumn("n", F.col("id") + 1)
employee = (
    employee_seed.withColumn("employee_key", F.col("n").cast("long"))
    .withColumn("employee_id", synthetic_id("EMP", F.col("n"), 6))
    .withColumn(
        "employee_name",
        F.concat_ws(
            " ",
            pick(FIRST_NAMES, hash_bucket("id", 50, len(FIRST_NAMES))),
            pick(LAST_NAMES, hash_bucket("id", 51, len(LAST_NAMES))),
        ),
    )
    .withColumn("department", pick(["Sales", "Supply Chain", "Field Service", "Finance", "Operations"], hash_bucket("id", 52, 5)))
    .withColumn("job_level", pick(["Associate", "Senior", "Lead", "Manager", "Director"], hash_bucket("id", 53, 5)))
    .withColumn("facility_key", (hash_bucket("id", 54, N["facilities"]) + 1).cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("facility_key"), 4))
    .withColumn("hire_date", F.date_add(F.lit("2015-01-01").cast("date"), hash_bucket("id", 55, 4018)))
    .withColumn("status", F.when(hash_bucket("id", 56, 30) == 0, "Leave").otherwise("Active"))
    .withColumn("created_on", F.col("hire_date"))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

technician_seed = spark.range(N["technicians"]).withColumn("n", F.col("id") + 1)
technician = (
    technician_seed.withColumn("technician_key", F.col("n").cast("long"))
    .withColumn("technician_id", synthetic_id("TEC", F.col("n"), 6))
    .withColumn("employee_key", (hash_bucket("id", 60, N["employees"]) + 1).cast("long"))
    .withColumn("employee_id", synthetic_id("EMP", F.col("employee_key"), 6))
    .withColumn("facility_key", (hash_bucket("id", 61, N["facilities"]) + 1).cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("facility_key"), 4))
    .withColumn("skill_level", pick(["Level 1", "Level 2", "Level 3", "Master"], hash_bucket("id", 62, 4)))
    .withColumn("specialty", pick(PRODUCT_FAMILIES, hash_bucket("id", 63, len(PRODUCT_FAMILIES))))
    .withColumn("certification_expiry", F.date_add(F.lit(END_DATE).cast("date"), hash_bucket("id", 64, 730) - 180))
    .withColumn("status", F.lit("Active"))
    .withColumn("created_on", F.lit("2020-01-01").cast("date"))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

sales_channel = spark.createDataFrame(
    [
        (1, "CH01", "Direct Sales", "Direct"),
        (2, "CH02", "Partner", "Indirect"),
        (3, "CH03", "Digital Commerce", "Digital"),
        (4, "CH04", "Renewal Desk", "Direct"),
    ],
    ["sales_channel_key", "sales_channel_id", "sales_channel_name", "channel_group"],
).withColumn("status", F.lit("Active"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 4. Assets and commercial transactions


# CELL ********************

asset_seed = spark.range(N["assets"]).withColumn("n", F.col("id") + 1)
asset = (
    asset_seed.withColumn("asset_key", F.col("n").cast("long"))
    .withColumn("asset_id", synthetic_id("AST", F.col("n"), 9))
    .withColumn("serial_number", F.concat(F.lit("SN-"), F.lpad(F.col("n").cast("string"), 12, "0")))
    .withColumn("customer_key", (hash_bucket("id", 70, N["customers"]) + 1).cast("long"))
    .withColumn("customer_id", synthetic_id("CUS", F.col("customer_key")))
    .withColumn("product_key", (hash_bucket("id", 71, N["products"]) + 1).cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("product_key"), 6))
    .withColumn("facility_key", (hash_bucket("id", 72, N["facilities"]) + 1).cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("facility_key"), 4))
    .withColumn("asset_type", pick(["EquipmentAsset", "VehicleAsset"], hash_bucket("id", 73, 5).cast("int") % 2))
    .withColumn("install_date", F.date_add(F.lit("2017-01-01").cast("date"), hash_bucket("id", 74, 3468)))
    .withColumn("warranty_expiry", F.add_months("install_date", 36))
    .withColumn("criticality", pick(["Low", "Medium", "High", "Critical"], hash_bucket("id", 75, 10).cast("int") % 4))
    .withColumn("status", F.when(hash_bucket("id", 76, 25) == 0, "Out of Service").otherwise("Active"))
    .withColumn("created_on", F.col("install_date"))
    .withColumn("modified_on", F.lit(END_DATE).cast("date"))
    .drop("id", "n")
)

order_seed = spark.range(N["orders"]).withColumn("n", F.col("id") + 1)
order_base = (
    order_seed.withColumn("sales_order_key", F.col("n").cast("long"))
    .withColumn("sales_order_id", synthetic_id("SO", F.col("n"), 10))
    .withColumn("customer_key", (hash_bucket("id", 80, N["customers"]) + 1).cast("long"))
    .withColumn("customer_id", synthetic_id("CUS", F.col("customer_key")))
    .withColumn("sales_channel_key", (hash_bucket("id", 81, 4) + 1).cast("long"))
    .withColumn("sales_channel_id", synthetic_id("CH", F.col("sales_channel_key"), 2))
    .withColumn("order_date", generated_date("id", 82))
    .withColumn("requested_date", F.date_add("order_date", hash_bucket("id", 83, 25) + 3))
    .withColumn("currency_code", F.lit("USD"))
    .withColumn(
        "order_status",
        F.when(F.col("order_date") > F.date_sub(F.lit(END_DATE).cast("date"), 30), "Open")
        .when(hash_bucket("id", 84, 20) == 0, "Cancelled")
        .otherwise("Completed"),
    )
    .select(
        "id",
        "sales_order_key",
        "sales_order_id",
        "customer_key",
        "customer_id",
        "sales_channel_key",
        "sales_channel_id",
        "order_date",
        "requested_date",
        "currency_code",
        "order_status",
    )
)

order_line_seed = spark.range(N["order_lines"]).withColumn("n", F.col("id") + 1)
order_line = (
    order_line_seed.withColumn("sales_order_line_id", synthetic_id("SOL", F.col("n"), 11))
    .withColumn("order_index", F.floor(F.col("id") * N["orders"] / N["order_lines"]).cast("long"))
    .withColumn("sales_order_key", (F.col("order_index") + 1).cast("long"))
    .withColumn("sales_order_id", synthetic_id("SO", F.col("sales_order_key"), 10))
    .withColumn("product_key", (hash_bucket("id", 90, N["products"]) + 1).cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("product_key"), 6))
    .withColumn("quantity", (hash_bucket("id", 91, 12) + 1).cast("int"))
    .withColumn("discount_percent", (hash_bucket("id", 92, 21) / F.lit(100.0)).cast("decimal(5,4)"))
    .join(
        F.broadcast(product.select("product_id", "standard_cost", "list_price")),
        "product_id",
        "inner",
    )
    .withColumn("unit_price", F.col("list_price"))
    .withColumn(
        "line_revenue",
        (F.col("quantity") * F.col("unit_price") * (F.lit(1) - F.col("discount_percent"))).cast("decimal(18,2)"),
    )
    .withColumn("line_cost", (F.col("quantity") * F.col("standard_cost")).cast("decimal(18,2)"))
    .select(
        "sales_order_line_id",
        "sales_order_key",
        "sales_order_id",
        "product_key",
        "product_id",
        "quantity",
        "unit_price",
        "discount_percent",
        "line_revenue",
        "line_cost",
    )
)

order_totals = order_line.groupBy("sales_order_id").agg(
    F.sum("line_revenue").cast("decimal(18,2)").alias("order_revenue"),
    F.sum("line_cost").cast("decimal(18,2)").alias("order_cost"),
)

sales_order = (
    order_base.drop("id")
    .join(order_totals, "sales_order_id", "inner")
    .withColumn("gross_margin", (F.col("order_revenue") - F.col("order_cost")).cast("decimal(18,2)"))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 5. Supply-chain transactions


# CELL ********************

supplier_product_seed = spark.range(N["products"] * 4).withColumn("n", F.col("id") + 1)
supplier_product = (
    supplier_product_seed.withColumn("supplier_product_id", synthetic_id("SP", F.col("n"), 9))
    .withColumn("product_index", F.floor(F.col("id") / 4).cast("long"))
    .withColumn("product_key", (F.col("product_index") + 1).cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("product_key"), 6))
    .withColumn("supplier_key", (hash_bucket("id", 100, N["suppliers"]) + 1).cast("long"))
    .withColumn("supplier_id", synthetic_id("SUP", F.col("supplier_key"), 5))
    .withColumn("supplier_part_number", F.concat(F.lit("SPN-"), F.lpad(F.col("n").cast("string"), 9, "0")))
    .withColumn("minimum_order_quantity", (hash_bucket("id", 101, 20) + 1).cast("int"))
    .withColumn("preferred_supplier", F.pmod(F.col("id"), F.lit(4)) == 0)
    .drop("id", "n", "product_index")
)

po_line_seed = spark.range(N["purchase_order_lines"]).withColumn("n", F.col("id") + 1)
purchase_order_line = (
    po_line_seed.withColumn("purchase_order_line_id", synthetic_id("POL", F.col("n"), 10))
    .withColumn("purchase_order_id", synthetic_id("PO", F.floor(F.col("id") / 3) + 1, 9))
    .withColumn("supplier_key", (hash_bucket("id", 110, N["suppliers"]) + 1).cast("long"))
    .withColumn("supplier_id", synthetic_id("SUP", F.col("supplier_key"), 5))
    .withColumn("product_key", (hash_bucket("id", 111, N["products"]) + 1).cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("product_key"), 6))
    .withColumn("facility_key", (hash_bucket("id", 112, N["facilities"]) + 1).cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("facility_key"), 4))
    .withColumn("order_date", generated_date("id", 113))
    .withColumn("expected_date", F.date_add("order_date", hash_bucket("id", 114, 45) + 5))
    .withColumn("received_date", F.date_add("expected_date", hash_bucket("id", 115, 18) - 5))
    .withColumn("ordered_quantity", (hash_bucket("id", 116, 200) + 20).cast("int"))
    .withColumn("received_quantity", F.greatest(F.lit(0), F.col("ordered_quantity") - hash_bucket("id", 117, 25)).cast("int"))
    .join(F.broadcast(product.select("product_id", "standard_cost")), "product_id", "inner")
    .withColumn("unit_cost", (F.col("standard_cost") * (F.lit(0.85) + hash_bucket("id", 118, 20) / F.lit(100.0))).cast("decimal(18,2)"))
    .withColumn(
        "purchase_order_status",
        F.when(F.col("received_date") > F.lit(END_DATE).cast("date"), "Open")
        .when(F.col("received_quantity") < F.col("ordered_quantity"), "Partially Received")
        .otherwise("Received"),
    )
    .drop("id", "n", "standard_cost")
)

shipment_seed = spark.range(N["shipments"]).withColumn("n", F.col("id") + 1)
shipment = (
    shipment_seed.withColumn("shipment_id", synthetic_id("SHP", F.col("n"), 10))
    .withColumn("sales_order_key", (hash_bucket("id", 120, N["orders"]) + 1).cast("long"))
    .withColumn("sales_order_id", synthetic_id("SO", F.col("sales_order_key"), 10))
    .withColumn("facility_key", (hash_bucket("id", 121, N["facilities"]) + 1).cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("facility_key"), 4))
    .withColumn("carrier_name", pick(CARRIERS, hash_bucket("id", 122, len(CARRIERS))))
    .withColumn("ship_date", generated_date("id", 123))
    .withColumn("promised_delivery_date", F.date_add("ship_date", hash_bucket("id", 124, 10) + 2))
    .withColumn("actual_delivery_date", F.date_add("promised_delivery_date", hash_bucket("id", 125, 9) - 3))
    .withColumn("freight_cost", (F.lit(75.0) + hash_bucket("id", 126, 25000) / F.lit(10.0)).cast("decimal(18,2)"))
    .withColumn("quality_hold", hash_bucket("id", 127, 100) < 2)
    .withColumn(
        "shipment_status",
        F.when(F.col("quality_hold"), "Quality Hold")
        .when(F.col("actual_delivery_date") > F.lit(END_DATE).cast("date"), "In Transit")
        .otherwise("Delivered"),
    )
    .drop("id", "n")
)

inventory_seed = spark.range(N["inventory_snapshots"]).withColumn("n", F.col("id") + 1)
inventory_snapshot = (
    inventory_seed.withColumn("inventory_snapshot_id", synthetic_id("INV", F.col("n"), 12))
    .withColumn("snapshot_date", generated_date("id", 130))
    .withColumn("facility_key", (hash_bucket("id", 131, N["facilities"]) + 1).cast("long"))
    .withColumn("facility_id", synthetic_id("FAC", F.col("facility_key"), 4))
    .withColumn("product_key", (hash_bucket("id", 132, N["products"]) + 1).cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("product_key"), 6))
    .withColumn("quantity_on_hand", hash_bucket("id", 133, 750).cast("int"))
    .withColumn("quantity_allocated", hash_bucket("id", 134, 250).cast("int"))
    .withColumn("safety_stock_quantity", (hash_bucket("id", 135, 120) + 20).cast("int"))
    .withColumn("reorder_point", (F.col("safety_stock_quantity") + hash_bucket("id", 136, 80)).cast("int"))
    .withColumn("stockout_flag", F.col("quantity_on_hand") < F.col("safety_stock_quantity"))
    .drop("id", "n")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 6. Service and asset operations


# CELL ********************

work_order_seed = spark.range(N["work_orders"]).withColumn("n", F.col("id") + 1)
work_order = (
    work_order_seed.withColumn("work_order_id", synthetic_id("WO", F.col("n"), 10))
    .withColumn("work_order_key", F.col("n").cast("long"))
    .withColumn("asset_key", (hash_bucket("id", 140, N["assets"]) + 1).cast("long"))
    .withColumn("asset_id", synthetic_id("AST", F.col("asset_key"), 9))
    .withColumn("technician_key", (hash_bucket("id", 141, N["technicians"]) + 1).cast("long"))
    .withColumn("technician_id", synthetic_id("TEC", F.col("technician_key"), 6))
    .withColumn("opened_date", generated_date("id", 142))
    .withColumn("priority", pick(["Low", "Medium", "High", "Critical"], hash_bucket("id", 143, 10).cast("int") % 4))
    .withColumn("work_order_type", pick(["Preventive", "Corrective", "Inspection", "Installation"], hash_bucket("id", 144, 4)))
    .withColumn(
        "duration_hours",
        (
            F.lit(1.0)
            + hash_bucket("id", 145, 72)
            + F.when(F.col("priority") == "Critical", 24).otherwise(0)
        ).cast("decimal(10,2)"),
    )
    .withColumn("resolved_date", F.date_add("opened_date", F.ceil(F.col("duration_hours") / 24).cast("int")))
    .withColumn(
        "work_order_status",
        F.when(F.col("resolved_date") > F.lit(END_DATE).cast("date"), "Open")
        .when(hash_bucket("id", 146, 20) == 0, "Waiting for Parts")
        .otherwise("Completed"),
    )
    .withColumn("labor_cost", (F.col("duration_hours") * (F.lit(65) + hash_bucket("id", 147, 75))).cast("decimal(18,2)"))
    .drop("id", "n")
)

work_order_part_seed = spark.range(N["work_order_parts"]).withColumn("n", F.col("id") + 1)
work_order_part = (
    work_order_part_seed.withColumn("work_order_part_id", synthetic_id("WOP", F.col("n"), 11))
    .withColumn("work_order_key", (hash_bucket("id", 150, N["work_orders"]) + 1).cast("long"))
    .withColumn("work_order_id", synthetic_id("WO", F.col("work_order_key"), 10))
    .withColumn("product_key", (hash_bucket("id", 151, N["products"]) + 1).cast("long"))
    .withColumn("product_id", synthetic_id("PRD", F.col("product_key"), 6))
    .withColumn("quantity_used", (hash_bucket("id", 152, 6) + 1).cast("int"))
    .join(F.broadcast(product.select("product_id", "standard_cost")), "product_id", "inner")
    .withColumn("part_cost", (F.col("quantity_used") * F.col("standard_cost")).cast("decimal(18,2)"))
    .drop("id", "n", "standard_cost")
)

status_seed = spark.range(N["asset_daily_status"]).withColumn("n", F.col("id") + 1)
asset_daily_status = (
    status_seed.withColumn("asset_daily_status_id", synthetic_id("ADS", F.col("n"), 12))
    .withColumn("asset_key", (hash_bucket("id", 160, N["assets"]) + 1).cast("long"))
    .withColumn("asset_id", synthetic_id("AST", F.col("asset_key"), 9))
    .withColumn("status_date", generated_date("id", 161))
    .withColumn("operating_hours", (hash_bucket("id", 162, 240) / F.lit(10.0)).cast("decimal(5,1)"))
    .withColumn("downtime_hours", (F.lit(24.0) - F.col("operating_hours")).cast("decimal(5,1)"))
    .withColumn("health_score", (F.lit(45.0) + hash_bucket("id", 163, 551) / F.lit(10.0)).cast("decimal(5,1)"))
    .withColumn("temperature_c", (F.lit(15.0) + hash_bucket("id", 164, 700) / F.lit(10.0)).cast("decimal(5,1)"))
    .withColumn("alert_count", hash_bucket("id", 165, 6).cast("int"))
    .drop("id", "n")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 7. Write Bronze source tables


# CELL ********************

bronze_frames = {
    "date": date_dim,
    "facility": facility,
    "customer": customer,
    "product": product,
    "supplier": supplier,
    "employee": employee,
    "technician": technician,
    "sales_channel": sales_channel,
    "asset": asset,
    "sales_order": sales_order,
    "sales_order_line": order_line,
    "supplier_product": supplier_product,
    "purchase_order_line": purchase_order_line,
    "shipment": shipment,
    "inventory_snapshot": inventory_snapshot,
    "work_order": work_order,
    "work_order_part": work_order_part,
    "asset_daily_status": asset_daily_status,
}

for table, frame in bronze_frames.items():
    write_delta(add_lineage(frame, f"SYNTHETIC_{table.upper()}"), f"bronze.{table}_raw")
    print(f"Wrote bronze.{table}_raw")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 8. Conform Silver tables


# CELL ********************

table_keys = {
    "date": ["date_key"],
    "facility": ["facility_id"],
    "customer": ["customer_id"],
    "product": ["product_id"],
    "supplier": ["supplier_id"],
    "employee": ["employee_id"],
    "technician": ["technician_id"],
    "sales_channel": ["sales_channel_id"],
    "asset": ["asset_id"],
    "sales_order": ["sales_order_id"],
    "sales_order_line": ["sales_order_line_id"],
    "supplier_product": ["supplier_product_id"],
    "purchase_order_line": ["purchase_order_line_id"],
    "shipment": ["shipment_id"],
    "inventory_snapshot": ["inventory_snapshot_id"],
    "work_order": ["work_order_id"],
    "work_order_part": ["work_order_part_id"],
    "asset_daily_status": ["asset_daily_status_id"],
}

for table, keys in table_keys.items():
    clean = (
        spark.table(f"bronze.{table}_raw")
        .where(F.col(keys[0]).isNotNull())
        .dropDuplicates(keys)
    )
    write_delta(clean, f"silver.{table}")
    require_unique(f"silver.{table}", keys)
    print(f"Wrote and validated silver.{table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 9. Publish Gold star-schema tables


# CELL ********************

gold_map = {
    "dim_date": "date",
    "dim_facility": "facility",
    "dim_customer": "customer",
    "dim_product": "product",
    "dim_supplier": "supplier",
    "dim_employee": "employee",
    "dim_technician": "technician",
    "dim_sales_channel": "sales_channel",
    "dim_asset": "asset",
    "fact_sales_order": "sales_order",
    "fact_sales_order_line": "sales_order_line",
    "bridge_supplier_product": "supplier_product",
    "fact_purchase_order_line": "purchase_order_line",
    "fact_shipment": "shipment",
    "fact_inventory_snapshot": "inventory_snapshot",
    "fact_work_order": "work_order",
    "fact_work_order_part": "work_order_part",
    "fact_asset_daily_status": "asset_daily_status",
}

for gold_table, silver_table in gold_map.items():
    write_delta(spark.table(f"silver.{silver_table}"), f"gold.{gold_table}")
    print(f"Published gold.{gold_table}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 10. Gold-layer integrity checks


# CELL ********************

for table, keys in table_keys.items():
    target = next(name for name, source in gold_map.items() if source == table)
    require_unique(f"gold.{target}", keys)

foreign_keys = [
    ("gold.dim_employee", "facility_key", "gold.dim_facility", "facility_key"),
    ("gold.dim_technician", "employee_key", "gold.dim_employee", "employee_key"),
    ("gold.dim_technician", "facility_key", "gold.dim_facility", "facility_key"),
    ("gold.dim_asset", "customer_key", "gold.dim_customer", "customer_key"),
    ("gold.dim_asset", "product_key", "gold.dim_product", "product_key"),
    ("gold.dim_asset", "facility_key", "gold.dim_facility", "facility_key"),
    ("gold.fact_sales_order", "customer_key", "gold.dim_customer", "customer_key"),
    ("gold.fact_sales_order", "sales_channel_key", "gold.dim_sales_channel", "sales_channel_key"),
    ("gold.fact_sales_order_line", "sales_order_key", "gold.fact_sales_order", "sales_order_key"),
    ("gold.fact_sales_order_line", "product_key", "gold.dim_product", "product_key"),
    ("gold.bridge_supplier_product", "supplier_key", "gold.dim_supplier", "supplier_key"),
    ("gold.bridge_supplier_product", "product_key", "gold.dim_product", "product_key"),
    ("gold.fact_purchase_order_line", "supplier_key", "gold.dim_supplier", "supplier_key"),
    ("gold.fact_purchase_order_line", "product_key", "gold.dim_product", "product_key"),
    ("gold.fact_purchase_order_line", "facility_key", "gold.dim_facility", "facility_key"),
    ("gold.fact_shipment", "sales_order_key", "gold.fact_sales_order", "sales_order_key"),
    ("gold.fact_shipment", "facility_key", "gold.dim_facility", "facility_key"),
    ("gold.fact_inventory_snapshot", "product_key", "gold.dim_product", "product_key"),
    ("gold.fact_inventory_snapshot", "facility_key", "gold.dim_facility", "facility_key"),
    ("gold.fact_work_order", "asset_key", "gold.dim_asset", "asset_key"),
    ("gold.fact_work_order", "technician_key", "gold.dim_technician", "technician_key"),
    ("gold.fact_work_order_part", "work_order_key", "gold.fact_work_order", "work_order_key"),
    ("gold.fact_work_order_part", "product_key", "gold.dim_product", "product_key"),
    ("gold.fact_asset_daily_status", "asset_key", "gold.dim_asset", "asset_key"),
]

for relationship in foreign_keys:
    require_no_orphans(*relationship)

invalid_revenue = (
    spark.table("gold.fact_sales_order_line")
    .where((F.col("line_revenue") <= 0) | (F.col("quantity") <= 0))
    .limit(1)
    .count()
)
if invalid_revenue:
    raise ValueError("Gold sales order lines contain nonpositive quantities or revenue")

invalid_inventory = (
    spark.table("gold.fact_inventory_snapshot")
    .where(
        (F.col("quantity_on_hand") < 0)
        | (F.col("quantity_allocated") < 0)
        | (F.col("safety_stock_quantity") < 0)
    )
    .limit(1)
    .count()
)
if invalid_inventory:
    raise ValueError("Gold inventory contains negative quantities")

print("Gold-layer uniqueness, referential integrity, and business-rule checks passed.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## 11. Direct Lake optimization and load audit


# CELL ********************

large_gold_tables = [
    "fact_sales_order_line",
    "fact_purchase_order_line",
    "fact_shipment",
    "fact_inventory_snapshot",
    "fact_work_order",
    "fact_work_order_part",
    "fact_asset_daily_status",
]

if OPTIMIZE_GOLD:
    for table in large_gold_tables:
        spark.sql(f"OPTIMIZE gold.{table}")
        print(f"Optimized gold.{table}")

audit_rows = []
for gold_table in gold_map:
    row_count = spark.table(f"gold.{gold_table}").count()
    audit_rows.append(
        (
            BATCH_ID,
            SCALE,
            f"gold.{gold_table}",
            row_count,
            LOAD_TS,
            "Succeeded",
        )
    )

audit = spark.createDataFrame(
    audit_rows,
    [
        "pipeline_run_id",
        "scale_profile",
        "table_name",
        "row_count",
        "load_timestamp",
        "status",
    ],
)
write_delta(audit, "gold.load_audit")

display(audit.orderBy("table_name"))
print("Enterprise 360 lakehouse population completed successfully.")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
