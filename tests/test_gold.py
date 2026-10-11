import sys, os
sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from datetime import date
from pipeline.gold.gold_processing import (
    get_description_lookup,
    get_top_country_lookup,
    build_gold_daily_product_sales,
    build_gold_top_anomalies
)

def test_description_lookup_picks_most_frequent(spark):
    data = [
        ("22423", "REGENCY CAKESTAND 3 TIER"),
        ("22423", "REGENCY CAKESTAND 3 TIER"),
        ("22423", "regency cakestand 3 tier"),
        ("85099B", "JUMBO BAG 3 TIER")
    ]

    df = spark.createDataFrame(data, ["stockcode", "description"])
    result_df = get_description_lookup(df)
    rows = {r["stockcode"]: r["description"] for r in result_df.collect()}

    assert rows["22423"] == "REGENCY CAKESTAND 3 TIER"
    assert rows["85099B"] == "JUMBO BAG 3 TIER"
    assert result_df.count() == 2

def test_top_country_lookup_picks_highest_revenue(spark):
    from datetime import datetime

    data = [
        (datetime(2011, 11, 3), "22423", "UK", False, 100.0),
        (datetime(2011, 11, 3), "22423", "France", False, 500.0),
        (datetime(2011, 11, 3), "22423", "Germany", False, 50.0)
    ]    

    df = spark.createDataFrame(data, ["invoicedate", "stockcode", "country", "is_cancelled", "total_amount"])
    result_df = get_top_country_lookup(df)
    row = result_df.collect()[0]
    assert row["top_country"] == "France"
    

def test_build_gold_daily_product_sales_preserves_row_count(spark): 
    anomaly_data = [ (date(2011, 11, 3), "22423", 10, 50.0, 2, 5.0, 0, 0.1, False), ]
    anomaly_df = spark.createDataFrame(anomaly_data, [ "sale_date", "stockcode", "total_quantity", "total_revenue", "total_transactions", "avg_price", "num_cancellations", "anomaly_score", "is_anomaly" ]) 

    desc_df = spark.createDataFrame( [("22423", "REGENCY CAKESTAND 3 TIER")], ["stockcode", "description"]) 
    country_df = spark.createDataFrame( [(date(2011, 11, 3), "22423", "United Kingdom")], ["sale_date", "stockcode", "top_country"]) 
    result = build_gold_daily_product_sales(anomaly_df, desc_df, country_df) 
    
    assert result.count() == 1 # join must not duplicate or drop rows 
    row = result.collect()[0] 
    assert row["description"] == "REGENCY CAKESTAND 3 TIER" 
    assert row["top_country"] == "United Kingdom" 

def test_build_gold_top_anomalies_filters_and_orders(spark): 
    data = [ (date(2011, 11, 1), "A", 0.5, False), (date(2011, 11, 2), "B", -0.3, True), (date(2011, 11, 3), "C", -0.1, True), ] 
    df = spark.createDataFrame(data, ["sale_date", "stockcode", "anomaly_score", "is_anomaly"]) 
    result = build_gold_top_anomalies(df, top_n=10) 
    rows = result.collect() 
    assert len(rows) == 2 # only is_anomaly=True rows 
    assert rows[0]["stockcode"] == "B" # most negative score first    