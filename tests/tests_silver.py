import sys,os
sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality
from pipeline.silver.silver_processing import (cast_types, deduplicate, add_derived_columns)

def test_deduplicate_removes_exact_duplicates(spark):
    data = [
        ("541909", "22443", 3, 12.75),
        ("541909", "22443", 3, 12.75),
        ("541809", "22423", 3, 12.55),
    ]
    columns = ["invoice", "stockcode", "quantity", "price"]
    df = spark.createDataFrame(data, columns)

    result_df = deduplicate(df)

    assert result_df.count() == 2


def test_add_derived_columns(spark):
    data = [
        ("541909", "22443", 3, 12.75),
        ("C541909", "22443", 3, 12.75)
    ]
    df = spark.createDataFrame(data, ["invoice", "stockcode", "quantity", "price"])

    result_df = add_derived_columns(df)
    rows = {r["invoice"]: r for r in result_df.collect()}
    assert rows["541909"]["is_cancelled"] == False
    assert rows["C541909"]["is_cancelled"] == True
    assert rows["541909"]["total_amount"] == 3 * 12.75

def test_cast_types_produces_correct_types(spark):
    data = [
        ("22443", "5","2021-01-01 10:00:00", "12.75" )
      ]
    df = spark.createDataFrame(data, ["invoice", "quantity", "invoicedate", "price",])

    result_df = cast_types(df)
    schema = {f.name: f.dataType.typeName() for f in result_df.schema.fields}

    assert schema["quantity"] == "integer"     
    assert schema["price"] == "double"     
    assert schema["invoicedate"] == "timestamp"   