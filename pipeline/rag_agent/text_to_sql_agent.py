import sys
import os

sys.path.append(os.environ.get("PROJECT_ROOT", "D:\\retail_pipeline"))

from pipeline.spark_session import get_spark_session
from pipeline.config import (
    GOLD_DAILY_PATH,
    GOLD_MONTHLY_PATH,
    GOLD_TOP_ANOMALIES_PATH
)

#import ollama
from openai import OpenAI
from colorama import Fore, Style, init
init(autoreset=True)

#MODEL_NAME = "llama3.2:3b"
client  = OpenAI()
MODEL_NAME = "gpt-4o-mini"

TABLES = {
    "gold_daily_product_sales": GOLD_DAILY_PATH,
    "gold_monthly_country_sales": GOLD_MONTHLY_PATH,
    "gold_top_anomalies": GOLD_TOP_ANOMALIES_PATH
}

def load_tables_as_views(spark):

    print(Fore.GREEN + "Loading gold tables and registering as temporary views...")

    for view_name, path in TABLES.items():
        df = spark.read.format("delta").load(path)
        df.createOrReplaceTempView(view_name)
        print(Fore.GREEN + f"Registered '{view_name}' ({df.count()} rows)")

def build_schema_description(spark):

    """
    produces a text block describing each table's column and types,
    so the model knows exactly what it can query
    """        
    lines = []

    for view_name in TABLES:
        df = spark.sql(f"SELECT * FROM {view_name} LIMIT 0")
        cols = ", ".join(f"{f.name} ({f.dataType.simpleString()})" for f in df.schema.fields)
        lines.append(f"Table: {view_name}\nColumns: {cols}\n")

    return "\n".join(lines)

def call_llm(prompt, system_prompt = "You are a helpful SQL assistant."):
    
    response = client.chat.completions.create(
        model = MODEL_NAME,
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt}
        ],
        temperature = 0.0,
    )
    return response.choices[0].message.content.strip()

def generate_sql(question, schema_description):

    prompt = f""" You are a SQL assistant. Given the schema below, write a single
    Spark SQL SELECT query that answers the question. Only output the raw SQL,
    nothing else - no explanation, no markdown formatting, no backticks.

    Schema:
    {schema_description}
    Question: {question}
    SQL:    
                """
    sql = call_llm(prompt)

    #strip any markdown formatting if present
    sql = sql.replace("```sql", "").replace("```","").strip()
    return sql

def is_safe_select(sql):
    """
    Basic guardrail: only allow SELECT statements. Blocks any accidental
     or model hallucinated destructive statements. """

    normalized = sql.strip().lower()
    forbidden = ["insert", "update", "delete", "drop", "alter", "create", "truncate"]

    if not normalized.startswith("select"):
        return False
    if any(word in normalized for word in forbidden):
        return False
    return True

def generate_natural_language_answer(question, sql, result_rows):

    prompt = f""" The user asked : "{question}" 
    The followig SQL was run: {sql}
    The results were: {result_rows}
    Answer the user's question in a clear and concise manner: a sentence or two
    based only on the results. Do not mention sql. """

    answer = call_llm(prompt, system_prompt="You are a helpful assistant that answers questions based on SQL query results.")
    return answer

def run():
    spark = get_spark_session("RetailPipeline_RAGagent")
    spark.sparkContext.setLogLevel("ERROR")

    load_tables_as_views(spark)
    schema_description = build_schema_description(spark)

    print(Fore.GREEN + "\nAgent ready. Ask a question about your retail data."
          "(or type 'exit' to quit)\n")

    while True:
        question = input(Fore.CYAN + "You:").strip()
        if question.lower() in ("exit", "quit"):
            print(Fore.GREEN + "Exiting. Goodbye!")
            break
        if not question:
            print(Fore.RED + "Please enter a question.")
            continue

        sql = generate_sql(question, schema_description)
        print(Fore.YELLOW + f"\n[Generated SQL]:\n{sql}")

        if not is_safe_select(sql):
            print(Fore.RED + "Generated SQL failed safety check, try rephrasing your question.")
            continue

        try:
            result_df = spark.sql(sql)
            result_pdf = result_df.limit(50).toPandas()
        except Exception as e:
            print(Fore.RED + f"Query failed to execute: {e}\n")
            continue

        result_rows = result_pdf.to_dict(orient="records")
        answer = generate_natural_language_answer(question, sql, result_rows)

        print(Fore.GREEN + f"\nAgent: {answer}\n")
    spark.stop()

if __name__ == "__main__":
    run()        