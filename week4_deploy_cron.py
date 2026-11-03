from week3_duckdb_pipeline import run_medallion_pipeline

if __name__ == "__main__":
   # Purani line: print("🚀 [PREFECT DEPLOYMENT] Registering deployment and starting background runner...")
# Nayi line:
    print("[PREFECT DEPLOYMENT] Registering deployment and starting background runner...")
    
    # Prefect 3.x serve mechanism
    run_medallion_pipeline.serve(
        name="sales-duckdb-daily-sync",
        # Cron syntax: Har 2 minute me automatically run karega (Testing ke liye)
        cron="*/2 * * * *",
        tags=["production", "duckdb", "medallion", "sales"],
        description="Automated Medallion ETL pipeline exporting daily Gold revenue mart to Parquet."
    )