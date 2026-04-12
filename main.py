from src.database.database_factory import DatabaseFactory

def main():
    kwargs = {"database": "rlass"}
    db = DatabaseFactory.build(engine="sqlserver", **kwargs)

    query = "SELECT * from tb_test_script"

    # result = db.execute_stream(query, chunk_size=10000)

    result = db.extract_to_parquet(query, output="script_test")

    print(result)

if __name__ == "__main__":
    main()
