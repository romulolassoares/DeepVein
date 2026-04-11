from src.database.database_factory import DatabaseFactory

def main():
    kwargs = {"database": "rlass"}
    db = DatabaseFactory.build(engine="sqlserver", **kwargs)

    result = db.execute_stream("SELECT * from tb_test_script", chunk_size=10000)

    print(result)

if __name__ == "__main__":
    main()
