from src.database.database_factory import DatabaseFactory

def main():
    kwargs = {"database": "rlass"}
    db = DatabaseFactory.build(engine="sqlserver", **kwargs)

    result = db.execute("SELECT 1 as 'test'")

    print(result)

if __name__ == "__main__":
    main()
