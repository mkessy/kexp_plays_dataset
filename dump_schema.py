import duckdb
import pandas as pd


def dump_schema_and_indexes(conn: duckdb.DuckDBPyConnection):
    """
    Dumps the schema, constraints, and indexes for all tables in the database.
    """
    print("Dumping database schema and indexes...")

    # Set pandas display options to show all rows/columns
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 200)
    pd.set_option('display.max_colwidth', None)

    # Get a list of all tables
    tables_df = conn.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """).fetchdf()

    table_names = tables_df['table_name'].tolist()

    if not table_names:
        print("No tables found in the database.")
        return

    print("\n" + "="*80)
    print("TABLE SCHEMAS")
    print("="*80)

    for table_name in table_names:
        print(f"\n-- Schema for table: {table_name}")
        try:
            table_info = conn.execute(
                f"PRAGMA table_info('{table_name}');").fetchdf()
            print(table_info.to_string())
        except duckdb.Error as e:
            print(f"  Could not retrieve schema for {table_name}: {e}")

    print("\n" + "="*80)
    print("CONSTRAINTS (Primary Keys & Unique)")
    print("="*80)
    try:
        constraints_info = conn.execute(
            "SELECT * FROM duckdb_constraints() ORDER BY table_name, constraint_type;").fetchdf()
        if not constraints_info.empty:
            print(constraints_info.to_string())
        else:
            print("No constraints found.")
    except duckdb.Error as e:
        print(f"Could not retrieve constraints: {e}")

    print("\n" + "="*80)
    print("INDEXES")
    print("="*80)
    try:
        indexes_info = conn.execute(
            "SELECT * FROM duckdb_indexes() ORDER BY table_name;").fetchdf()
        if not indexes_info.empty:
            print(indexes_info.to_string())
        else:
            print("No user-defined indexes found.")
    except duckdb.Error as e:
        print(f"Could not retrieve indexes: {e}")


def main():
    db_path = "kexp_data.db"
    print(f"Connecting to DuckDB at {db_path}...")
    with duckdb.connect(db_path, read_only=True) as conn:
        dump_schema_and_indexes(conn)
    print("\n🔐 Database connection closed.")


if __name__ == "__main__":
    main()
