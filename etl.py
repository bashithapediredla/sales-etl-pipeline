import pandas as pd
import sqlite3

print("----- Starting ETL Process -----")

try:
    # -------------------- EXTRACT --------------------
    df = pd.read_csv("sales.csv")
    print(f"Loaded {len(df)} rows")

    # -------------------- VALIDATE BEFORE --------------------
    print("\nMissing values BEFORE cleaning:")
    print(df.isnull().sum())

    # -------------------- TRANSFORM --------------------
    # Fill missing values
    df['date'].fillna("2024-01-01", inplace=True)
    df['amount'].fillna(df['amount'].mean(), inplace=True)

    # Convert data types
    df['date'] = pd.to_datetime(df['date'])

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Feature engineering
    df['year'] = df['date'].dt.year

    # -------------------- VALIDATE AFTER --------------------
    print("\nMissing values AFTER cleaning:")
    print(df.isnull().sum())

    print(f"\nRows after cleaning: {len(df)}")

    # -------------------- LOAD --------------------
    conn = sqlite3.connect("sales.db")

    # Check if table exists
    table_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='sales';"
    ).fetchone()

    if table_exists:
        print("\nTable exists → Performing incremental load")

        existing_ids = pd.read_sql("SELECT order_id FROM sales", conn)

        # Keep only new rows
        df = df[~df['order_id'].isin(existing_ids['order_id'])]

        print(f"New rows to insert: {len(df)}")

        df.to_sql("sales", conn, if_exists="append", index=False)

    else:
        print("\nTable does not exist → Creating new table")
        df.to_sql("sales", conn, if_exists="replace", index=False)

    # -------------------- VERIFY --------------------
    print("\nDatabase Preview:")
    preview = pd.read_sql("SELECT * FROM sales LIMIT 5", conn)
    print(preview)

    # -------------------- QUERY --------------------
    query = """
    SELECT year, SUM(amount) as total_sales
    FROM sales
    GROUP BY year
    """

    result = pd.read_sql(query, conn)

    print("\nTotal Sales by Year:")
    print(result)

    # -------------------- CLOSE --------------------
    conn.close()

    print("\n----- ETL Process Completed Successfully -----")

except Exception as e:
    print("\n❌ ETL FAILED")
    print(f"Error: {e}")