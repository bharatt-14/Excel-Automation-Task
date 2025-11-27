import os
import pandas as pd
import psycopg2
from psycopg2 import sql
import configparser


CONFIG_PATH = r"C:\Users\Naval Singh\OneDrive\Desktop\SS Internship\Excel Automation\config.ini"

config = configparser.ConfigParser()
config.read(CONFIG_PATH, encoding="utf-8")

print("Loaded Config Sections:", config.sections())
if "EXCEL" not in config or "DATABASE" not in config:
    raise Exception("Config not loaded! Check config.ini formatting.")


EXCEL_FOLDER = config["EXCEL"]["FOLDER"]

CONN_INFO = {
    "host": config["DATABASE"]["HOST"],
    "dbname": config["DATABASE"]["DBNAME"],
    "user": config["DATABASE"]["USER"],
    "password": config["DATABASE"]["PASSWORD"],
}


def find_excel_in_folder(folder):
    for f in os.listdir(folder):
        if f.lower().endswith((".xlsx", ".xls")):
            return os.path.join(folder, f)
    return None


def create_table(cur, table_name, df):
    cols = [sql.Identifier(c) for c in df.columns]
    defs = [sql.SQL("{} TEXT").format(c) for c in cols]

    q = sql.SQL(
        "CREATE TABLE IF NOT EXISTS {} ({});"
    ).format(sql.Identifier(table_name), sql.SQL(", ").join(defs))

    cur.execute(q)


def insert_rows(cur, table_name, df):
    for _, row in df.iterrows():
        cols = [sql.Identifier(c) for c in df.columns]
        vals = [sql.Literal(str(v)) for v in row]

        q = sql.SQL(
            "INSERT INTO {} ({}) VALUES ({});"
        ).format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(cols),
            sql.SQL(', ').join(vals)
        )
        cur.execute(q)


def detect_tables(df):
    table_starts = []

    for i in range(len(df)):
        first_cell = df.iloc[i, 0]
        if isinstance(first_cell, str) and first_cell.strip().lower() == "date":
            table_starts.append(i)

    tables = []

    for idx in range(len(table_starts)):
        start = table_starts[idx]
        end = table_starts[idx + 1] if idx + 1 < len(table_starts) else len(df)

        part = df.iloc[start:end].copy()

        part.columns = part.iloc[0].astype(str)
        part = part.iloc[1:].reset_index(drop=True)

        part.columns = [
            str(c).strip().replace(" ", "_").lower()
            for c in part.columns
        ]

        tables.append(part)

    return tables


def main():

    excel_file = find_excel_in_folder(EXCEL_FOLDER)

    if excel_file is None:
        print("No Excel found.")
        return

    print("Excel found:", excel_file)

    df = pd.read_excel(excel_file, header=None)
    tables = detect_tables(df)

    conn = psycopg2.connect(**CONN_INFO)
    cur = conn.cursor()

    for i, t in enumerate(tables, start=1):
        name = f"table{i}"
        create_table(cur, name, t)
        insert_rows(cur, name, t)

    conn.commit()
    cur.close()
    conn.close()

    print("Data inserted successfully!")


if __name__ == "__main__":
    main()
