from pathlib import Path
import csv
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import JSON

WORKING_DIR = Path(__file__).parent

engine = create_engine("postgresql+psycopg://mike@edw:5432/ipds")


def main():
    # Each record type has a different schema
    record_cols = {}
    with open(WORKING_DIR / "conf" / "rownames.csv") as f:
        reader = csv.reader(f)

        for row in reader:
            if row:
                record_cols[row[0]] = row[1:]

    mode = "replace"
    inventory = pd.read_csv(WORKING_DIR / "conf" / "datainventory.csv")
    for _, source in inventory.iterrows():
        print(f"Processing {source["source"]}")
        if source["is_file"]:
            frame = pd.read_csv(  
                source["source"],
                names=[
                    "field_1",
                    "field_2",
                    "field_3",
                    "field_4",
                    "field_5",
                    "field_6",
                    "field_7",
                    "field_8",
                    "field_9",
                    "field_10",
                    "field_11",
                ],  # see PDF docs in the vault
                usecols=range(11),  # There are extra columns on a couple hundred rows
            )
        else:
            query = text(f"""
            SELECT *
            FROM {source["source"]};
            """)

            with engine.connect() as db:
                frame = pd.read_sql(query, db)
                frame.columns = [
                        "field_1",
                        "field_2",
                        "field_3",
                        "field_4",
                        "field_5",
                        "field_6",
                        "field_7",
                        "field_8",
                        "field_9",
                        "field_10",
                        "field_11",
                    ]

        table_names = {
            "D": "documents",
            "N": "parties",
            "L": "properties",
        }

        record_groups = frame.groupby("field_4")

        for name, group in record_groups:
            frame = group.copy()
            frame.columns = record_cols[name]

            frame = frame.drop(
                columns=[col for col in frame.columns if col.startswith("empty")]
            )

            if not name.startswith("D"):  # type: ignore
                frame = frame.drop(
                    columns=["document_type", "date_received", "record_type"]
                )
            else:
                frame["date_received"] = pd.to_datetime(
                    frame["date_received"], format="%m/%d/%Y", errors="coerce"
                ).dt.date

                frame["document_date"] = pd.to_datetime(
                    frame["document_date"], format="%m/%d/%Y", errors="coerce"
                ).dt.date

            if name.startswith("L") and len(name) > 1:  # type: ignore
                # For the L extras we make
                key_col = "instrument_no"
                data_cols = [col for col in frame.columns if col != key_col]
                _, t = name.split("(")  # type: ignore

                frame["type"] = t[:-1]  # Skip closing brace
                frame["data"] = (
                    frame[data_cols]
                    .fillna(np.nan)
                    .replace(np.nan, None)
                    .apply(lambda row: row[data_cols].to_dict(), axis=1)
                )
                frame = frame.drop(columns=data_cols)

                frame.to_sql(
                    "property_details",
                    engine,
                    schema="rod",
                    dtype={"data": JSON},
                    index=False,
                    if_exists=mode,
                )
            else:
                frame.to_sql(
                    table_names[name],  # type: ignore
                    engine,
                    schema="rod",
                    index=False,
                    if_exists=mode,
                )

        mode = "append"
    

    # Finally add the doc type documentation
    frame = (
        pd.read_excel(WORKING_DIR / "conf" / "document_type_reference.xlsx")
        .rename(columns={"ID": "document_type", "Description": "description"})
        .drop(0)
    )

    with engine.connect() as db:
        frame.to_sql("document_types", db, schema="rod", if_exists="replace", index=False)

if __name__ == "__main__":
    main()
