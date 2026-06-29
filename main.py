from pathlib import Path
import csv
import pandas as pd
import re
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import JSON

from entity_analyze import breakdown_entity_name, graph_cluster


WORKING_DIR = Path(__file__).parent

engine = create_engine("postgresql+psycopg://mike@edw:5432/ipds")


def clean_parcel_id(parcel_id: str):
    # Remove any empty strings or nans
    if (not parcel_id) or pd.isna(parcel_id):
        return None

    # Remove any parcel number that has been corrupted by excel
    if re.match(r"^\d\.\d+E\+\d+", parcel_id):
        return None

    parcel_id = str(parcel_id).replace("/", "")

    if (
        ("-" not in parcel_id)
        and ("." not in parcel_id)
        and len(parcel_id) < 10
    ):
        parcel_id = parcel_id + "."

    return parcel_id


def main():

    # Each record type has a different schema so create a dictionary that maps
    # the row 'type' to the correct column names.
    record_cols = {}
    with open(WORKING_DIR / "conf" / "rownames.csv") as f:
        reader = csv.reader(f)

        for row in reader:
            if row:
                record_cols[row[0]] = row[1:]

    # Loop through the sources in the 'datainventory.csv' sheet and load each
    # Each source could be a database table or a csv so handle each approprately
    mode = "replace"
    inventory = pd.read_csv(WORKING_DIR / "conf" / "datainventory.csv")
    for _, source in inventory.iterrows():
        print(f"Processing {source['source']}")
        if source["is_file"]:  # type: ignore
            frame = pd.read_csv(
                source["source"],  # type: ignore Typing nightmare
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
                usecols=range(
                    11
                ),  # There are extra columns on a couple hundred rows
            )
        else:
            with engine.connect() as db:
                frame = pd.read_sql_table(source["source"], db, schema="raw")  # type: ignore
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

            # Find the columns that correspond to the group code
            frame.columns = record_cols[name]

            # The column reference loaded above notes which columns are empty so
            # we can use that to lose the empty cols.
            frame = frame.drop(
                columns=[
                    col for col in frame.columns if col.startswith("empty")
                ]
            )

            # Main switchboard that directs each group
            if name.startswith("D"):

                # These are the main document rows they need to have the date
                frame["date_received"] = pd.to_datetime(
                    frame["date_received"], format="%m/%d/%Y", errors="coerce"
                ).dt.date.fillna(
                    pd.to_datetime(
                        frame["date_received"],
                        format="%Y-%m-%d",
                        errors="coerce",
                    ).dt.date
                )

                frame["document_date"] = pd.to_datetime(
                    frame["document_date"], format="%m/%d/%Y", errors="coerce"
                ).dt.date.fillna(
                    pd.to_datetime(
                        frame["document_date"],
                        format="%Y-%m-%d",
                        errors="coerce",
                    ).dt.date
                )

                frame.to_sql(
                    table_names[name],  # type: ignore
                    engine,
                    schema="rod",
                    index=False,
                    if_exists=mode,
                )

            if name.startswith("N"):
                # These are the party rows -- they have information about the
                # grantors and grantees
                frame = frame.drop(
                    columns=["document_type", "date_received", "record_type"]
                )

                frame = frame.reset_index().rename(columns={"index": "id"})
                frame.to_sql(
                    table_names[name],  # type: ignore
                    engine,
                    schema="rod",
                    index=False,
                    if_exists=mode,
                )

            if name.startswith("L"):
                frame = frame.drop(
                    columns=["document_type", "date_received", "record_type"]
                )
                if len(name) > 1:  # type: ignore
                    # For the L extras we make
                    key_col = "instrument_no"
                    data_cols = [col for col in frame.columns if col != key_col]

                    # These have a format like 'L(platted)' and we're trying to
                    # get to the 'platted' part
                    _, t = name.split("(")  # type: ignore

                    frame["type"] = t[:-1]  # Skip closing brace
                    frame["data"] = (
                        frame[data_cols]
                        .fillna(value=np.nan)
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

                    frame["parcel_id"] = frame["parcel_id"].apply(
                        clean_parcel_id
                    )
                    frame.to_sql(
                        table_names[name],  # type: ignore
                        engine,
                        schema="rod",
                        index=False,
                        if_exists=mode,
                    )

        mode = "append"

    # Run hard deduplication and naive_name_dedupe queries
    qs = [
        "sql/0001_deduplication_queries.sql",
        "sql/0003_create_name_links.sql",
    ]

    with engine.begin() as db:  # begin auto-commits
        for q in qs:
            db.execute(text(Path(q).read_text()))

    # Parse name for easier advanced deduplication
    q = "sql/0004_to_name_parse.sql"
    print("Loading dataset from EDW")
    frame = pd.read_sql(text(query_path.read_text()), engine)

    print("Breaking down example field")
    frame[
        [
            "example",
            "preamble",
            "leading_digits",
            "core",
            "suffixes",
            "designations",
        ]
    ] = breakdown_entity_name(frame)

    print("Returning break down to EDW")
    frame[
        [
            "naive_name_dedupe",
            "example",
            "preamble",
            "leading_digits",
            "core",
            "suffixes",
            "designations",
        ]
    ].to_sql(
        "parsed_party_name",
        engine,
        schema="rod",
        if_exists="replace",
        index=False,
    )

    # Perform advanced deduplication (with clustering)
    pairs_query = """
    SELECT 
        a.naive_name_dedupe AS a_id, 
        b.naive_name_dedupe AS b_id
    FROM rod.parsed_party_name a
    JOIN rod.parsed_party_name b
        ON (
            a.leading_digits = b.leading_digits
            AND a.naive_name_dedupe > b.naive_name_dedupe
            AND a.core = b.core
        ) OR (
            a.leading_digits IS NULL
            AND b.leading_digits IS NULL
            AND a.naive_name_dedupe > b.naive_name_dedupe
            AND a.core = b.core
        );
    """

    frame = pd.read_sql(pairs_query, engine)

    clusters = graph_cluster(frame)
    clusters.to_sql(
        "party_name_clusters",
        db,
        schema="rod",
        index=False,
        if_exists="replace",
    )

    # Finally add the doc type documentation
    frame = (
        pd.read_excel(WORKING_DIR / "conf" / "document_type_reference.xlsx")
        .rename(columns={"ID": "document_type", "Description": "description"})
        .drop(0)
    )

    frame.to_sql(
        "document_types", engine, schema="rod", if_exists="replace", index=False
    )


if __name__ == "__main__":
    main()
