import os
from pathlib import Path
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.types import JSON
from dotenv import load_dotenv

from entity_analyze import breakdown_entity_name, graph_cluster

from transforms import (
    FIELD_COLUMNS,
    align_v2,
    check_v2_header,
    clean_parcel_id,
    detect_encoding,
    iter_record_groups,
    load_record_cols,
    normalize_v2,
    read_v2,
    source_layout,
    validate_v2,
)


WORKING_DIR = Path(__file__).parent

engine = create_engine("postgresql+psycopg://mike@edw:5432/ipds")


def main():
    load_dotenv()
    vault_location = Path(os.getenv("VAULT_LOCATION", "mnt/v"))

    # Each record type has a different schema so create a dictionary that maps
    # the row 'type' to the correct column names.
    record_cols = load_record_cols(WORKING_DIR / "conf" / "rownames.csv")

    # Every run is a full reload, so the first write to each table replaces it
    # and the rest append. This is tracked per table rather than per source
    # because one source can write property_details several times -- once per
    # legal subtype -- and each of those must not drop the previous one.
    table_modes: dict[str, str] = {}

    def write_table(frame, table, **kwargs):
        frame.to_sql(
            table,
            engine,
            schema="rod",
            index=False,
            if_exists=table_modes.get(table, "replace"),
            **kwargs,
        )
        table_modes[table] = "append"

    # parties.id is a row counter within a source, so offset it to stay unique
    # across the whole load.
    party_id_offset = 0

    # Loop through the sources in the 'datainventory.csv' sheet and load each
    # Each source could be a database table or a csv so handle each approprately
    inventory = pd.read_csv(WORKING_DIR / "conf" / "datainventory.csv")
    for _, source in inventory.iterrows():
        layout = source_layout(source)
        print(f"Processing {source['source']} (layout={layout})")

        if source["is_file"]:  # type: ignore
            path = vault_location / source["source"]  # type: ignore

            # These exports are not reliably UTF-8, and a single stray byte
            # aborts the parse thousands of rows in.
            encoding = detect_encoding(path)
            if encoding != "utf-8":
                print(f"  encoding -- not utf-8, decoding as {encoding}")

            if layout == "v2":
                # v2 files carry a header, so they are read by name -- no
                # positional names, and the column count can change.
                for problem in check_v2_header(path, encoding=encoding):
                    print(f"  header -- {problem}")

                frame = read_v2(path, encoding=encoding)
            else:
                frame = pd.read_csv(
                    path,
                    names=FIELD_COLUMNS,  # see PDF docs in the vault
                    usecols=range(
                        len(FIELD_COLUMNS)
                    ),  # There are extra columns on a couple hundred rows
                    encoding=encoding,
                )
        else:
            with engine.connect() as db:
                frame = pd.read_sql_table(source["source"], db, schema="raw")  # type: ignore

                if layout == "v2":
                    frame = align_v2(frame)
                else:
                    frame.columns = FIELD_COLUMNS

        if layout == "v2":
            # v2 carries no record type column, so reshape it into the same
            # positional intermediate the switchboard below already expects.
            for key, value in validate_v2(frame).items():
                print(f"  {key}: {value}")

            frame = normalize_v2(frame)

        table_names = {
            "D": "documents",
            "N": "parties",
            "L": "properties",
        }

        source_rows = len(frame)

        for name, group in iter_record_groups(frame, record_cols):
            frame = group

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

                write_table(frame, table_names[name])  # type: ignore

            if name.startswith("N"):
                # These are the party rows -- they have information about the
                # grantors and grantees
                frame = frame.drop(
                    columns=["document_type", "date_received", "record_type"]
                )

                frame = frame.reset_index().rename(columns={"index": "id"})
                frame["id"] += party_id_offset
                write_table(frame, table_names[name])  # type: ignore

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

                    write_table(
                        frame, "property_details", dtype={"data": JSON}
                    )
                else:

                    frame["parcel_id"] = frame["parcel_id"].apply(
                        clean_parcel_id
                    )
                    write_table(frame, table_names[name])  # type: ignore

        party_id_offset += source_rows

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
