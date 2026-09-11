"""Source-layout handling for the ROD loader.

Two source layouts exist:

``v1``
    The original headerless 11-column export. Column 4 carries a record-type
    code (``D``, ``N``, ``L``, ``L(platted)``, ...) and ``conf/rownames.csv``
    maps that code to the column names for the rest of the row.

``v2``
    The newer 19-column export, which *has* a header row and *no* record-type
    column. The document-level fields are repeated on every row and the record
    type is implicit in which columns are populated: ``PARTY_NAME`` means a
    grantor, ``PARTY_NAME2`` a grantee, ``TAX_ID1``/``ADDRESS`` a property.

``normalize_v2`` reshapes a v2 frame into the v1 positional intermediate so the
switchboard in ``main.py`` -- and everything in ``sql/`` that depends on the
resulting column names -- stays unchanged.

This module deliberately imports nothing but pandas/numpy so it can be tested
without a database, without the ``entity_analyze`` sibling package, and without
the source files in the vault.
"""

from pathlib import Path
import codecs
import csv
import re

import pandas as pd

# The v1 positional intermediate every source is reshaped into.
FIELD_COLUMNS = [f"field_{i}" for i in range(1, 12)]

# The v2 header, in order. Read positionally, like v1, but verified against the
# file's own header row by check_v2_header.
V2_COLUMNS = [
    "DOC_TYPE_CODE",
    "RECORDED_DATE_TIME",
    "CF_VCINSTNUM",
    "LIBER",
    "CONSIDERATION",
    "DM_INSTRUMENT",
    "PARTY_NAME",
    "PARTY_NAME2",
    "TAX_ID1",
    "ADDRESS",
    "MUNICIPALITY",
    "Textbox18",
    "PLAT_LIBER",
    "Textbox20",
    "PLAT_PAGE",
    "Textbox22",
    "LOT",
    "Textbox59",
    "Textbox61",
]

# Document-level fields, repeated on every v2 row. Documents are the distinct
# tuples over these -- there is no dedicated document row in v2.
V2_DOC_COLUMNS = V2_COLUMNS[:6]

# Read everything as text, as the headerless v1 read effectively does. This
# matters for TAX_ID1: if a chunk of it were all scientific notation pandas
# would infer float64 and clean_parcel_id's re.match would raise rather than
# return None. CF_VCINSTNUM is left to infer so instrument_no keeps the dtype
# v1 produces.
V2_DTYPES = {col: str for col in V2_COLUMNS if col != "CF_VCINSTNUM"}

# Tried in order against a source file. latin-1 maps every possible byte, so
# it is the backstop that always succeeds; cp1252 sits ahead of it because
# these reports come off Windows and it decodes the printable range correctly.
SOURCE_ENCODINGS = ("utf-8", "cp1252", "latin-1")

# The Textbox columns are report-generator labels, so a file without them is
# still perfectly loadable. Everything else has to be there.
V2_REQUIRED_COLUMNS = [
    col for col in V2_COLUMNS if not col.startswith("Textbox")
]

# The report generator emits its own field labels as data. They carry no
# information, but a label landing under the wrong name shows up here first.
V2_LABELS = {
    "Textbox18": "Plat Liber:",
    "Textbox20": "Plat Page:",
    "Textbox22": "Lot:",
}

# A leading street number: 112, 1234A, 1234-1/2. Anything that does not match
# leaves street_no empty rather than putting a word like 'PO' in it, which
# would corrupt the properties dedupe key in sql/0001.
ADDRESS_PATTERN = (
    r"^\s*(?P<street_no>\d+(?:[-/]\d+)*[A-Za-z]?)\s+(?P<street_name>.*?)\s*$"
)


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


def load_record_cols(path: Path) -> dict[str, list[str]]:
    """Read conf/rownames.csv into {record code: [column names]}."""
    record_cols = {}
    with open(path) as f:
        reader = csv.reader(f)

        for row in reader:
            if row:
                record_cols[row[0]] = row[1:]

    return record_cols


def iter_record_groups(frame: pd.DataFrame, record_cols: dict[str, list[str]]):
    """Split a field_1..field_11 frame by record type, naming the columns.

    Yields (code, frame) pairs with the positional fields renamed per
    conf/rownames.csv and the columns that reference notes as empty dropped.
    """
    for name, group in frame.groupby("field_4"):
        out = group.copy()

        # Find the columns that correspond to the group code
        out.columns = record_cols[name]

        # The column reference notes which columns are empty so we can use that
        # to lose the empty cols.
        out = out.drop(
            columns=[col for col in out.columns if col.startswith("empty")]
        )

        yield name, out


def source_layout(source: pd.Series) -> str:
    """The layout declared for one row of conf/datainventory.csv.

    A missing column, an empty cell, or whitespace all mean the original v1
    layout. Note that a blank cell reads as NaN, which is truthy -- so this
    cannot be written as `source.get("layout") or "v1"`.
    """
    layout = source.get("layout", "v1")

    if pd.isna(layout) or not str(layout).strip():
        return "v1"

    return str(layout).strip().lower()


def _as_text(series: pd.Series) -> pd.Series:
    """An object-dtype view, so the .str accessor is always available."""
    if series.dtype == object:
        return series

    return series.astype(object).where(series.notna())


def _strip(series: pd.Series) -> pd.Series:
    """Trim text columns; leave anything else (e.g. the instrument no) alone."""
    if series.dtype != object:
        return series

    return series.str.strip()


def _blank_to_none(series: pd.Series) -> pd.Series:
    # Via the object view: a column that is entirely absent arrives as float64,
    # and None written into a float column silently becomes NaN again.
    series = _as_text(series)

    return series.where(series.notna() & series.ne(""), None)


def _filled(series: pd.Series) -> pd.Series:
    """True where the cell holds something other than whitespace."""
    if series.dtype != object:
        return series.notna()

    return series.notna() & series.str.strip().ne("")


def split_liber(series: pd.Series) -> pd.DataFrame:
    """'58620:573' -> liber '58620', page '573'. No colon leaves page empty."""
    parts = (
        _as_text(series)
        .str.split(":", n=1, expand=True)
        .reindex(columns=[0, 1])
        .set_axis(["liber", "page"], axis=1)
    )

    return parts.apply(lambda col: _blank_to_none(_strip(col)))


def split_address(series: pd.Series) -> pd.DataFrame:
    """'112 EDISON ' -> street_no '112', street_name 'EDISON'.

    An address with no leading street number keeps its whole text as the street
    name rather than losing the first word to street_no.
    """
    text = _as_text(series)
    out = text.str.extract(ADDRESS_PATTERN)

    unmatched = out["street_name"].isna() & text.notna()
    out.loc[unmatched, "street_name"] = text[unmatched].str.strip()

    return out.apply(_blank_to_none)


def _base(frame: pd.DataFrame, code: str) -> pd.DataFrame:
    """The doc-level slots plus the record code, aligned to frame's index."""
    return pd.DataFrame(
        {
            "field_1": _strip(frame["DOC_TYPE_CODE"]),
            "field_2": _strip(frame["RECORDED_DATE_TIME"]),
            "field_3": frame["CF_VCINSTNUM"],
            "field_4": code,
        },
        index=frame.index,
    )


def v2_documents(frame: pd.DataFrame) -> pd.DataFrame:
    """One D row per distinct document-level tuple.

    v2 has no document record, so the documents are the distinct values of the
    six leading columns that every row repeats.
    """
    doc = frame[V2_DOC_COLUMNS].apply(_strip).drop_duplicates()

    out = _base(doc, "D")
    liber = split_liber(doc["LIBER"])
    out["field_5"] = liber["liber"]
    out["field_6"] = liber["page"]
    # Consideration stays a verbatim string ('33,500.00'): sql/0001 dedupes on
    # the literal value, so parsing it here would stop v2 rows collapsing
    # against the v1 rows for the same document.
    out["field_10"] = _blank_to_none(doc["CONSIDERATION"])
    # DM_INSTRUMENT is the document date, despite the name.
    out["field_11"] = _blank_to_none(doc["DM_INSTRUMENT"])

    return out.reindex(columns=FIELD_COLUMNS)


def v2_parties(frame: pd.DataFrame) -> pd.DataFrame:
    """One N row per populated party column. R is a grantor, E a grantee."""
    groups = []

    for column, role in (("PARTY_NAME", "R"), ("PARTY_NAME2", "E")):
        rows = frame[_filled(frame[column])]

        out = _base(rows, "N")
        out["field_5"] = role
        out["field_6"] = rows[column].str.strip()

        groups.append(out.reindex(columns=FIELD_COLUMNS))

    return pd.concat(groups).drop_duplicates()


def v2_properties(frame: pd.DataFrame) -> pd.DataFrame:
    """One L row per populated parcel/address. Multi-lot rows collapse here."""
    rows = frame[_filled(frame["TAX_ID1"]) | _filled(frame["ADDRESS"])]

    out = _base(rows, "L")
    # clean_parcel_id runs in main's L branch, as it does for v1.
    out["field_5"] = _blank_to_none(_strip(rows["TAX_ID1"]))
    address = split_address(rows["ADDRESS"])
    out["field_6"] = address["street_no"]
    out["field_7"] = address["street_name"]
    out["field_8"] = _blank_to_none(_strip(rows["MUNICIPALITY"]))

    return out.reindex(columns=FIELD_COLUMNS).drop_duplicates()


def v2_platted(frame: pd.DataFrame) -> pd.DataFrame:
    """One L(platted) row per lot.

    v2 carries no lot_to or subdivision, but both slots are emitted so the JSON
    in rod.property_details has the same shape it does for v1.
    """
    rows = frame[
        _filled(frame["LOT"])
        | _filled(frame["PLAT_LIBER"])
        | _filled(frame["PLAT_PAGE"])
    ]

    out = _base(rows, "L(platted)")
    out["field_5"] = _blank_to_none(_strip(rows["LOT"]))  # lot
    out["field_6"] = None  # lot_to -- absent in v2
    out["field_7"] = _blank_to_none(_strip(rows["PLAT_LIBER"]))
    out["field_8"] = _blank_to_none(_strip(rows["PLAT_PAGE"]))
    out["field_9"] = None  # subdivision -- absent in v2

    return out.reindex(columns=FIELD_COLUMNS).drop_duplicates()


def normalize_v2(frame: pd.DataFrame) -> pd.DataFrame:
    """Reshape one v2 source into the v1 field_1..field_11 intermediate."""
    frame = frame.reset_index(drop=True)

    return pd.concat(
        [
            v2_documents(frame),
            v2_parties(frame),
            v2_properties(frame),
            v2_platted(frame),
        ],
        ignore_index=True,
    )


def validate_v2(frame: pd.DataFrame) -> dict[str, int]:
    """Non-fatal counts worth reading in the log before a v2 load."""
    grantor = _filled(frame["PARTY_NAME"])
    grantee = _filled(frame["PARTY_NAME2"])
    property_row = _filled(frame["TAX_ID1"]) | _filled(frame["ADDRESS"])

    doc_fields = frame[V2_DOC_COLUMNS].apply(_strip)
    conflicts = (
        doc_fields.groupby(frame["CF_VCINSTNUM"])
        .nunique()
        .gt(1)
        .any(axis=1)
        .sum()
    )

    unexpected_labels = 0
    for column, label in V2_LABELS.items():
        values = _strip(_as_text(frame[column]))
        unexpected_labels += int((values.notna() & values.ne(label)).sum())

    parcels = _as_text(frame["TAX_ID1"])
    dropped = parcels[parcels.notna()].map(
        lambda value: clean_parcel_id(value) is None
    )

    addresses = split_address(frame["ADDRESS"])

    return {
        "rows": len(frame),
        "rows_unclassified": int((~grantor & ~grantee & ~property_row).sum()),
        "rows_both_party_cols": int((grantor & grantee).sum()),
        "instruments_with_conflicting_doc_fields": int(conflicts),
        "addresses_without_street_no": int(
            (addresses["street_name"].notna() & addresses["street_no"].isna())
            .sum()
        ),
        "unexpected_labels": unexpected_labels,
        "parcel_ids_dropped": int(dropped.sum()),
    }


def align_v2(frame: pd.DataFrame) -> pd.DataFrame:
    """Select the v2 columns by name, in the order the transform expects.

    v2 files carry a header, so they are read by name rather than by position.
    That means a source can gain, lose or reorder columns without breaking the
    load: extra columns are ignored and the optional label columns are filled
    in when absent. A missing data column is an error, because silently
    treating it as empty would quietly drop records.
    """
    frame = frame.rename(columns=lambda col: str(col).strip())

    missing = [col for col in V2_REQUIRED_COLUMNS if col not in frame.columns]
    if missing:
        raise ValueError(
            f"v2 source is missing {len(missing)} required column(s): "
            f"{', '.join(missing)}. Found: {', '.join(map(str, frame.columns))}"
        )

    return frame.reindex(columns=V2_COLUMNS)


def check_v2_header(path: Path, encoding: str | None = None) -> list[str]:
    """Differences between a v2 file's header and the expected layout.

    Everything reported here is survivable -- align_v2 raises on the ones that
    are not -- but they are worth seeing in the log.
    """
    encoding = encoding or detect_encoding(path)
    header = [column.strip() for column in _read_header(path, encoding)]

    problems = []

    for column in V2_COLUMNS:
        if column not in header:
            problems.append(f"missing column {column!r}")

    for column in header:
        if column not in V2_COLUMNS:
            problems.append(f"unexpected column {column!r} (ignored)")

    present = [column for column in header if column in V2_COLUMNS]
    if present != [column for column in V2_COLUMNS if column in header]:
        problems.append("columns are in a different order (read by name)")

    return problems


def detect_encoding(path, chunk_size: int = 1 << 20) -> str:
    """The first encoding in SOURCE_ENCODINGS that decodes the whole file.

    These exports are generated on Windows and are not always UTF-8 -- a single
    stray byte (an accented character in a name, say) aborts the parse
    thousands of rows in. Decoding incrementally keeps this cheap: UTF-8 fails
    at the first bad byte, and the file is never held in memory twice.
    """
    for encoding in SOURCE_ENCODINGS:
        decoder = codecs.getincrementaldecoder(encoding)()

        try:
            with open(path, "rb") as f:
                while chunk := f.read(chunk_size):
                    decoder.decode(chunk)

                decoder.decode(b"", final=True)

            return encoding
        except UnicodeDecodeError:
            continue

    # latin-1 maps every byte, so this is only reachable if it is dropped.
    return SOURCE_ENCODINGS[-1]


def _read_header(path, encoding: str) -> list[str]:
    with open(path, newline="", encoding=encoding) as f:
        return next(csv.reader(f), [])


def read_v2(path, encoding: str | None = None) -> pd.DataFrame:
    """Read a v2 CSV, selecting columns by the file's own header.

    Nothing is passed positionally: the file decides how many columns it has
    and in what order. Naming the columns in usecols also absorbs the stray
    extra fields these exports carry on a handful of rows, which would
    otherwise abort the parse.
    """
    encoding = encoding or detect_encoding(path)
    header = _read_header(path, encoding)

    missing = [
        col
        for col in V2_REQUIRED_COLUMNS
        if col not in {name.strip() for name in header}
    ]
    if missing:
        raise ValueError(
            f"{path} is not a v2 source -- missing {len(missing)} required "
            f"column(s): {', '.join(missing)}. "
            f"Its header is: {', '.join(header)}"
        )

    # Match on the raw names so a padded header still selects, then let
    # align_v2 trim the names and order them.
    wanted = [name for name in header if name.strip() in V2_COLUMNS]

    return align_v2(
        pd.read_csv(
            path,
            header=0,
            usecols=wanted,
            dtype=V2_DTYPES,
            encoding=encoding,
        )
    )


if __name__ == "__main__":
    # Dry run against a v2 file, no database:
    #     python transforms.py rod_new_schema_example.csv
    import sys

    source = Path(sys.argv[1])

    encoding = detect_encoding(source)
    if encoding != "utf-8":
        print(f"Not utf-8, decoding as {encoding}")

    problems = check_v2_header(source, encoding=encoding)
    if problems:
        print("Header does not match the expected v2 layout:")
        for problem in problems:
            print(f"  {problem}")

    frame = read_v2(source, encoding=encoding)

    for key, value in validate_v2(frame).items():
        print(f"{key}: {value}")

    record_cols = load_record_cols(
        Path(__file__).parent / "conf" / "rownames.csv"
    )

    for name, group in iter_record_groups(normalize_v2(frame), record_cols):
        print(f"\n=== {name} ({len(group)} rows) ===")
        print(group.head(10).to_string(index=False))
