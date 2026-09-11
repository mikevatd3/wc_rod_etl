"""Offline checks for the v2 source layout.

These run against the committed sample export with nothing but pandas -- no
database, no entity_analyze, no files from the vault.
"""

from pathlib import Path

import pandas as pd
import pytest

from transforms import (
    V2_COLUMNS,
    align_v2,
    check_v2_header,
    detect_encoding,
    clean_parcel_id,
    iter_record_groups,
    load_record_cols,
    normalize_v2,
    read_v2,
    source_layout,
    split_address,
    split_liber,
    v2_documents,
    v2_parties,
    v2_platted,
    v2_properties,
    validate_v2,
)

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "rod_new_schema_example.csv"

# What each record type looks like once iter_record_groups has renamed the
# positional fields. Everything in sql/ is keyed on these names.
EXPECTED_COLUMNS = {
    "D": [
        "document_type",
        "date_received",
        "instrument_no",
        "record_type",
        "liber",
        "page",
        "consideration",
        "document_date",
    ],
    "N": [
        "document_type",
        "date_received",
        "instrument_no",
        "record_type",
        "grantor_grantee",
        "combined_name",
    ],
    "L": [
        "document_type",
        "date_received",
        "instrument_no",
        "record_type",
        "parcel_id",
        "street_no",
        "street_name",
        "municipality",
    ],
    "L(platted)": [
        "document_type",
        "date_received",
        "instrument_no",
        "record_type",
        "lot",
        "lot_to",
        "plat_liber",
        "plat_page",
        "subdivision",
    ],
}


@pytest.fixture(scope="module")
def sample() -> pd.DataFrame:
    return read_v2(SAMPLE)


def test_header_matches_the_expected_layout():
    assert check_v2_header(SAMPLE) == []


def test_every_row_is_exactly_one_kind_of_record(sample):
    # 8 grantors + 15 grantees + 10 property rows accounts for all 33 rows.
    # This is the strongest single check that the layout is being read right.
    counts = validate_v2(sample)

    assert counts["rows"] == 33
    assert counts["rows_unclassified"] == 0
    assert counts["rows_both_party_cols"] == 0
    assert counts["instruments_with_conflicting_doc_fields"] == 0
    assert counts["unexpected_labels"] == 0
    assert counts["addresses_without_street_no"] == 0
    assert counts["parcel_ids_dropped"] == 2  # the two HIGHLAND PARK rows


def test_documents_are_one_row_per_instrument(sample):
    documents = v2_documents(sample)

    assert len(documents) == 7
    assert set(documents["field_3"]) == {
        2024005182,
        2024004098,
        2024004735,
        2024006993,
        2024004708,
        2024004159,
        2024005691,
    }
    assert set(documents["field_4"]) == {"D"}


def test_documents_split_the_composite_liber(sample):
    documents = v2_documents(sample).set_index("field_3")

    assert documents.loc[2024004708, "field_5"] == "58619"
    assert documents.loc[2024004708, "field_6"] == "1"
    assert documents.loc[2024005182, "field_5"] == "58620"
    assert documents.loc[2024005182, "field_6"] == "573"


def test_consideration_stays_a_verbatim_string(sample):
    # sql/0001 dedupes documents on the literal value, so parsing this to a
    # number here would stop v2 rows collapsing against v1 rows.
    documents = v2_documents(sample).set_index("field_3")

    assert documents.loc[2024004735, "field_10"] == "33,500.00"
    assert documents.loc[2024005182, "field_10"] == "0"


def test_document_date_comes_from_dm_instrument(sample):
    documents = v2_documents(sample).set_index("field_3")

    assert documents.loc[2024005182, "field_11"] == "2023-12-15"


def test_parties_split_by_column_into_grantor_and_grantee(sample):
    parties = v2_parties(sample)

    assert len(parties) == 23
    assert parties["field_5"].value_counts().to_dict() == {"E": 15, "R": 8}
    assert set(parties["field_4"]) == {"N"}

    grantors = parties[parties["field_5"] == "R"]
    assert "SMITH HARRY J" in set(grantors["field_6"])

    grantees = parties[parties["field_5"] == "E"]
    assert "SMITH HARRY J -SUT" in set(grantees["field_6"])


def test_properties_collapse_the_multi_lot_fan_out(sample):
    # Ten source rows carry a parcel, but a document repeats the same parcel
    # once per lot -- there are only six distinct properties.
    assert len(v2_properties(sample)) == 6


def test_platted_lots_do_not_collapse(sample):
    # MOSS 925/924, CASS 92/93/94 and MITCHELL 26/27 are genuinely different
    # lots on one document and each must survive.
    platted = v2_platted(sample)

    assert len(platted) == 10
    assert sorted(platted[platted["field_3"] == 2024006993]["field_5"]) == [
        "92",
        "93",
        "94",
    ]
    # v2 has no lot_to or subdivision, but both slots stay in the JSON shape.
    assert platted["field_6"].isna().all()
    assert platted["field_9"].isna().all()


def test_normalize_round_trips_through_the_real_switchboard(sample):
    # The one real risk of reusing the v1 intermediate is a positional slot
    # landing under the wrong name. This is what rules that out.
    record_cols = load_record_cols(ROOT / "conf" / "rownames.csv")
    groups = dict(iter_record_groups(normalize_v2(sample), record_cols))

    assert set(groups) == {"D", "N", "L", "L(platted)"}

    for code, frame in groups.items():
        assert list(frame.columns) == EXPECTED_COLUMNS[code]

    assert len(groups["D"]) == 7
    assert len(groups["N"]) == 23
    assert len(groups["L"]) == 6
    assert len(groups["L(platted)"]) == 10

    edison = groups["L"][groups["L"]["instrument_no"] == 2024005182].iloc[0]
    assert edison["parcel_id"] == "02/005147-3"  # cleaned in main's L branch
    assert edison["street_no"] == "112"
    assert edison["street_name"] == "EDISON"
    assert edison["municipality"] == "DETROIT"


@pytest.mark.parametrize(
    "value, expected",
    [
        ("58620:573", ("58620", "573")),
        ("58619:1", ("58619", "1")),
        ("58620", ("58620", None)),  # no page component
        (None, (None, None)),
    ],
)
def test_split_liber(value, expected):
    result = split_liber(pd.Series([value], dtype=object)).iloc[0]

    assert (result["liber"], result["page"]) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("112 EDISON ", ("112", "EDISON")),
        ("4465 MARYLAND ", ("4465", "MARYLAND")),
        ("19251 MITCHELL ", ("19251", "MITCHELL")),
        ("1234A GRAND RIVER", ("1234A", "GRAND RIVER")),
        ("1234-1/2 WOODWARD", ("1234-1/2", "WOODWARD")),
        # No leading number: keep the whole text rather than losing a word to
        # street_no, which would corrupt the sql/0001 properties dedupe key.
        ("PO BOX 5", (None, "PO BOX 5")),
        (None, (None, None)),
    ],
)
def test_split_address(value, expected):
    result = split_address(pd.Series([value], dtype=object)).iloc[0]

    assert (result["street_no"], result["street_name"]) == expected


@pytest.mark.parametrize(
    "value, expected",
    [
        ("02/005147-3", "02005147-3"),
        ("02/002128", "02002128."),
        ("4.3003E+13", None),  # corrupted by excel upstream
        ("13/023977-8", "13023977-8"),
        (None, None),
    ],
)
def test_clean_parcel_id(value, expected):
    assert clean_parcel_id(value) == expected


@pytest.mark.parametrize(
    "row, expected",
    [
        ({"source": "x", "is_file": 1, "layout": "v2"}, "v2"),
        ({"source": "x", "is_file": 1, "layout": "V1"}, "v1"),
        ({"source": "x", "is_file": 1, "layout": " v2 "}, "v2"),
        # A blank cell reads as NaN, which is truthy -- so it has to be tested.
        ({"source": "x", "is_file": 1, "layout": float("nan")}, "v1"),
        ({"source": "x", "is_file": 1, "layout": ""}, "v1"),
        # An un-migrated datainventory.csv with no layout column at all.
        ({"source": "x", "is_file": 1}, "v1"),
    ],
)
def test_source_layout(row, expected):
    assert source_layout(pd.Series(row)) == expected


def test_check_v2_header_reports_unknown_and_missing_columns(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("DOC_TYPE_CODE,SURPRISE,CF_VCINSTNUM\n")

    problems = check_v2_header(bad)

    assert any("unexpected column 'SURPRISE'" in problem for problem in problems)
    assert any("missing column 'PARTY_NAME'" in problem for problem in problems)


def test_align_ignores_extra_columns_and_fills_absent_labels(sample):
    # A v2 export that gained a column and dropped the label columns still
    # loads: the count and order of the columns no longer matter.
    frame = sample.drop(columns=["Textbox59", "Textbox61", "Textbox22"])
    frame["SOMETHING_NEW"] = "x"

    aligned = align_v2(frame)

    assert list(aligned.columns) == list(sample.columns)
    assert aligned["Textbox22"].isna().all()
    assert len(v2_documents(aligned)) == 7


def test_align_reads_by_name_not_position(sample):
    shuffled = sample[list(reversed(sample.columns))]

    aligned = align_v2(shuffled)

    assert list(aligned.columns) == list(sample.columns)
    assert len(v2_parties(aligned)) == 23


def test_align_tolerates_padded_header_names(sample):
    padded = sample.rename(columns=lambda col: f" {col} ")

    assert list(align_v2(padded).columns) == list(sample.columns)


def test_align_raises_on_a_missing_data_column(sample):
    # Treating a genuinely absent data column as empty would silently drop
    # records, so this has to be loud.
    with pytest.raises(ValueError, match="PARTY_NAME2"):
        align_v2(sample.drop(columns=["PARTY_NAME2"]))


def test_empty_input_does_not_blow_up():
    empty = read_v2(SAMPLE).iloc[:0]

    assert len(normalize_v2(empty)) == 0
    assert validate_v2(empty)["rows"] == 0


def test_v1_records_still_split_the_way_they_always_did():
    # iter_record_groups was lifted out of main() unchanged; this pins the
    # original v1 behaviour so the extraction stays a no-op.
    record_cols = load_record_cols(ROOT / "conf" / "rownames.csv")
    frame = pd.DataFrame(
        [
            ["DD", "01/02/2024", "1", "D", "58620", "573", "", "", "", "0", "01/01/2024"],
            ["DD", "01/02/2024", "1", "N", "R", "SMITH HARRY J", "", "", "", "", ""],
            ["DD", "01/02/2024", "1", "L", "02/005147-3", "112", "EDISON", "DETROIT", "", "", ""],
            ["DD", "01/02/2024", "1", "L(condo)", "PLAN", "1", "2", "THE CONDO", "", "", ""],
        ],
        columns=[f"field_{i}" for i in range(1, 12)],
    )

    groups = dict(iter_record_groups(frame, record_cols))

    assert set(groups) == {"D", "N", "L", "L(condo)"}
    assert list(groups["D"].columns) == EXPECTED_COLUMNS["D"]
    assert list(groups["N"].columns) == EXPECTED_COLUMNS["N"]
    assert list(groups["L"].columns) == EXPECTED_COLUMNS["L"]
    assert list(groups["L(condo)"].columns) == [
        "document_type",
        "date_received",
        "instrument_no",
        "record_type",
        "plan",
        "unit",
        "unit_to",
        "condominium",
    ]



def _write_v2(path, header, rows):
    path.write_text(
        "\n".join([",".join(header)] + [",".join(row) for row in rows]) + "\n"
    )
    return path


def test_read_v2_survives_a_different_column_count(tmp_path):
    # The real exports do not all have the sample's 19 columns. Reading by name
    # means gaining a column, losing the label columns, and reordering are all
    # non-events.
    header = [c for c in V2_COLUMNS if not c.startswith("Textbox")]
    header = list(reversed(header)) + ["GRANTOR_ADDRESS"]
    row = {
        "DOC_TYPE_CODE": "DD",
        "RECORDED_DATE_TIME": "2024-01-02",
        "CF_VCINSTNUM": "2024005182",
        "LIBER": "58620:573",
        "CONSIDERATION": "0",
        "DM_INSTRUMENT": "2023-12-15",
        "PARTY_NAME": "SMITH HARRY J",
        "PARTY_NAME2": "",
        "TAX_ID1": "",
        "ADDRESS": "",
        "MUNICIPALITY": "",
        "PLAT_LIBER": "",
        "PLAT_PAGE": "",
        "LOT": "",
        "GRANTOR_ADDRESS": "somewhere",
    }

    frame = read_v2(_write_v2(tmp_path / "v2.csv", header, [[row[c] for c in header]]))

    assert list(frame.columns) == V2_COLUMNS
    assert len(v2_documents(frame)) == 1
    assert v2_parties(frame).iloc[0]["field_6"] == "SMITH HARRY J"


def test_read_v2_absorbs_stray_extra_fields(tmp_path):
    # "There are extra columns on a couple hundred rows" -- the v1 reader used
    # usecols for this; naming the columns does the same job without dropping
    # the row.
    header = list(V2_COLUMNS)
    good = ["DD", "2024-01-02", "2024005182", "58620:573", "0", "2023-12-15"] + [""] * 13
    ragged = list(good) + ["junk", "more junk"]

    frame = read_v2(_write_v2(tmp_path / "ragged.csv", header, [good, ragged]))

    assert list(frame.columns) == V2_COLUMNS
    assert len(frame) == 2


def test_read_v2_names_the_missing_columns_and_the_real_header(tmp_path):
    bad = _write_v2(tmp_path / "bad.csv", ["DOC_TYPE_CODE", "WHO_KNOWS"], [["DD", "x"]])

    with pytest.raises(ValueError) as error:
        read_v2(bad)

    assert "CF_VCINSTNUM" in str(error.value)
    assert "WHO_KNOWS" in str(error.value)  # shows what the file actually has



def test_detect_encoding_finds_utf8(tmp_path):
    path = tmp_path / "utf8.csv"
    path.write_text("A,B\nMÜLLER,x\n", encoding="utf-8")

    assert detect_encoding(path) == "utf-8"


def test_detect_encoding_falls_back_for_a_windows_export(tmp_path):
    path = tmp_path / "cp1252.csv"
    path.write_bytes("A,B\nM\u00dcLLER,x\n".encode("cp1252"))

    assert detect_encoding(path) == "cp1252"


def test_read_v2_handles_a_non_utf8_export(tmp_path):
    # The 0xDC byte that aborted a real load: cp1252 'U with umlaut', thousands
    # of rows into an otherwise plain-ascii file.
    header = list(V2_COLUMNS)
    rows = [
        ["DD", "2024-01-02", "2024005182", "58620:573", "0", "2023-12-15"]
        + [name, "", "", "", "", "", "", "", "", "", "", "", ""]
        for name in ("SMITH HARRY J", "M\u00dcLLER HANS")
    ]
    path = tmp_path / "latin.csv"
    path.write_bytes(
        ("\n".join([",".join(header)] + [",".join(r) for r in rows]) + "\n").encode(
            "cp1252"
        )
    )

    frame = read_v2(path)

    assert set(v2_parties(frame)["field_6"]) == {"SMITH HARRY J", "M\u00dcLLER HANS"}


def test_detect_encoding_reads_across_chunk_boundaries(tmp_path):
    # A multi-byte character split across two reads must not be misreported.
    path = tmp_path / "big.csv"
    path.write_text("A\n" + ("x" * 5000) + "\u00e9\n", encoding="utf-8")

    assert detect_encoding(path, chunk_size=64) == "utf-8"
