"""Offline checks for the v2 source layout.

These run against the committed sample export with nothing but pandas -- no
database, no entity_analyze, no files from the vault.
"""

from pathlib import Path

import pandas as pd
import pytest

from transforms import (
    check_v2_header,
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


def test_check_v2_header_reports_a_shifted_column(tmp_path):
    bad = tmp_path / "bad.csv"
    bad.write_text("DOC_TYPE_CODE,SURPRISE,CF_VCINSTNUM\n")

    problems = check_v2_header(bad)

    assert any("SURPRISE" in problem for problem in problems)
    assert any("expected 19 columns" in problem for problem in problems)


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
