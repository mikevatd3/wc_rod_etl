CREATE TABLE rod.grantors AS (
    SELECT id, instrument_no, combined_name AS grantor
    FROM rod.parties
    WHERE grantor_grantee = 'R'
);

CREATE TABLE rod.grantees AS (
    SELECT id, instrument_no, combined_name AS grantee
    FROM rod.parties
    WHERE grantor_grantee = 'E'
);

