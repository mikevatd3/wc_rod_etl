DROP MATERIALIZED VIEW rod.parties_complete CASCADE;

CREATE MATERIALIZED VIEW rod.parties_complete AS (
    SELECT
        id,
        instrument_no,
        CASE 
            WHEN grantor_grantee = 'E' THEN 'GRANTEE'
            WHEN grantor_grantee = 'R' THEN 'GRANTOR'
            ELSE 'CORRUPTED'
        END AS role,
        combined_name,
        COALESCE(cluster_id, p.naive_name_dedupe * 1000000), -- bigints go to quintillions so we're good
        predicted_category,
        designations,
        p.naive_name_dedupe
    FROM rod.parties p
    LEFT JOIN rod.party_name_clusters pnc
        ON p.naive_name_dedupe = pnc.naive_name_dedupe
    LEFT JOIN rod.party_classifications pc
        ON pc.naive_name_dedupe = p.naive_name_dedupe
    LEFT JOIN rod.parsed_party_name ppn
        ON pc.naive_name_dedupe = ppn.naive_name_dedupe
    ORDER BY cluster_id
);

CREATE INDEX idx_parties_instrument_no ON rod.parties_complete(instrument_no);
CREATE INDEX idx_parties_cluster_id ON rod.parties_complete(cluster_id);
CREATE INDEX idx_parties_naive_dedupe ON rod.parties_complete(naive_name_dedupe);


