-- Deduplication queries 

DELETE FROM rod.documents
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM rod.documents
    GROUP BY 
        instrument_no, 
        date_received, 
        record_type, 
        consideration, 
        document_date  -- columns that define a duplicate
);


DELETE FROM rod.parties
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM rod.parties
    GROUP BY 
        instrument_no,
        combined_name,
        grantor_grantee
);


DELETE FROM rod.properties
WHERE ctid NOT IN (
    SELECT MIN(ctid)
    FROM rod.properties
    GROUP BY 
        instrument_no,
        parcel_id,
        street_no,
        street_name,
        municipality
);
