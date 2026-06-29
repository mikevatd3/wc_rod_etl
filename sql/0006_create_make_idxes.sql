CREATE INDEX idx_prop_parcel_id ON rod.properties(parcel_id);
CREATE INDEX idx_prop_instrument_no ON rod.properties(parcel_id);
CREATE INDEX idx_docs_instrument_no ON rod.documents(instrument_no);
CREATE INDEX idx_doc_date ON rod.documents(date_received);

CREATE INDEX idx_assr_sdate ON rod.main_assessors(start_date);
CREATE INDEX idx_assr_edate ON rod.main_assessors(end_date);
CREATE INDEX idx_doc_drec ON rod.documents(date_received);
