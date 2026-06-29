 -- Obviously parsed_party_name must be built first

 CREATE INDEX ON rod.parsed_party_name USING gin (core gin_trgm_ops);
 CREATE INDEX ON rod.parsed_party_name (LEFT(core, 1));
