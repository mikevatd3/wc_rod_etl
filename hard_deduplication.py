from sqlalchemy import text, create_engine
import pandas as pd

from entity_analyze import graph_cluster


engine = create_engine("postgresql+psycopg://mike@edw:5432/ipds")

def main():
    # Perform advanced deduplication (with clustering)
    pairs_query = """
    SELECT 
        a.naive_name_dedupe AS a_id,
        b.naive_name_dedupe AS b_id
    FROM rod.parsed_party_name a
    JOIN rod.party_classifications apc
        ON a.naive_name_dedupe = apc.naive_name_dedupe
    JOIN rod.parsed_party_name b
        ON (
            a.leading_digits = b.leading_digits
            AND a.naive_name_dedupe > b.naive_name_dedupe
            AND LEFT(a.core, 1) = LEFT(b.core, 1)
            AND a.core = b.core
        ) OR (
            a.leading_digits IS NULL
            AND b.leading_digits IS NULL
            AND a.naive_name_dedupe > b.naive_name_dedupe
            AND LEFT(a.core, 1) = LEFT(b.core, 1)
            AND a.core = b.core
        );
    """
    
    print("pulling pairs")
    frame = pd.read_sql(text(pairs_query), engine)
    
    print("pairing clusters")
    clusters = graph_cluster(frame)

    print("pushing back to database")
    clusters.to_sql(
        "party_name_clusters",
        engine,
        schema="rod",
        index=False,
        if_exists="replace",
    )


if __name__ == "__main__":
    main()


