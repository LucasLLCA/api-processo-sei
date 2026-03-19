"""
Clean all data from Neo4j graph database.

Usage:
    python scripts/clean_neo4j.py              # interactive confirmation
    python scripts/clean_neo4j.py --force      # skip confirmation
"""

import argparse
import logging

from neo4j import GraphDatabase

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password123"


def clean(driver):
    """Delete all nodes and relationships in batches, then drop constraints."""
    with driver.session() as session:
        result = session.run(
            "MATCH (n) RETURN count(n) AS nodes, "
            "size([(n)-[r]-() | r]) AS rels"
        )
        rec = result.single()
    total_nodes = rec["nodes"]
    log.info("Found %d nodes to delete", total_nodes)

    # Step 1: Delete relationships first (lightweight, no cascade)
    deleted_rels = 0
    while True:
        with driver.session() as session:
            result = session.run(
                "MATCH ()-[r]->() WITH r LIMIT 5000 DELETE r RETURN count(*) AS cnt"
            )
            batch = result.single()["cnt"]
        if batch == 0:
            break
        deleted_rels += batch
        if deleted_rels % 50000 == 0 or batch < 5000:
            log.info("  Deleted %d relationships", deleted_rels)

    log.info("All %d relationships deleted", deleted_rels)

    # Step 2: Delete orphan nodes (no DETACH needed)
    deleted_nodes = 0
    while True:
        with driver.session() as session:
            result = session.run(
                "MATCH (n) WITH n LIMIT 10000 DELETE n RETURN count(*) AS cnt"
            )
            batch = result.single()["cnt"]
        if batch == 0:
            break
        deleted_nodes += batch
        if deleted_nodes % 50000 == 0 or batch < 10000:
            log.info("  Deleted %d / %d nodes", deleted_nodes, total_nodes)

    log.info("All %d nodes deleted", deleted_nodes)

    # Drop constraints and indexes
    with driver.session() as session:
        result = session.run("SHOW CONSTRAINTS")
        constraints = [r["name"] for r in result]
    for name in constraints:
        with driver.session() as session:
            session.run(f"DROP CONSTRAINT {name} IF EXISTS")
        log.info("  Dropped constraint: %s", name)

    with driver.session() as session:
        result = session.run("SHOW INDEXES")
        indexes = [r["name"] for r in result if r["type"] != "LOOKUP"]
    for name in indexes:
        with driver.session() as session:
            session.run(f"DROP INDEX {name} IF EXISTS")
        log.info("  Dropped index: %s", name)

    log.info("Cleanup complete")


def main():
    parser = argparse.ArgumentParser(description="Clean Neo4j database")
    parser.add_argument("--force", action="store_true", help="Skip confirmation")
    args = parser.parse_args()

    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    driver.verify_connectivity()
    log.info("Connected to Neo4j: %s", NEO4J_URI)

    if not args.force:
        answer = input("This will DELETE ALL DATA in Neo4j. Continue? [y/N] ")
        if answer.lower() != "y":
            log.info("Aborted")
            driver.close()
            return

    clean(driver)
    driver.close()


if __name__ == "__main__":
    main()
