BOOTSTRAP_CYPHER = [
    "CREATE CONSTRAINT recipe_urn_unique IF NOT EXISTS FOR (r:Recipe) REQUIRE r.urn IS UNIQUE;",
    "CREATE CONSTRAINT guide_urn_unique IF NOT EXISTS FOR (g:Guide) REQUIRE g.urn IS UNIQUE;",
    "CREATE CONSTRAINT policy_urn_unique IF NOT EXISTS FOR (p:Policy) REQUIRE p.urn IS UNIQUE;"
]
