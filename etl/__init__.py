"""Fantasy Baseball ETL Pipeline.

Three-layer architecture:
  - Staging (stg_*): source-shaped, minimally transformed, auditable
  - Core (core_*): deduplicated, keyed, business logic applied
  - Serving: views the ORM reads from (named after original tables)
"""
