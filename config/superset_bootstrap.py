"""
Bootstrap script for Superset — auto-registers the Trino Iceberg database connection.
Runs as part of superset-init after `superset init` completes.
"""
from superset.app import create_app

app = create_app()

with app.app_context():
    # Import after app initialization to avoid "App not initialized yet" errors
    from superset.models.core import Database
    from superset.extensions import db as superset_db

    existing = superset_db.session.query(Database).filter_by(database_name="Trino Iceberg").first()

    if existing:
        # Update URI if changed
        existing.sqlalchemy_uri = "trino://admin@trino:8080/iceberg"
        existing.expose_in_sqllab = True
        existing.allow_ctas = True
        existing.allow_cvas = True
        existing.allow_dml = True
        existing.allow_run_async = True
        superset_db.session.commit()
        print("✅ Trino Iceberg database connection updated!")
    else:
        trino_conn = Database(
            database_name="Trino Iceberg",
            sqlalchemy_uri="trino://admin@trino:8080/iceberg",
            expose_in_sqllab=True,
            allow_ctas=True,
            allow_cvas=True,
            allow_dml=True,
            allow_run_async=True,
        )
        superset_db.session.add(trino_conn)
        superset_db.session.commit()
        print("✅ Trino Iceberg database connection created!")
