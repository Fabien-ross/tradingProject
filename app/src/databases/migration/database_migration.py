import os
import re
import psycopg2
import subprocess
from datetime import datetime
from typing import Optional

from src.core.logging.loggers import logger_database
from src.core.utils.config.secret_management import DATABASE_URL
from src.core.exceptions.exceptions import *

class DatabaseMigration:

    def __init__(self):
        self.versions_path="src/databases/migration/versions"
        self.alembic_ini_path = "src/databases/migration/alembic.ini"


    def db_structure_update(self):

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        migration_message = f"auto_migration_{timestamp}"

        current_rev_cmd = ["alembic", "-c", self.alembic_ini_path, "current"]    # table version
        head_rev_cmd = ["alembic", "-c", self.alembic_ini_path, "heads"]         # last written version
        generate_cmd = ["alembic","-c", self.alembic_ini_path,"revision","--autogenerate","-m", migration_message]
        upgrade_cmd = ["alembic","-c", self.alembic_ini_path, "upgrade", "head"]

        try:
            """Generate a versionning file if necessary."""
            gen_result = subprocess.run(generate_cmd, check=True, capture_output=True, text=True)

            name_of_last_identical_version = self.delete_if_migration_is_empty()
            if not name_of_last_identical_version:
                if gen_result.stdout:
                    logger_database.debug(gen_result.stdout)
                if gen_result.stderr:
                    logger_database.debug(gen_result.stderr)
            else:
                logger_database.debug(f"Generated version file is empty. Skipping revision generation. Last head version {name_of_last_identical_version} is already up to date.")
        except subprocess.CalledProcessError as e:
            logger_database.debug("No changes detected in last version file during autogenerate. Skipping revision generation.")
        except Exception as e:
            raise RevisionGenerationError(f"Couldn't generate revision file. Details: {str(e)}")

        try:
            """Get head version."""
            head_result = subprocess.run(head_rev_cmd, check=True, capture_output=True, text=True)
            head_rev = head_result.stdout.strip()
        except Exception as e:
            raise RevisionGenerationError(f"Failed to check Alembic head revision: {e}")

        try:
            """Get current version."""
            current_result = subprocess.run(current_rev_cmd, check=True, capture_output=True, text=True)
            current_rev = current_result.stdout.strip()
        except subprocess.CalledProcessError as e:
            current_rev = None
            logger_database.warning("No current revision found. Database probably not initialized.")
        except Exception as e:
            raise RevisionGenerationError(f"Failed to check current Alembic revision: {e}")

        if current_rev == head_rev:
            logger_database.info(f"Database is already up to date (version {head_rev}). No migration needed.") 
        else:
            logger_database.info(f"Database not up to date. Updating from '{current_rev}' to '{head_rev}'.")
            try:
                upg_result = subprocess.run(upgrade_cmd, check=True, capture_output=True, text=True)
                if upg_result.stdout:
                    logger_database.debug(upg_result.stdout)
                if upg_result.stderr:
                    logger_database.debug(upg_result.stderr)
                logger_database.info("Database successfully upgraded.")
                return True
            except subprocess.CalledProcessError as e:
                print(e.stderr)
            except Exception as e:
                raise MigrationError(f"Couldn't upgrade database to last migration: {e}")


    def reset_alembic_db(self):
        """Deletes alembic table."""

        try:
            for filename in os.listdir(self.versions_path):
                if filename.endswith(".py") or filename.endswith(".pyc"):
                    os.remove(os.path.join(self.versions_path, filename))
            logger_database.info(f"Deleted migration files in {self.versions_path}")
        except Exception as e:
            raise MigrationFilesError(f"Couldn't delete migration files. Details: {str(e)}")
        
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = True
            cur = conn.cursor()
            cur.execute("DROP TABLE IF EXISTS alembic_version;")
            cur.close()
            conn.close()
            logger_database.info("Dropped alembic_version table in database")
        except Exception as e:
            raise TableError(f"Couldn't drop alembic_version table. Details: {str(e)}")


    def is_migration_file_empty(self, path: str) -> bool:
        """Checks if newly created head version file is empty."""
        with open(path, 'r') as f:
            content = f.read()

        return (
            "def upgrade()" in content and "def downgrade()" in content and
            re.search(r"\bop\.", content) is None and
            re.search(r"\bpass\b", content) is not None and
            content.count("pass") >= 2
        )


    def delete_if_migration_is_empty(self):
        """Deletes newly created head version file if empty."""
        files = sorted(
            (os.path.join(self.versions_path, f) for f in os.listdir(self.versions_path) if f.endswith(".py")),
            key=os.path.getmtime,
            reverse=True
        )
        if files:
            latest_file = files[0]
            if self.is_migration_file_empty(latest_file):
                os.remove(latest_file)
                return latest_file
        return None


