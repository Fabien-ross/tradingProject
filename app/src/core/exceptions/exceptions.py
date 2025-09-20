# -- Base Exception Classes

class StructureError(Exception):
    pass

class DataRetrievingError(Exception):
    pass

class SpoError(Exception):
    pass

class DatabaseError(Exception):
    pass


# -- Structure exceptions

class MissingEnvKeyError(StructureError):
    """Raised when one or several .env keys are missing."""
    def __init__(self, missing_keys : list[str]):
        self.missing_keys = missing_keys
        message = f"Missing env key(s): {', '.join(missing_keys)}"
        super().__init__(message)

# -- Database process exceptions


class TableError(DatabaseError):
    def __init__(self, table_name):
        self.table_name = table_name
        message = f"Table name : {table_name}."
        super().__init__(message)

class MigrationError(DatabaseError):
    pass

class DatabaseAvailabilityError(DatabaseError):
    """Raised when the database is unreachable or fails."""
    pass

class TableNotFoundError(TableError):
    """Raised when the specified table doesn't exist."""
    pass

class InvalidTableNameError(TableError):
    """Raised when a table is unreachable or fails."""
    pass

class EmptyTable(TableError):
    """Raised when a table is empty."""
    pass

class RevisionGenerationError(MigrationError):
    pass

class MigrationFilesError(MigrationError):
    pass


# -- Data Retrieving process exceptions

class MarketAvailabilityError(DataRetrievingError):
    pass


# -- Structural exceptions

class NoMarketSupported(StructureError):
    pass

class AssetTypeNameError(StructureError):
    pass

class MarketNameError(StructureError):
    def __init__(self, market_name):
        self.table_name = market_name
        message = f"Market name : {market_name}."
        super().__init__(message)

class TimeFrameError(StructureError):
    pass
