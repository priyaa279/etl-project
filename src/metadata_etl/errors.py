class ETLError(Exception):
    """Base error for expected ETL failures."""


class ConfigError(ETLError):
    """Raised when a configuration is invalid or unapproved."""


class ExtractionError(ETLError):
    """Raised when a configured source cannot be extracted."""


class LoadError(ETLError):
    """Raised when a destination load cannot be completed."""
