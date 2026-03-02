# Database module
from .review_data_extractor import ReviewDataExtractor, get_extractor

# Backwards-compatible aliases
DatabaseConnection = ReviewDataExtractor
get_db_connection = get_extractor

__all__ = [
    'ReviewDataExtractor',
    'get_extractor',
    'DatabaseConnection',  # Alias for backwards compatibility
    'get_db_connection',   # Alias for backwards compatibility
]
