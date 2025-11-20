import logging
from contextlib import contextmanager

# This ensures atomicity: all inserts commit, or all rollback on error.
@contextmanager
def transaction(conn):
    """Context manager for database transactions."""
    try:
        cursor = conn.cursor()
        yield cursor
        # Commit on the connection object
        conn.commit() 
    except Exception as e:
        # Rollback MUST be called on the connection object (conn)
        conn.rollback() 
        logging.error(f"Transaction failed, rolling back: {e}")
        cursor.close()
        raise
    finally:
        # Always close the cursor, whether success or failure
        cursor.close()
    