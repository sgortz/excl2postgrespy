from config.db import get_db_connection

def get_all_vendors():
    """Fetch all vendor names and their IDs from the database once."""
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT id, name FROM your_table")
    vendors = {name: vendor_id for vendor_id, name in cursor.fetchall()}
    cursor.close()
    conn.close()
    return vendors
