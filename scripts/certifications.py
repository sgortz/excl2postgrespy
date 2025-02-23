from config.db import get_db_connection

def get_certification_ids(certifications, cursor):
    """Fetch or create certification IDs for a given list of certifications."""
    if not certifications:
        return []
    
    cert_list = [cert.strip() for cert in certifications.split(",") if cert.strip()]
    cert_ids = []
    
    for cert in cert_list:
        cursor.execute("SELECT id FROM ref_cert WHERE name = %s", (cert,))
        result = cursor.fetchone()
        if result:
            cert_ids.append(result[0])
        else:
            cursor.execute("INSERT INTO ref_cert (name) VALUES (%s) RETURNING id", (cert,))
            new_id = cursor.fetchone()[0]
            cert_ids.append(new_id)
    
    return cert_ids
