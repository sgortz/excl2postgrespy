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

def process_certifications(dataframe, vendor_dict, cert_dict):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        for _, row in dataframe.iterrows():
            name = row["Name"]
            certifications = row["Certifications"]

            # Get vendor ID from dictionary
            vendor_id = next((vid for vid, vname in vendor_dict.items() if vname == name), None)
            if not vendor_id:
                continue  # Skip if vendor not found in dictionary

            if pd.isna(certifications) or certifications.strip() == "":
                continue  # Skip if certifications are empty

            cert_list = [cert.strip() for cert in certifications.split(",")]

            for cert in cert_list:
                # Get or create certification ID
                cert_id = next((cid for cid, cname in cert_dict.items() if cname == cert), None)
                if not cert_id:
                    cursor.execute("INSERT INTO ref_cert (name) VALUES (%s) RETURNING id", (cert,))
                    cert_id = cursor.fetchone()[0]
                    cert_dict[cert_id] = cert  # Update dictionary with new cert

                # Check if vendor-cert relationship exists
                cursor.execute("SELECT 1 FROM vendor_cert WHERE vendor_id = %s AND cert_id = %s", (vendor_id, cert_id))
                exists = cursor.fetchone()

                if not exists:
                    cursor.execute("INSERT INTO vendor_cert (vendor_id, cert_id) VALUES (%s, %s)", (vendor_id, cert_id))

        conn.commit()
        cursor.close()
        conn.close()
        print("Certifications processed successfully.")

    except Exception as e:
        print("Error processing certifications:", e)

