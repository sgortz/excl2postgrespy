import psycopg2
import pandas as pd

# Database connection details
DB_HOST = "your_host"
DB_NAME = "your_database"
DB_USER = "your_username"
DB_PASSWORD = "your_password"

# Sample vendor and certification dictionaries
vendor_list = {"Vendor A": 1, "Vendor B": 2}
cert_list = {"Cert X": 101, "Cert Y": 102}

# Sample dataframe
data = {"Vendor": ["Vendor A", "Vendor B", "Vendor C"],
        "Certification": ["Cert X", "", "Cert Y"]}
cert_df = pd.DataFrame(data)

try:
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    # Iterate over the dataframe
    for _, row in cert_df.iterrows():
        vendor_name = row["Vendor"]
        cert_name = row["Certification"]

        # Lookup vendor_id
        vendor_id = vendor_list.get(vendor_name)

        if not vendor_id:
            print(f"Vendor '{vendor_name}' not found, skipping...")
            continue  # Skip if vendor not found

        if cert_name:  # Step 1: If Certification column has a value
            cert_id = cert_list.get(cert_name)

            if not cert_id:
                print(f"Certification '{cert_name}' not found, skipping...")
                continue  # Skip if certification not found

            # Step 1.3: Check if the record already exists
            cur.execute("""
                SELECT 1 FROM vendor_certifications 
                WHERE vendor_id = %s AND cert_id = %s
            """, (vendor_id, cert_id))
            
            if not cur.fetchone():  # If no record exists, insert new one
                cur.execute("""
                    INSERT INTO vendor_certifications (vendor_id, cert_id)
                    VALUES (%s, %s)
                """, (vendor_id, cert_id))
                print(f"Inserted ({vendor_id}, {cert_id})")

        else:  # Step 2: If "Certification" column is empty
            # Check if vendor has existing records
            cur.execute("""
                SELECT 1 FROM vendor_certifications WHERE vendor_id = %s
            """, (vendor_id,))
            
            if cur.fetchone():  # If records exist, delete them
                cur.execute("""
                    DELETE FROM vendor_certifications WHERE vendor_id = %s
                """, (vendor_id,))
                print(f"Deleted all certifications for Vendor ID {vendor_id}")

    # Commit all changes to the database
    conn.commit()
    print("Database update complete.")

    # Close the cursor and connection
    cur.close()
    conn.close()

except Exception as e:
    print("Error:", e)
