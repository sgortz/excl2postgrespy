import pandas as pd
from config.db import get_db_connection
from scripts.vendor import get_all_vendors
from scripts.certification import get_certification_ids

def load_excel_data(file_path):
    return pd.read_excel(file_path, sheet_name=1)  # Load second sheet

def convert_ownership(value):
    if pd.isna(value) or value.lower() == "unknown":
        return None
    elif value.lower() == "yes":
        return True
    elif value.lower() == "no":
        return False
    return None

def update_database(dataframe):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        vendors = get_all_vendors()  # Get vendor ID mappings once
        
        for _, row in dataframe.iterrows():
            name = row["Name"]
            ownership = convert_ownership(row["Firm Ownership"])
            certifications = row["Certifications"]
            
            vendor_id = vendors.get(name)
            if vendor_id:
                # Update vendor ownership and year
                cursor.execute("""
                    UPDATE your_table
                    SET ownership = %s, year = 2025
                    WHERE id = %s
                """, (ownership, vendor_id))
                
                # Get certification IDs
                cert_ids = get_certification_ids(certifications, cursor)
                
                # Insert into vendor_cert table if not exists
                for cert_id in cert_ids:
                    cursor.execute("SELECT 1 FROM vendor_cert WHERE vendor_id = %s AND cert_id = %s", (vendor_id, cert_id))
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO vendor_cert (vendor_id, cert_id) VALUES (%s, %s)", (vendor_id, cert_id))
        
        conn.commit()
        cursor.close()
        conn.close()
        print("Database updated successfully.")
        
    except Exception as e:
        print("Error updating database:", e)

if __name__ == "__main__":
    file_path = "your_file.xlsx"
    data = load_excel_data(file_path)
    update_database(data)
