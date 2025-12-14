import sqlite3
import pandas as pd
import bcrypt
import os
from pathlib import Path
import datetime

# Define paths
DATA_DIR = Path(r"C:\Users\samue\PycharmProjects\PythonProject\CST1510\CST1510 CW2\DATA")
DB_PATH = DATA_DIR / "intelligence_platform.db"

# Create DATA folder if it doesn't exist
DATA_DIR.mkdir(parents=True, exist_ok=True)

print(" Imports successful!")
print(f" DATA folder: {DATA_DIR.resolve()}")
print(f" Database will be created at: {DB_PATH.resolve()}")


def connect_database(db_path=DB_PATH):
    conn = sqlite3.connect(str(db_path))
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print(f"The database was connected to {db_path} successfully!")

    return conn


def create_users_table(conn):
    cursor = conn.cursor()

    cursor.execute("""CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT NOT NULL UNIQUE,
        password_hash TEXT NOT NULL,
        role TEXT DEFAULT 'user',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    conn.commit()
    print("Users table was created successfully!")


def create_cyber_incidents_table(conn):
    cursor = conn.cursor()
    table = """
    CREATE TABLE IF NOT EXISTS cyber_incidents (
        incident_id INTEGER PRIMARY KEY NOT NULL,
        date TEXT,
        timestamp TIMESTAMP,
        severity TEXT NOT NULL,
        category TEXT NOT NULL,
        status TEXT NOT NULL,
        description TEXT ,
        reported_by TEXT 
    )
    """

    cursor.execute(table)

    conn.commit()
    print("Cyber incidents table was created successfully!")
    pass

def create_it_tickets_table(conn):
    cursor = conn.cursor()
    table = """
    CREATE TABLE IF NOT EXISTS it_tickets (
        ticket_id INTEGER PRIMARY KEY NOT NULL,
        priority TEXT NOT NULL,
        description TEXT NOT NULL,
        status TEXT NOT NULL,
        assigned_to TEXT NOT NULL,
        created_at TIMESTAMP,
        resolution_time_hours INTEGER NOT NULL
    )
    """
    cursor.execute(table)
    print("IT tickets table was created successfully!")
    return

def create_datasets_metadata(conn):
    cursor = conn.cursor()
    table= """
    CREATE TABLE IF NOT EXISTS datasets_metadata (
        dataset_id INTEGER PRIMARY KEY NOT NULL,
        name TEXT NOT NULL,
        rows INTEGER NOT NULL,
        columns INTEGER NOT NULL,
        uploaded_by TEXT NOT NULL,
        upload_date TIMESTAMP 
    )
    """
    cursor.execute(table)
    print("Datasets metadata table was created successfully!")
    return

def migrate_users_from_file(conn, filepath=DATA_DIR / "users.txt"):

    if not filepath.exists():
        print(f"File not found: {filepath}")
        print("No users to migrate.")
        return
    cursor = conn.cursor()
    migratedCount = 0
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            parts = line.split(',')
            if len(parts) >= 2:
                username = parts[0]
                password = parts[1]
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO users (username, password_hash, role) VALUES (?, ?, ?)",
                        (username, password, 'user')
                    )
                    if cursor.rowcount > 0:
                        migratedCount += 1
                except sqlite3.Error as e:
                    print(f"Error migrating user {username}: {e}")

    conn.commit()
    print(f"Migrated {migratedCount} users from {filepath.name}")
    return migratedCount


def register_user(username, password, role="user"):
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    if cursor.fetchone():
        conn.close()
        return False, f"Username '{username}' already exists. Please choose another Username"
    encodedPassword = password.encode('utf-8')
    salt = bcrypt.gensalt()
    hashedPassword = bcrypt.hashpw(encodedPassword, salt)
    encodedHashedPassword = hashedPassword.decode('utf-8')
    cursor.execute(
        "INSERT INTO users (username, password_hash, role) VALUES (?, ?, ?)",
        (username, encodedHashedPassword, role)
    )
    conn.commit()
    conn.close()
    print(f"User {username} was successfully added to the database")

    return True


def login_user(username, password):
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    user = cursor.fetchone()
    conn.close()
    if not user:
        return("User was not found")
    dbPassword= user[2]
    encodedPassword = password.encode('utf-8')
    encodedDbPassword = dbPassword.encode('utf-8')

    if bcrypt.checkpw(encodedPassword, encodedDbPassword):
        return (f"Welcome {username}")
    else:
        return (f"Invalid password for {username}")


def load_csv_to_table(conn, csvPath, tableName):
    try :
        df = pd.read_csv(csvPath)
        print(df.head())
        df.to_sql(tableName, conn, if_exists='append', index=False)

        print(f"Table {tableName} was successfully loaded from {csvPath}")
        return len(df)
    except FileNotFoundError:
        print(f"File not found: {csvPath}")

def get_column_names(conn, table_name):
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = [col[1] for col in cursor.fetchall()]
    return sorted(columns)

def insert_incident(conn, date, incident_type, severity, status, description, reported_by=None):
    cursor= conn.cursor()
    cursor.execute("INSERT INTO cyber_incidents (date, incident_type, severity, status, description, reported_by) VALUES (?,?,?,?,?,?) ", (date, incident_type, severity, status, description, reported_by))
    conn.commit()
    return cursor.lastrowid

def insert_it_ticket(conn, ticket_id, priority, description, status, assigned_to, created_at,resolution_time):
   cursor = conn.cursor()
   cursor.execute("INSERT INTO it_tickets (ticket_id, priority, description, status, assigned_to, created_at,resolution_time) VALUES (?,?,?,?,?,?,?)""",(ticket_id, priority, description, status, assigned_to, created_at,resolution_time))
   return cursor.lastrowid

def insert_dataset(conn,dataset_id, name, rows, columns, uploaded_by, uploaded_at ):
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO datasets_metadata (dataset_id, name, rows, columns, uploaded_by, uploaded_at) VALUES (?,?,?,?,?,?)""", (dataset_id, name, rows, columns, uploaded_by, uploaded_at))
    return cursor.lastrowid

def get_all_incidents():
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM cyber_incidents")
    temp = cursor.fetchall()
    df = pd.DataFrame(temp)
    return df

def get_all_dataset():
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM datasets_metadata")
    temp = cursor.fetchall()
    df = pd.DataFrame(temp)
    return df

def get_all_ticket():
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM it_tickets")
    temp = cursor.fetchall()
    df = pd.DataFrame(temp)
    return df

def look_for_incident(conn, incident_id, date, severity, category, status, reported_by = None):
    incidentDictionary= {
        "incident_id":[],
        "date": [],
        "severity": [],
        "category": [],
        "status": [],
        "description": [],
        "reported_by": []
    }
    cursor = conn.cursor()
    if incident_id != -1:
        cursor.execute("""SELECT * FROM cyber_incidents WHERE incident_id = ?""", (int(incident_id),))
        return cursor.fetchone()
    else:
        cursor.execute("""SELECT * FROM cyber_incidents""")
        incidentList = cursor.fetchall()
        for incident in incidentList:
            print(incident)
            if str(date) == incident[1] or date == "":
                if severity == incident[3] or severity == "":
                    if category == incident[4] or category == "":
                        if status == incident[5] or status == "":
                            if reported_by == incident[7] or reported_by == "":
                                incidentDictionary["incident_id"].append(incident[0])
                                incidentDictionary["date"].append(incident[1])
                                incidentDictionary["severity"].append(incident[3])
                                incidentDictionary["category"].append(incident[4])
                                incidentDictionary["status"].append(incident[5])
                                incidentDictionary["description"].append(incident[6])
                                incidentDictionary["reported_by"].append(incident[7])
        df = pd.DataFrame(incidentDictionary)
        return df

def look_for_IT_ticket(conn, ticket_id, priority, description, status, assigned_to, created_at,resolution_time):
    ticketDictionary= {
        "ticket_id":[],
        "priority": [],
        "description": [],
        "status": [],
        "assigned_to": [],
        "created_at": [],
        "resolution_time": []

    }
    cursor = conn.cursor()
    if ticket_id != -1:
        cursor.execute("""SELECT * FROM it_tickets WHERE ticket_id = ?""", (ticket_id,))
        return cursor.fetchone()
    else:
        cursor.execute("""SELECT * FROM it_tickets""")
        ticketList = cursor.fetchall()
        for ticket in ticketList:
            print(ticket)
            if priority == ticket[1] or priority == "":
                if status == ticket[3] or status == "":
                    if assigned_to == ticket[4] or assigned_to == "":
                        if created_at == str(ticket[5]) or created_at == "":
                            if resolution_time == ticket[6] or resolution_time == "":
                                ticketDictionary["ticket_id"].append(ticket[0])
                                ticketDictionary["priority"].append(ticket[1])
                                ticketDictionary["description"].append(ticket[2])
                                ticketDictionary["status"].append(ticket[3])
                                ticketDictionary["assigned_to"].append(ticket[4])
                                ticketDictionary["created_at"].append(ticket[5])
                                ticketDictionary["resolution_time"].append(ticket[6])
        df = pd.DataFrame([ticketDictionary])
        return df

def look_for_dataset(conn, dataset_id, name, rows, columns, uploaded_by, uploaded_at ):
    datasetDictionary= {
        "dataset_id":[],
        "name": [],
        "rows": [],
        "columns": [],
        "uploaded_by": [],
        "uploaded_at": [],
    }
    cursor = conn.cursor()
    if dataset_id != -1:
        cursor.execute("""SELECT * FROM datasets_metadata WHERE dataset_id = ?""", (dataset_id,))
        return cursor.fetchone()
    else:
        cursor.execute("""SELECT * FROM datasets_metadata""")
        datasetList = cursor.fetchall()
        for dataset in datasetList:
            if name == dataset[1] or name == "":
                if rows == dataset[2] or rows == "":
                    if columns == dataset[3] or columns == "":
                        if uploaded_by == dataset[4] or uploaded_by == "":
                            if uploaded_at == dataset[5] or uploaded_at == "":
                                datasetDictionary["dataset_id"].append(dataset[0])
                                datasetDictionary["name"].append(dataset[1])
                                datasetDictionary["rows"].append(dataset[2])
                                datasetDictionary["columns"].append(dataset[3])
                                datasetDictionary["uploaded_by"].append(dataset[4])
                                datasetDictionary["uploaded_at"].append(dataset[5])
        df = pd.DataFrame([datasetDictionary])
        return df

def delete_incident(conn, incident_id):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cyber_incidents WHERE incident_id = ?", (incident_id,))

def delete_IT_ticket(conn, ticket_id):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM it_tickets WHERE ticket_id = ?", (ticket_id,))

def delete_dataset(conn, dataset_id):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM datasets_metadata WHERE dataset_id = ?", (dataset_id,))

def setup_database_complete():
    """
    Complete database setup:
    1. Connect to database
    2. Create all tables
    3. Migrate users from users.txt
    4. Load CSV data for all domains
    5. Verify setup
    """
    print("\n" + "="*60)
    print("STARTING COMPLETE DATABASE SETUP")
    print("="*60)

    # Step 1: Connect
    print("\n[1/5] Connecting to database...")
    conn = connect_database()
    print("       Connected")

    # Step 2: Create tables
    print("\n[2/5] Creating database tables...")
    create_cyber_incidents_table(conn)
    create_datasets_metadata(conn)
    create_it_tickets_table(conn)
    create_users_table(conn)


    # Step 3: Migrate users
    print("\n[3/5] Migrating users from users.txt...")
    user_count = migrate_users_from_file(conn, DATA_DIR / "users.txt")
    print(f"       Migrated {user_count} users")

    # Step 4: Load CSV data
    print("\n[4/5] Loading CSV data...")
    load_csv_to_table(conn, DATA_DIR /"cyber_incidents.csv", "cyber_incidents")
    load_csv_to_table(conn, DATA_DIR /"datasets_metadata.csv", "datasets_metadata")
    load_csv_to_table(conn, DATA_DIR /"it_tickets.csv", "it_tickets")
    # Step 5: Verify
    print("\n[5/5] Verifying database setup...")
    cursor = conn.cursor()

    # Count rows in each table
    tables = ['users', 'cyber_incidents', 'datasets_metadata','it_tickets']
    print("\n Database Summary:")
    print(f"{'Table':<25} {'Row Count':<15}")
    print("-" * 40)

    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:<25} {count:<15}")

    conn.close()

    print("\n" + "="*60)
    print(" DATABASE SETUP COMPLETE!")
    print("="*60)
    print(f"\n Database location: {DB_PATH.resolve()}")
    print("\nYou're ready for Week 9 (Streamlit web interface)!")

conn = connect_database()
cursor = conn.cursor()


