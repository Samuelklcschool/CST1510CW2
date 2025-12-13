import sqlite3
import pandas as pd
import bcrypt
from pathlib import Path

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
        print(f"User {username} was not found in the database")
        return False
    dbPassword= user[2]
    encodedPassword = password.encode('utf-8')
    encodedDbPassword = dbPassword.encode('utf-8')

    if bcrypt.checkpw(encodedPassword, encodedDbPassword):
        print(f"Welcome {username}")
        return True
    else:
        print(f"Invalid password for {username}")
        return False


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
    create_users_table(conn)


    # Step 3: Migrate users
    print("\n[3/5] Migrating users from users.txt...")
    user_count = migrate_users_from_file(conn, DATA_DIR / "users.txt")
    print(f"       Migrated {user_count} users")

    # Step 4: Load CSV data
    print("\n[4/5] Loading CSV data...")
    total_rows = load_csv_to_table(conn, DATA_DIR / "cyber_incidents.csv", "cyber_incidents")

    # Step 5: Verify
    print("\n[5/5] Verifying database setup...")
    cursor = conn.cursor()

    # Count rows in each table
    tables = ['users', 'cyber_incidents']
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
setup_database_complete()

