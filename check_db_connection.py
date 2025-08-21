import psycopg2

# Replace these with your actual DB details
DB_CONFIG = {
	'host': 'localhost',         # 👈 change if your DB is remote
    'dbname': 'Nifty_Data_Analysis',
    'user': 'postgres',
    'password': 'Alliswell@28',
    'port': 5432
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    print("✅ Database connection successful!")
    conn.close()
except Exception as e:
    print("❌ Failed to connect to database:")
    print(e)
