import psycopg2

# Your correct External Render PostgreSQL connection string
database_url = "postgresql://nexus_db_r3hx_user:ytJmwdkN0ZeLfMjo5bCKAxnFA1Gm8sPO@dpg-dabrh6afngtc73f2i240-a.oregon-postgres.render.com/nexus_db_r3hx"

try:
    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    cursor = conn.cursor()
    
    cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    print("Successfully enabled pgvector extension!")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"Error connecting or enabling extension: {e}")