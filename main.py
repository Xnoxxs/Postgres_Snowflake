
import psycopg2
import csv
import snowflake.connector

# ---------- GET DATA FROM POSTGRES ----------
def connect_pg():
    return psycopg2.connect(
        database="postgres",
        user="student",
        password="HSBCN323TestDb36111",
        host="database-1.cffitrsftriq.eu-central-1.rds.amazonaws.com",
        port="5432"
    )

pg_conn = connect_pg()
pg_cursor = pg_conn.cursor()

# Query to select all data from Venture.bookings
query = '''
SELECT 
    id, activity_id, user_id, people, 
    extras::text, 
    promotion, promotion_id, status, booking_type,
    start_date, end_date, reservation_date, activity_price, total_price, 
    asked_for_rating, is_rated, 
    cancellation_policies::text, 
    refund::text, 
    payment_intent_id
FROM "Venture".bookings;
'''
pg_cursor.execute(query)
rows = pg_cursor.fetchall()

# Write to CSV
csv_file_name = 'bookings_data.csv'
csv_file_path = '/Users/skr/Documents/' + csv_file_name

with open(csv_file_path, 'w', newline='') as csvfile:
    csvwriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
    csvwriter.writerow([desc[0] for desc in pg_cursor.description])  # header
    csvwriter.writerows(rows)

print(f"Data extracted to {csv_file_path}")

print("CSV columns:", [desc[0] for desc in pg_cursor.description])

pg_cursor.close()
pg_conn.close()

# ---------- INSERTING DATA TO SNOWFLAKE ----------
snow_con = snowflake.connector.connect(
    user='student',
    password='HSUnivSFTests42213',
    account='GKB48589',
    warehouse='COMPUTE_S',
    database='SF_SAMPLE',
    ocsp_fail_open=False
)
snow_curs = snow_con.cursor()

# Ensure schema exists
snow_curs.execute("CREATE SCHEMA IF NOT EXISTS SF_SAMPLE.Venture")

# Create table in the correct schema
create_table_query = """
CREATE TABLE IF NOT EXISTS SF_SAMPLE.PUBLIC.Venture_bookings (
    id INT,
    activity_id INT,
    user_id INT,
    people INT,
    extras STRING,
    promotion BOOLEAN,
    promotion_id STRING,
    status STRING,
    booking_type STRING,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    reservation_date TIMESTAMP,
    activity_price FLOAT,
    total_price FLOAT,
    asked_for_rating BOOLEAN,
    is_rated BOOLEAN,
    cancellation_policies STRING,
    refund STRING,
    payment_intent_id STRING
);
"""
snow_curs.execute(create_table_query)
snow_curs.execute("USE SCHEMA SF_SAMPLE.Venture")
sf_stage = '@~'

# Upload CSV to Snowflake stage
put_command = f"""PUT file://{csv_file_path} {sf_stage}"""
print(put_command)
snow_curs.execute(put_command).fetchall()
snow_curs.execute("LIST @~").fetchall()

# Load data into table
table_name = "SF_SAMPLE.PUBLIC.Venture_bookings"
copy_into_cmd = f"""
COPY INTO {table_name}
FROM {sf_stage}/{csv_file_name}.gz
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"' NULL_IF=(''));
"""
print(copy_into_cmd)
snow_curs.execute(copy_into_cmd).fetchall()

# Check data
sql = 'SELECT * FROM SF_SAMPLE.PUBLIC.Venture_bookings'
snow_curs.execute(sql)
for r in snow_curs.fetchall():
    print(r)

# Clean up stage
snow_curs.execute("REMOVE @~").fetchall()
snow_curs.close()
snow_con.close()