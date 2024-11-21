import random
import string
import mysql.connector


def generate_random_string(length=10):
    """Generates a random alphanumeric string."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def insert_random_records():
    """Inserts 10 random records into the MySQL database."""
    # MySQL connection details
    conn = mysql.connector.connect(
        host="mysql",  # Use the MySQL service name from docker-compose.yml
        user="airflow",
        password="airflow_password",
        database="test_db"
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_query = """
    CREATE TABLE IF NOT EXISTS random_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(50),
        value INT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    # Insert 10 random records
    for _ in range(10):
        name = generate_random_string()
        value = random.randint(1, 100)
        insert_query = "INSERT INTO random_data (name, value) VALUES (%s, %s)"
        cursor.execute(insert_query, (name, value))

    conn.commit()
    cursor.close()
    conn.close()
