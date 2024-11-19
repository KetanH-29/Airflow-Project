import mysql.connector
from mysql.connector import Error

# Database connection details
HOST = "mysql"  # Replace with your MySQL host
USER = "airflow_user"  # Replace with your MySQL username
PASSWORD = "airflow_password"  # Replace with your MySQL password
DATABASE = "airflow"  # Replace with your MySQL database

def create_table_and_insert_data():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host='mysql',
            user='airflow_user',
            password='airflow_password',
            database='airflow'
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Create table if it doesn't exist
            create_table_query = """
            CREATE TABLE IF NOT EXISTS dummy_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100),
                age INT,
                email VARCHAR(100)
            )
            """
            cursor.execute(create_table_query)
            print("Table 'dummy_table' is ready.")

            # Insert 10 dummy records
            insert_query = """
            INSERT INTO dummy_table (name, age, email)
            VALUES (%s, %s, %s)
            """
            dummy_data = [
                ("John Doe", 25, "johndoe@example.com"),
                ("Jane Smith", 30, "janesmith@example.com"),
                ("Alice Johnson", 22, "alicej@example.com"),
                ("Bob Brown", 27, "bobb@example.com"),
                ("Charlie White", 29, "charliew@example.com"),
                ("Daisy Black", 24, "daisyb@example.com"),
                ("Edward Green", 28, "edwardg@example.com"),
                ("Fiona Blue", 26, "fionab@example.com"),
                ("George Gray", 23, "georgeg@example.com"),
                ("Hannah Pink", 31, "hannahp@example.com"),
            ]

            cursor.executemany(insert_query, dummy_data)
            connection.commit()
            print("10 dummy records inserted into 'dummy_table'.")

    except Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    create_table_and_insert_data()
