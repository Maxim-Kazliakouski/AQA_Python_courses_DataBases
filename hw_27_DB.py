import mysql.connector
from mysql.connector import Error


# connection to the data base
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print(f"Connection to MySQL DB -> '{db_name}' successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection


connection = create_connection("localhost", "root", "", 'hw_db_orders')


# query execution
def execute_query(connection, query, query_description):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print(f"Query for {query_description} executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")


# selection from the table
def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")


# table creating...

create_user_table = """
CREATE TABLE IF NOT EXISTS orders (
ord_no INT NOT NULL,
purch_amt FLOAT NOT NULL,
ord_date DATE NOT NULL,
customer_id INT,
salesman_id INT
)
"""

execute_query(connection, create_user_table, 'table creating')
print('-' * 100)

# data for fulling table...

fulling_users_table = """
INSERT INTO IF NOT EXISTS
    orders (ord_no, purch_amt, ord_date, customer_id, salesman_id)
VALUES 
        (70001, 150.5, '2012-10-05', 3005, 5002),
        (70009, 270.65, '2012-09-10', 3001, 5005),
        (70002, 65.26, '2012-10-05', 3002, 5001),
        (70004, 110.5, '2012-08-17', 3009, 5003),
        (70007, 948.5, '2012-09-10', 3005, 5002),
        (70005, 2400.6, '2012-07-27', 3007, 5001),
        (70008, 5760, '2012-09-10', 3002, 5001),
        (70010, 1983.43, '2012-10-10', 3004, 5006),
        (70003, 2480.4, '2012-10-10', 3009, 5003),
        (70012, 250.45, '2012-06-27', 3008, 5002),
        (70011, 75.29, '2012-08-17', 3003, 5007),
        (70013, 3045.6, '2012-04-25', 3002, 5001);
"""
execute_query(connection, fulling_users_table, 'adding data to the table')

# query for 1st task
first_query = "select ord_no as 'Order number', ord_date 'Order date'," \
              " purch_amt as 'Purchase amount' from orders where salesman_id=5002" \
              " order by purch_amt asc;"
query_1 = execute_read_query(connection, first_query)
print(' ' * 20, 'Result with order number, order date, purchase amount for salesman_id=5002')
for row in query_1:
    print(row)
print('-' * 100)

# query for 2nd task:

second_query = "select distinct salesman_id from orders order by salesman_id asc;"
query_2 = execute_read_query(connection, second_query)
print(' ' * 20, 'Result with unique salesman_id')
for row in query_2:
    print(str(row).replace(',', ''))
print('-' * 100)

# query for 3rd task:

third_query = "select ord_date as 'Order date', salesman_id as 'Salesman ID'," \
              " ord_no as 'Order number', purch_amt as 'Purchase amount'" \
              " from orders order by purch_amt asc;"
query_3 = execute_read_query(connection, third_query)
print(' ' * 20, 'Result with order date, salesman_id, order number, purchase amount')
for row in query_3:
    print(row)
print('-' * 100)

# query for 4th tas:

fourth_query = "select ord_no as 'Order Number' from orders" \
               " where ord_no between '70001' and '70007' order by ord_no asc;"
query_4 = execute_read_query(connection, fourth_query)
print(' ' * 20, 'Result with number orders between 70001 and 70007')
for row in query_4:
    print(str(row).replace(',', ''))
print('-' * 100)

print(f'Closing connection with database...')
connection.close()
