import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from sqlalchemy import text




connection = pymysql.connect(
    host="localhost",
    user="root",
    password="haresh@27root"
)
cursor_mysql = connection.cursor()

cursor_mysql.execute("CREATE DATABASE IF NOT EXISTS Bank_sight;")
cursor_mysql.execute("USE Bank_sight;")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS customers(
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(50),
    gender VARCHAR(10),
    age INT,
    city VARCHAR(50),
    account_type VARCHAR(50),
    join_date DATE
)
""")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS accounts(
    account_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id VARCHAR(20),
    account_balance FLOAT,
    last_updated DATETIME
)
""")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS transactions(
    tnx_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    tnx_type VARCHAR(30),
    amount FLOAT,
    tnx_time DATETIME,
    status VARCHAR(20)
)
""")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS loan(
    Loan_ID INT PRIMARY KEY,
    Customer_ID INT,
    Account_ID INT,
    Branch VARCHAR(50),
    Loan_Type VARCHAR(30),
    Loan_Amount INT,
    Interest_Rate FLOAT,
    Loan_Term_Months INT,
    Start_Date DATE,
    End_Date DATE,
    Loan_Status VARCHAR(20)
)
""")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS credit_cards(
    Card_ID INT PRIMARY KEY,
    Customer_ID INT,
    Account_ID INT,
    Branch VARCHAR(50),
    Card_Number VARCHAR(20),
    Card_Type VARCHAR(20),
    Card_Network VARCHAR(20),
    Credit_Limit INT,
    Current_Balance FLOAT,
    Issued_Date DATE,
    Expiry_Date DATE,
    Status VARCHAR(20)
)
""")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS branches(
    Branch_ID INT PRIMARY KEY,
    Branch_Name VARCHAR(50),
    City VARCHAR(50),
    Manager_Name VARCHAR(50),
    Total_Employees INT,
    Branch_Revenue FLOAT,
    Opening_Date DATE,
    Performance_Rating INT
)
""")

cursor_mysql.execute("""
CREATE TABLE IF NOT EXISTS support__tickets(
    Ticket_ID VARCHAR(30) PRIMARY KEY,
    Customer_ID VARCHAR(20),
    Account_ID VARCHAR(20),
    Loan_ID VARCHAR(20),
    Branch_Name VARCHAR(50),
    Issue_Category VARCHAR(50),
    Description TEXT,
    Date_Opened DATE,
    Date_Closed DATE,
    Priority VARCHAR(20),
    Status VARCHAR(20),
    Resolution_Remarks TEXT,
    Support_Agent VARCHAR(50),
    Channel VARCHAR(20),
    Customer_Rating INT
)
""")

connection.commit()
connection.close()

print("Tables created successfully")

password ="haresh@27root"
encoded_password = quote_plus(password)   

engine = create_engine(

   f"mysql+pymysql://root:{encoded_password}@localhost/Bank_sight"
)

def execute_query(query):
   return pd.read_sql(query, engine)


q1 = """
SELECT c.city,
       COUNT(DISTINCT c.customer_id) AS total_customers,
       AVG(a.account_balance) AS avg_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.city;
"""

df1 = execute_query(q1)
print(df1)

q2 = """
SELECT account_type, SUM(account_balance) AS total_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY account_type
ORDER BY total_balance DESC
LIMIT 1;
"""
print(execute_query(q2))

q3 = """
SELECT c.customer_id, c.name,
SUM(a.account_balance) AS total_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_balance DESC
LIMIT 10;
"""
print(execute_query(q3))
q3 = """
SELECT c.customer_id, c.name,
SUM(a.account_balance) AS total_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
GROUP BY c.customer_id, c.name
ORDER BY total_balance DESC
LIMIT 10;
"""
print(execute_query(q3))

q4 = """
SELECT DISTINCT c.customer_id, c.name, a.account_balance
FROM customers c
JOIN accounts a ON c.customer_id = a.customer_id
WHERE YEAR(c.join_date) = 2023
AND a.account_balance > 100000;
"""
print(execute_query(q4))

q5 = """
SELECT tnx_type, SUM(amount) AS total_amount
FROM transactions
GROUP BY tnx_type;
"""
print(execute_query(q5))

q6 = """
SELECT tnx_type, COUNT(*) AS failed_count
FROM transactions
WHERE status = 'Failed'
GROUP BY tnx_type;
"""
print(execute_query(q6))

q7 = """
SELECT tnx_type, COUNT(*) AS total_transactions
FROM transactions
GROUP BY tnx_type;
"""
print(execute_query(q7))

q8 = """
SELECT customer_id, COUNT(*) AS high_value_count
FROM transactions
WHERE amount > 20000
GROUP BY customer_id
HAVING COUNT(*) >= 5;
"""
print(execute_query(q8))

q9 = """
SELECT Loan_Type,
AVG(Loan_Amount) AS avg_loan,
AVG(Interest_Rate) AS avg_interest
FROM loan
GROUP BY Loan_Type;
"""
print(execute_query(q9))

q10 = """
SELECT Customer_ID, COUNT(*) AS loan_count
FROM loan
WHERE Loan_Status IN ('Active', 'Approved')
GROUP BY Customer_ID
HAVING COUNT(*) > 1;
"""
print(execute_query(q10))

q11 = """
SELECT Customer_ID,
SUM(Loan_Amount) AS outstanding_amount
FROM loan
WHERE Loan_Status != 'Closed'
GROUP BY Customer_ID
ORDER BY outstanding_amount DESC
LIMIT 5;
"""
print(execute_query(q11))

q12 = """
SELECT Branch, AVG(Loan_Amount) AS avg_loan
FROM loan
GROUP BY Branch;
"""
print(execute_query(q12))

q13 = """
SELECT
CASE
    WHEN age BETWEEN 18 AND 25 THEN '18-25'
    WHEN age BETWEEN 26 AND 35 THEN '26-35'
    WHEN age BETWEEN 36 AND 45 THEN '36-45'
    ELSE '46+'
END AS age_group,
COUNT(*) AS total_customers
FROM customers
GROUP BY age_group;
"""
print(execute_query(q13))

q14 = """
SELECT Issue_Category,
AVG(DATEDIFF(Date_Closed, Date_Opened)) AS avg_days
FROM support__tickets
WHERE Date_Closed IS NOT NULL
GROUP BY Issue_Category
ORDER BY avg_days DESC;
"""
print(execute_query(q14))

q15 = """
SELECT Support_Agent, COUNT(*) AS resolved_count
FROM support__tickets
WHERE Priority = 'Critical'
AND Customer_Rating >= 4
AND Status = 'Resolved'
GROUP BY Support_Agent
ORDER BY resolved_count DESC;
"""
print(execute_query(q15))



import streamlit as st

st.set_page_config(page_title="BankSight", layout="wide")
st.markdown("""
<style>
h1 { font-weight: 800; color: #111; }
h2 { font-weight: 700; }
p  { font-size: 18px; font-weight: 500; }
</style>
""", unsafe_allow_html=True)

st.sidebar.title("📈 BankSight Navigation")
menu = st.sidebar.radio(
    "Go to",
    [
        "📘Introduction",
        "📋View Tables",
        "🔍Filter Data",
        "✏️CRUD Operations",
        "💳Credit/Debit Simulation",
        "📊Analytical Insights",
        "👤About Creator"
    ]
)
st.title("🏢 BankSight: Transaction Intelligence Dashboard")
if menu == "📘Introduction":
    st.header("Project Overview")
    st.write("Banksight is a financial analyticals system built using ***PYTHON***, ***STREAMLIT***, and ***PYMYSQL***.it allows user to explore customer, account, transaction, loan, and support data, perform CRUD opertion, simulate deposit/withdrawals, and view analytical insights.")
    st.header("Objectives:")
    st.write("""
    - Analyze customer profiles and transaction patterns to gain meaningful insights into banking behavior
    - Identify anomalies in transaction data and flag potential fraudulent activities
    - Implement complete CRUD (Create, Read, Update, Delete) operations across all datasets
    - Simulate real-world banking transactions, including credit and debit processes
        """  )
def view_table(table_name):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df    
    
if menu == "📋View Tables":
    st.header("📋 View Database Tables")

    table_name = st.selectbox(
        "Select a Table",
        [
            "customers",
            "accounts",
            "transactions",
            "branches",
            "loan",
            "credit_cards",
            "support__tickets"
        ]
    )
    

    if table_name:
        df = view_table(table_name)

        st.success(f"Showing data from `{table_name}` table")
        st.dataframe(df, use_container_width=True)

def get_table_data(table_name):
    query = f"SELECT * FROM {table_name}"
    return pd.read_sql(query, engine)


def get_column_values(table_name, column_name):
    query = f"SELECT DISTINCT {column_name} FROM {table_name}"
    df = pd.read_sql(query, engine)
    return df[column_name].dropna().tolist()

if menu == "🔍Filter Data":
    st.header("🔍 Filter Data")
    table_name = st.selectbox(
        "Select Table to Filter",
        [
            "customers",
            "accounts",
            "transactions",
            "branches",
            "loan",
            "credit_cards",
            "support__tickets"
        ]
    )

    if table_name:
        df = get_table_data(table_name)

        column_name = st.selectbox(
            "Select Column to Filter",
            df.columns
        )

        if column_name:
    
            values = get_column_values(table_name, column_name)

            value_selected = st.selectbox(
                f"Select value from `{column_name}`",
                values
            )

        if value_selected is not None:
            
                filtered_df = df[df[column_name] == value_selected]

                st.success(
                    f"Filtered results for {column_name} = {value_selected}"
                )

                st.dataframe(filtered_df, use_container_width=True)

def load_table(table_name):
    return pd.read_sql(f"SELECT * FROM {table_name}", engine)


def get_columns(table_name):
    df = load_table(table_name)
    return df.columns.tolist()
if menu == "✏️CRUD Operations":
    st.header("✏️ CRUD Operations")

    table_name = st.selectbox(
        "Select Table",
        [
            "customers",
            "accounts",
            "transactions",
            "branches",
            "loan",
            "credit_cards",
            "support__tickets"
        ]
    )

    if table_name:
        columns = get_columns(table_name)

        action = st.radio(
            "Choose Operation",
            ["View", "Add", "Update", "Delete"],
            horizontal=True
        )

    
        if action == "View":
            df = load_table(table_name)
            st.dataframe(df, use_container_width=True)

    
        if action == "Add":
            st.subheader(f"Add New Record to {table_name}")

            new_data = {}
            for col in columns:
                new_data[col] = st.text_input(f"Enter {col}")

            if st.button("Insert Record"):
                cols = ", ".join(new_data.keys())
                vals = ", ".join([f":{c}" for c in new_data.keys()])

                query = text(f"""
                    INSERT INTO {table_name} ({cols})
                    VALUES ({vals})
                """)

                with engine.begin() as conn:
                    conn.execute(query, new_data)

                st.success("Record inserted successfully")

        
        if action == "Update":
            st.subheader("✏️ Update Record")

            pk_col = columns[0]   
            pk_val = st.text_input(f"Enter {pk_col} value")

            update_col = st.selectbox("Column to Update", columns)
            new_value = st.text_input("New Value")

            if st.button("Update Record"):
                query = text(f"""
                    UPDATE {table_name}
                    SET {update_col} = :new_val
                    WHERE {pk_col} = :pk
                """)

                with engine.begin() as conn:
                    conn.execute(query, {"new_val": new_value, "pk": pk_val})

                st.success("Record updated successfully")

        if action == "Delete":
            st.subheader("Delete Record")

            pk_col = columns[0]
            pk_val = st.text_input(f"Enter {pk_col} to Delete")

            if st.button("Delete Record"):
                query = text(f"""
                    DELETE FROM {table_name}
                    WHERE {pk_col} = :pk
                """)

                with engine.begin() as conn:
                    conn.execute(query, {"pk": pk_val})

                st.warning("Record deleted successfully")


                st.success(f"New Balance: ₹{new_balance}")

        
if menu ==  "💳Credit/Debit Simulation":

    st.subheader(" Bank Transaction")

    customer_id = st.text_input("Enter Customer ID")
    amount = st.number_input("Enter Amount", min_value=0.0, step=100.0)

    action = st.radio(
        "Choose Action",
        ["Check Balance", "Deposit", "Withdraw"]
    )

    if st.button("Submit"):
        with engine.begin() as conn:

            
            if action == "Check Balance":
                result = conn.execute(
                    text("""
                        SELECT account_balance
                        FROM accounts
                        WHERE customer_id = :cid
                    """),
                    {"cid": customer_id}
                ).fetchone()

                if result:
                    st.success(f" Balance: ₹{result[0]}")
                else:
                    st.error("Customer not found")

            
            elif action == "Deposit":
                conn.execute(
                    text("""
                        UPDATE accounts
                        SET account_balance = account_balance + :amt
                        WHERE customer_id = :cid
                    """),
                    {"amt": amount, "cid": customer_id}
                )
                st.success(" Amount Deposited")

            
            elif action == "Withdraw":
                balance = conn.execute(
                    text("""
                        SELECT account_balance
                        FROM accounts
                        WHERE customer_id = :cid
                    """),
                    {"cid": customer_id}
                ).fetchone()

                if not balance:
                    st.error("Customer not found")
                elif balance[0] < amount:
                    st.error("Insufficient Balance")
                else:
                    conn.execute(
                        text("""
                            UPDATE accounts
                            SET account_balance = account_balance - :amt
                            WHERE customer_id = :cid
                        """),
                        {"amt": amount, "cid": customer_id}
                    )
                    st.success("✅ Amount Withdrawn")

if menu == "📊Analytical Insights":

    st.title("📊 Analytical Insights")
    st.write("Select a question to view the result")

    queries = {
        "Q1: Customers exist per city & avg account balance": q1,
        "Q2: Account type with highest balance(savings,current,Loan,etc)": q2,
        "Q3: Top 10 customers by total account balance across all account types": q3,
        "Q4: Customers opened in 2023 with >1L balance": q4,
        "Q5: Total transaction volume(sum of amounts)by transaction type": q5,
        "Q6: Failed transactions occured for each transaction type": q6,
        "Q7: Total number of transactions per transactions type": q7,
        "Q8: Accounts with 5 or more high_value transactions above": q8,
        "Q9: Avg loan & interest rate by loan type(Personal,Auto,Home,etc)": q9,
        "Q10: Customers currently hold more than one active or approved loan": q10,
        "Q11: Top 5 customers with the highest outstanding loan amount": q11,
        "Q12: Avg loan amount per branch": q12,
        "Q13: Customers exist in each age group": q13,
        "Q14: Issue category have the longest average resolution time": q14,
        "Q15: support agents have resolved the most critical tickets with high customer ratings": q15
    }

    selected_question = st.selectbox(
        " Select a Question",
        list(queries.keys())
    )

    if selected_question:
        df = execute_query(queries[selected_question])
        st.dataframe(df, use_container_width=True)

if menu ==  "👤About Creator":
     st.title("👤About Creator")
     "***NAME:   YASHMA.A***" 
     "***EMAIL:  yashmaraj412000@gmail.com***"
     "***ROLE:   AIML***"
     










