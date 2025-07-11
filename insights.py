import pandas as pd
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password='Password',   # Change this
    database="ppp"
)

# Sample Query 1: Top 10 states by transaction amount
df_txn = pd.read_sql("""
    SELECT state, SUM(amount) as total_amount
    FROM aggregated_transaction
    GROUP BY state
    ORDER BY total_amount DESC
    LIMIT 10
""", conn)

plt.figure(figsize=(10,6))
sns.barplot(data=df_txn, x="total_amount", y="state", palette="mako")
plt.title("💰 Top 10 States by Total Transaction Amount")
plt.xlabel("Total Amount (₹)")
plt.ylabel("State")
plt.tight_layout()
plt.show()

# Sample Query 2: App opens vs Registered users
df_users = pd.read_sql("""
    SELECT state, SUM(registered_users) as total_users, SUM(app_opens) as total_opens
    FROM map_user
    GROUP BY state
    ORDER BY total_users DESC
""", conn)

plt.figure(figsize=(10,6))
sns.scatterplot(data=df_users, x="total_users", y="total_opens", hue="state", palette="tab10", legend=False)
plt.title("📱 App Opens vs Registered Users by State")
plt.xlabel("Registered Users")
plt.ylabel("App Opens")
plt.tight_layout()
plt.show()

conn.close()
