
import pandas as pd
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password='Password',   # Change this
    database="ppp"
)
df = pd.read_sql("""
    SELECT state, year, quarter, district, SUM(registered_users) AS total_users
    FROM top_user
    GROUP BY state, year, quarter, district
""", conn)

top_districts = df.sort_values(by="total_users", ascending=False).head(10)

fig = px.bar(top_districts, x="total_users", y="district", color="state", orientation="h",
             title="🧑‍💻 Top 10 Districts by User Registrations")
fig.show()

st.subheader("8. 🧑‍💻 Top Districts by User Registrations")

df8 = pd.read_sql("""
    SELECT state, year, quarter, district, SUM(registered_users) AS total_users
    FROM top_user
    GROUP BY state, year, quarter, district
""", conn)

top_districts = df8.sort_values(by="total_users", ascending=False).head(10)

fig = px.bar(top_districts, x="total_users", y="district", color="state", orientation="h",
             title="Top 10 Districts by Registered Users")
st.plotly_chart(fig, use_container_width=True)
