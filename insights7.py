
import pandas as pd
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password='Password',  # Change this
    database="ppp"
)
df = pd.read_sql("""
    SELECT state, year, quarter, district, SUM(count) AS total_txns, SUM(amount) AS total_value
    FROM map_map
    GROUP BY state, year, quarter, district
""", conn)

top_10_districts = df.sort_values(by="total_value", ascending=False).head(10)

fig = px.bar(top_10_districts, x="total_value", y="district", color="state", orientation="h",
             title="🏙️ Top 10 Districts by Transaction Value")
fig.show()
st.subheader("7. 🏙️ Top Districts & States by Transaction Value")

df7 = pd.read_sql("""
    SELECT state, year, quarter, district, SUM(count) AS total_txns, SUM(amount) AS total_value
    FROM map_map
    GROUP BY state, year, quarter, district
""", conn)

top_districts = df7.sort_values(by="total_value", ascending=False).head(10)

fig = px.bar(top_districts, x="total_value", y="district", color="state", orientation="h",
             title="Top 10 Districts by Transaction Value")
st.plotly_chart(fig, use_container_width=True)
