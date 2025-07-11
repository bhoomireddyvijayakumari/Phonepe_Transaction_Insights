
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
    SELECT state, year, quarter, district, SUM(count) AS total_insurance_txns, SUM(amount) AS total_amount
    FROM map_insurance
    GROUP BY state, year, quarter, district
""", conn)

top_districts = df.groupby("district")["total_amount"].sum().nlargest(10).reset_index()

fig = px.bar(top_districts, x="total_amount", y="district", orientation="h",
             title="🏥 Top 10 Districts by Insurance Transaction Value", color="total_amount")
fig.show()
st.subheader("6. 🏥 Insurance Engagement by District")

df6 = pd.read_sql("""
    SELECT state, year, quarter, district, SUM(count) AS total_insurance_txns, SUM(amount) AS total_amount
    FROM map_insurance
    GROUP BY state, year, quarter, district
""", conn)

top_districts = df6.groupby("district")["total_amount"].sum().nlargest(10).reset_index()

fig = px.bar(top_districts, x="total_amount", y="district", orientation="h",
             title="Top 10 Districts by Insurance Transaction Value", color="total_amount")
st.plotly_chart(fig, use_container_width=True)

