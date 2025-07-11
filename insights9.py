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
    SELECT state, year, quarter, district, SUM(count) AS total_count, SUM(amount) AS total_amount
    FROM top_insurance
    GROUP BY state, year, quarter, district
""", conn)

top_insurance = df.sort_values(by="total_amount", ascending=False).head(10)

fig = px.bar(top_insurance, x="total_amount", y="district", color="state", orientation="h",
             title="📋 Top 10 Districts by Insurance Transaction Value")
fig.show()
st.subheader("9. 📋 Top Districts by Insurance Transactions")

df9 = pd.read_sql("""
    SELECT state, year, quarter, district, SUM(count) AS total_count, SUM(amount) AS total_amount
    FROM top_insurance
    GROUP BY state, year, quarter, district
""", conn)

top_insurance = df9.sort_values(by="total_amount", ascending=False).head(10)

fig = px.bar(top_insurance, x="total_amount", y="district", color="state", orientation="h",
             title="Top 10 Districts by Insurance Transaction Value")
st.plotly_chart(fig, use_container_width=True)
