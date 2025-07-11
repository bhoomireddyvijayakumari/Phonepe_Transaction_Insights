import pandas as pd
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt
import plotly.express as px

# MySQL connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password='Password', # Change this
    database="ppp"
)
df = pd.read_sql("""
    SELECT state, year, quarter, txn_type, SUM(count) AS total_txn_count, SUM(amount) AS total_txn_amount
    FROM aggregated_transaction
    GROUP BY state, year, quarter, txn_type
""", conn)

fig = px.line(df, x='quarter', y='total_txn_amount', color='txn_type',
              facet_col='state', facet_col_wrap=3,
              title='💸 Quarterly Transaction Amount by Type Across States')
fig.update_layout(height=800)
fig.show()
st.subheader("1. 📊 Decoding Transaction Dynamics")

df1 = pd.read_sql("""
    SELECT state, year, quarter, txn_type, SUM(count) AS total_txn_count, SUM(amount) AS total_txn_amount
    FROM aggregated_transaction
    GROUP BY state, year, quarter, txn_type
""", conn)

selected_txn_type = st.selectbox("Select Transaction Type", df1['txn_type'].unique())
filtered = df1[df1['txn_type'] == selected_txn_type]

fig = px.bar(filtered, x="quarter", y="total_txn_amount", color="state", barmode="group",
             title=f"{selected_txn_type} Amount Trends Across States")
st.plotly_chart(fig, use_container_width=True)
