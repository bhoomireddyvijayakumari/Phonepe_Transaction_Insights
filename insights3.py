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
    SELECT state, year, quarter, insurance_type, SUM(count) AS total_policies, SUM(amount) AS total_value
    FROM aggregated_insurance
    GROUP BY state, year, quarter, insurance_type
""", conn)

fig = px.line(df, x="quarter", y="total_value", color="insurance_type",
              facet_col="state", facet_col_wrap=3,
              title="🛡️ Insurance Transaction Trends by State & Type")
fig.update_layout(height=900)
fig.show()
st.subheader("3. 🛡️ Insurance Penetration and Growth")

df3 = pd.read_sql("""
    SELECT state, year, quarter, insurance_type, SUM(count) AS total_policies, SUM(amount) AS total_value
    FROM aggregated_insurance
    GROUP BY state, year, quarter, insurance_type
""", conn)

state = st.selectbox("Select State", df3["state"].unique())
df_filtered = df3[df3["state"] == state]

fig = px.bar(df_filtered, x="quarter", y="total_value", color="insurance_type",
             title=f"Insurance Transactions in {state}")
st.plotly_chart(fig, use_container_width=True)
