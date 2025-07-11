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
    SELECT state, brand, SUM(count) AS total_users, ROUND(AVG(percentage)*100, 2) AS avg_share
    FROM aggregated_user
    GROUP BY state, brand
""", conn)

top_states = df.groupby("state")["total_users"].sum().nlargest(5).index.tolist()
filtered = df[df["state"].isin(top_states)]

fig = px.bar(filtered, x="brand", y="total_users", color="state", barmode="group",
             title="📱 Device Brand Usage by State")
fig.show()
st.subheader("2. 📱 Device Dominance & User Engagement")

df2 = pd.read_sql("""
    SELECT state, brand, SUM(count) AS total_users, ROUND(AVG(percentage)*100, 2) AS avg_share
    FROM aggregated_user
    GROUP BY state, brand
""", conn)

selected_state = st.selectbox("Select State", df2["state"].unique())
state_df = df2[df2["state"] == selected_state]

fig = px.pie(state_df, names='brand', values='total_users',
             title=f"Device Brand Share in {selected_state}")
st.plotly_chart(fig, use_container_width=True)
