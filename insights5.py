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
    SELECT state, year, quarter, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens
    FROM map_user
    GROUP BY state, year, quarter
""", conn)

fig = px.scatter(df, x="total_users", y="total_opens", color="state",
                 size="total_opens", title="📱 User Engagement: App Opens vs Registrations")
fig.show()

st.subheader("5. 📈 User Engagement & Growth Strategy")

df5 = pd.read_sql("""
    SELECT state, year, quarter, SUM(registered_users) AS total_users, SUM(app_opens) AS total_opens
    FROM map_user
    GROUP BY state, year, quarter
""", conn)

fig = px.scatter(df5, x="total_users", y="total_opens", color="state", size="total_opens",
                 title="App Opens vs Registered Users by State")
st.plotly_chart(fig, use_container_width=True)
