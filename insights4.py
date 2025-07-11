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
    SELECT state, SUM(count) AS total_txns, SUM(amount) AS total_value
    FROM aggregated_transaction
    GROUP BY state
    ORDER BY total_value DESC
""", conn)

fig = px.bar(df, x="state", y="total_value", title="📈 Total Transaction Value by State")
fig.show()

st.subheader("4. 🌍 Market Expansion Opportunity")

df4 = pd.read_sql("""
    SELECT state, SUM(count) AS total_txns, SUM(amount) AS total_value
    FROM aggregated_transaction
    GROUP BY state
    ORDER BY total_value DESC
""", conn)

fig = px.choropleth(df4, geojson='https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json',
                    locations="state", locationmode="geojson-id", color="total_value",
                    scope="asia", title="Transaction Value Heatmap (India)", color_continuous_scale="Blues")
st.plotly_chart(fig, use_container_width=True)
