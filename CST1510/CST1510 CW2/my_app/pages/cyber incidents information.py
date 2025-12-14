import database as db
import streamlit as st

st.title("CYBER INCIDENTS INFORMATION")
st.header("List of all incidents")
df = db.get_all_incidents()
st.write(df)

dfChart = {
    "id" : df["incident_id"].tolist(),
"incident_type" : df["incident_type"].tolist()
}

sortBy = st.selectbox("sort by: ", ["date", "severity", "category", "status", "reported_by" ])
st.bar_chart(dfChart)

