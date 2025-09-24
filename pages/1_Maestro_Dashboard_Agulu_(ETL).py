import streamlit as st
import plotly.express as px
from helpers import read_population_data, transform_df

st.set_page_config(
    page_icon=":sloth:",
    layout="wide",
)

with st.sidebar:
    st.sidebar.page_link("app.py", label="Home", icon="🦧")
    st.sidebar.page_link("pages/1_Maestro_Dashboard_Agulu_(ETL).py", label="Maestro_Dashboard_Agulu_(ETL)", icon="🪼")
    st.sidebar.page_link("pages/2_Ifem_The_Study_Buddy_(AI).py", label="Ifem_The_Study_Buddy_(AI)", icon="🦩")
    st.sidebar.page_link("pages/3_Nnanna_The_Text_to_SQL_Alchemist_(AI).py", label="Nnanna_The_Text_to_SQL_Alchemist_(AI)", icon="🐿")
    st.divider()

st.markdown("""
    <style>
        .block-container {
            padding-top: 1rem; 
            padding-bottom: 0rem;
            padding-left: 5rem;
            padding-right: 5rem;
        }
    </style>
    """, unsafe_allow_html=True)

st.title("Canada Population Dashboard")
all_df_agGrp, all_df_Summary = transform_df()

# st.dataframe(all_df_agGrp)

# show age group graph for all provinces against / and federal
st.sidebar.header("Filter by Region:")
Region = st.sidebar.multiselect(
    "Select Region(s) below to view :",
    options=all_df_Summary['REGION_NAME'].unique(),
    default=all_df_Summary['REGION_NAME'].unique()[0]
    # default=['Canada']
)

Age_Group = st.sidebar.multiselect(
    "Select Age Group(s) below to view :",
    options=all_df_agGrp['DIMENSIONS'].unique(),
    default=all_df_agGrp['DIMENSIONS'].unique()
)

df_selection = all_df_agGrp.query("REGION_NAME == @Region & DIMENSIONS == @Age_Group")
total_pop = all_df_Summary.query("REGION_NAME == @Region & DIMENSIONS == 'Population, 2021'")
df_summ_selection = all_df_Summary.query(
    "REGION_NAME == @Region & DIMENSIONS == 'Population density per square kilometre'"
)
df_summ_selection_chg = all_df_Summary.query(
    "REGION_NAME == @Region & DIMENSIONS == 'Population percentage change, 2016 to 2021'"
)

fig_pop_age_grp = px.bar(df_selection, x="REGION_NAME", y="TOTAL_COUNT", color="DIMENSIONS",
                         title="Population by Age Group",
                         labels={"REGION_NAME": "Region", "TOTAL_COUNT": "Total Population", "DIMENSIONS": "Age "
                                                                                                           "Group"},
                         barmode='group',
                         # color_discrete_sequence=px.colors.qualitative.Vivid,
                         color_discrete_sequence=px.colors.qualitative.G10,
                         height=500,
                         template='plotly_white'
                         )

st.plotly_chart(fig_pop_age_grp, use_container_width=True)

# st.markdown("---")

total_population = int(total_pop['TOTAL_COUNT'].sum())
pop_density = int(df_summ_selection['TOTAL_COUNT'].sum())
pop_chg_perc = int(df_summ_selection_chg['TOTAL_COUNT'].sum())

l_col, mid_col, r_col = st.columns(3)

with l_col:
    st.subheader("Total Population:")
    st.subheader(f"{total_population:,}")
with mid_col:
    st.subheader("Population Density per sq.km:")
    st.subheader(f"{pop_density:,} per sq.km")
with r_col:
    st.subheader("Population Change (2016-2021):")
    st.subheader(f"{pop_chg_perc:,}%")

st.markdown("---")

st.dataframe(df_selection, height=250, hide_index=True)
