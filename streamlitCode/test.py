import streamlit as st

st.write(" # 数据分区推荐系统 ")

genre = st.radio('选择工作负载类型', ('固定工作负载', '动态工作负载'))

file = st.file_uploader("Pick a file")

genre1 = st.radio('选择数据分区算法', ('GEA', 'PEA', 'GAA', 'PAA'))

