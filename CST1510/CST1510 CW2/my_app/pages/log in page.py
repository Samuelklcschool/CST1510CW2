import streamlit as st
from database import login_user

st.title("LOGIN PAGE")
st.header("user info")
st.write("Please enter your username and password to log in")
username = st.text_input("username")
if st.session_state != False:
    st.session_state = False

if not st.session_state:
    password = st.text_input("Enter password:", type="password", key="password_input")
    check =login_user(username, password)
    st.write(check)
    if check == f"Welcome {username}":
        st.session_state = True
print("Access Granted")