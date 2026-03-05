import streamlit as st

def require_login() -> str:
    if "auth_user" in st.session_state and st.session_state["auth_user"]:
        return st.session_state["auth_user"]

    st.title("Login")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Entrar", type="primary", use_container_width=True):
        # Access the nested structure [auth.users] from secrets
        auth_secrets = st.secrets.get("auth", {})
        users = auth_secrets.get("users", {})

        if not isinstance(users, dict) or not users:
            st.error("No hay usuarios configurados en Secrets ([auth.users]).")
            st.stop()

        stored_password = users.get(username)
        
        # Check if user exists and password matches plain text
        if stored_password and str(stored_password) == password:
            st.session_state["auth_user"] = username
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")
            st.stop()

    st.stop()

def logout_button() -> None:
    if st.button("Cerrar sesión", use_container_width=True):
        st.session_state.pop("auth_user", None)
        st.rerun()
