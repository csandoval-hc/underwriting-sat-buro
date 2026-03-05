import streamlit as st

def require_login() -> str:
    if "auth_user" in st.session_state and st.session_state["auth_user"]:
        return st.session_state["auth_user"]

    st.title("Login")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Entrar", type="primary", use_container_width=True):
        # Try different ways Streamlit might store your secrets
        users = {}
        
        if "auth" in st.secrets and "users" in st.secrets["auth"]:
            # Case for [auth.users]
            users = st.secrets["auth"]["users"]
        elif "USERS" in st.secrets:
            # Case for [USERS]
            users = st.secrets["USERS"]
            
        if not users:
            st.error("No se encontró la configuración de usuarios. Revisa que en Secrets tengas el formato [auth.users]")
            st.stop()

        # Get the stored password for the typed username
        # Use .get() to handle case where user doesn't exist
        stored_password = users.get(username)

        # Check plain text match
        if stored_password and str(stored_password) == password:
            st.session_state["auth_user"] = username
            # Success!
            st.rerun()
        else:
            st.error("Usuario o contraseña incorrectos.")
            st.stop()

    st.stop()

def logout_button() -> None:
    if st.button("Cerrar sesión", use_container_width=True):
        for key in list(st.session_state.keys()):
            st.session_state.pop(key)
        st.rerun()
