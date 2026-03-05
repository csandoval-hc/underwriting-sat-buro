import hashlib
import hmac
import streamlit as st


def _hash_password(password: str) -> str:
    # deterministic SHA256 (good enough for internal gating; bcrypt is better but adds deps)
    return hashlib.sha256(password.encode("utf-8")).hexdigest()


def _verify_password(password: str, password_hash: str) -> bool:
    return hmac.compare_digest(_hash_password(password), password_hash)


def require_login() -> str:
    """
    Requires st.secrets to contain:
      USERS = { "username": "<sha256_hex_hash>", ... }
    """
    if "auth_user" in st.session_state and st.session_state["auth_user"]:
        return st.session_state["auth_user"]

    st.title("Login")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Entrar", type="primary", use_container_width=True):
        users = st.secrets.get("USERS", {})
        if not isinstance(users, dict) or not users:
            st.error("No hay usuarios configurados en Secrets (USERS).")
            st.stop()

        stored_hash = users.get(username)
        if not stored_hash:
            st.error("Usuario o contraseña incorrectos.")
            st.stop()

        if not _verify_password(password, str(stored_hash)):
            st.error("Usuario o contraseña incorrectos.")
            st.stop()

        st.session_state["auth_user"] = username
        st.rerun()

    st.stop()


def logout_button() -> None:
    if st.button("Cerrar sesión", use_container_width=True):
        st.session_state.pop("auth_user", None)
        st.rerun()


def password_hash_tool(password: str) -> str:
    """Helper if you want to generate hashes locally/import it elsewhere."""
    return _hash_password(password)
