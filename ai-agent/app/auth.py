"""Простая авторизация через логин/пароль."""
import os
import hashlib

import streamlit as st


def _load_users() -> dict:
    """Читает пользователей из ENV."""
    raw = os.getenv("AUTH_USERS", "")
    users = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if not pair:
            continue
        if ":" in pair:
            login, pwd = pair.split(":", 1)
            users[login.strip()] = pwd.strip()
    return users


def _is_auth_enabled() -> bool:
    return os.getenv("AUTH_ENABLED", "false").lower() in ("true", "1", "yes")


def check_password(username: str, password: str) -> bool:
    """Проверяет логин/пароль."""
    users = _load_users()
    if not users:
        return False
    return users.get(username) == password


def login_form():
    """Отображает форму логина. Возвращает True если авторизован."""
    if not _is_auth_enabled():
        return True

    if st.session_state.get("authenticated"):
        return True

    # === СТРАНИЦА ЛОГИНА ===
    st.markdown(
        """
        <div style="max-width: 400px; margin: 100px auto; padding: 40px;
                    background: #161b22; border-radius: 12px;
                    border: 2px solid #4d9de0;
                    box-shadow: 0 4px 20px rgba(0,0,0,0.4);">
            <h2 style="color: #f0f6fc; text-align: center; margin: 0 0 20px 0;">
                🔐 Sales Analytics AI Agent
            </h2>
            <p style="color: #8b949e; text-align: center; margin: 0 0 30px 0;">
                Введите логин и пароль для входа
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        with st.form("login_form"):
            username = st.text_input("👤 Логин", key="login_username")
            password = st.text_input("🔑 Пароль", type="password", key="login_password")
            submit = st.form_submit_button("Войти", type="primary", use_container_width=True)

            if submit:
                if check_password(username, password):
                    st.session_state["authenticated"] = True
                    st.session_state["username"] = username
                    st.rerun()
                else:
                    st.error("❌ Неверный логин или пароль")

    return False


def logout_button():
    """Кнопка выхода в сайдбаре."""
    if not _is_auth_enabled():
        return

    username = st.session_state.get("username", "гость")

    st.sidebar.divider()
    st.sidebar.caption(f"👤 Вы вошли как: **{username}**")

    if st.sidebar.button("🚪 Выйти", use_container_width=True):
        for key in ("authenticated", "username"):
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()