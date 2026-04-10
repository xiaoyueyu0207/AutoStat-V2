import streamlit as st

COZE_REGION_INTL = "国际版 Coze.com"
COZE_REGION_CN = "国内版 coze.cn"

COZE_REGION_OPTIONS = [COZE_REGION_INTL]
COZE_REGION_TO_URL = {
    COZE_REGION_INTL: "https://api.coze.com/v1/workflow/run",
    COZE_REGION_CN: "https://api.coze.cn/v1/workflow/run",
}


def ensure_coze_session_defaults() -> None:
    if "coze_region" not in st.session_state:
        st.session_state.coze_region = COZE_REGION_INTL
    if "coze_api_key" not in st.session_state:
        st.session_state.coze_api_key = ""
    if "coze_auth_saved" not in st.session_state:
        st.session_state.coze_auth_saved = False


def resolve_coze_runtime(default_api_key: str = "", default_url: str = "") -> dict:
    region = COZE_REGION_INTL
    api_key = (default_api_key or "").strip() or (st.session_state.get("coze_api_key", "") or "").strip()
    coze_url = default_url or COZE_REGION_TO_URL[COZE_REGION_INTL]

    return {
        "region": region,
        "api_key": api_key,
        "coze_url": coze_url,
    }
