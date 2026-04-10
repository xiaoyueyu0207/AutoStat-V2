import base64
import gzip
import importlib
import json
import pickle
import traceback

import lightgbm
import numpy as np
import pandas as pd
import streamlit as st
import xgboost
from sklearn.ensemble import (
    GradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
)
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from utils.sanitize_code import sanitize_code, to_json_serializable


def train_execution(agent):
    code = agent.load_code()
    df = agent.load_df()

    torch_module = importlib.import_module("torch")
    torchvision_module = importlib.import_module("torchvision")

    exec_ns = {
        "df": df,
        "np": np,
        "pd": pd,
        "torch": torch_module,
        "torchvision": torchvision_module,
        "train_test_split": train_test_split,
        "StandardScaler": StandardScaler,
        "LinearRegression": LinearRegression,
        "RandomForestRegressor": RandomForestRegressor,
        "GradientBoostingRegressor": GradientBoostingRegressor,
        "RandomForestClassifier": RandomForestClassifier,
        "xgboost": xgboost,
        "lightgbm": lightgbm,
    }

    try:
        with st.spinner("正在运行程序..."):
            exec(code, exec_ns)
    except Exception:
        st.error("已保存报错，请重新生成调试后的建模代码。")
        st.text(traceback.format_exc())
        agent.save_error(traceback.format_exc())
        modeling_code_gen(agent, debug=True)
        return

    result_dict = exec_ns.get("result_dict")
    if result_dict is None:
        st.error("脚本未写入 `result_dict`。请确保脚本末尾赋值 `result_dict`。")
        return

    artifacts = result_dict.get("artifacts", {})
    best_model_b64 = artifacts.pop("best_model_b64", None)
    result_dict.pop("artifact_warning", None)
    if not artifacts:
        result_dict.pop("artifacts", None)

    serializable = to_json_serializable(result_dict)
    try:
        result_json = json.dumps(serializable, ensure_ascii=False)
    except Exception:
        result_json = json.dumps(serializable, default=str, ensure_ascii=False)

    with st.spinner("正在格式化训练结果..."):
        formatted = agent.result_format_prompt(result_json)
        agent.save_modeling_result(formatted)

    if best_model_b64:
        gz_bytes = base64.b64decode(best_model_b64)
        try:
            agent.save_best_model_gz_bytes(gz_bytes)
            model_obj = pickle.loads(gzip.decompress(gz_bytes))
            agent.save_best_model(model_obj)
            st.success("最佳模型已加载到内存，可用于后续推理。")
        except Exception as exc:
            st.error(f"加载模型失败：{exc}")


def modeling_code_gen(agent, debug=False, auto=False) -> None:
    df = agent.load_df()
    suggest = agent.load_suggestion()
    summary_4 = st.session_state.get("summary_4") or st.session_state.get("modeling_summary_4")
    workflow_code = ""
    if isinstance(summary_4, dict):
        workflow_code = str(summary_4.get("code") or "").strip()

    chat_history = agent.load_memory()
    already_generated = any(
        entry["role"] == "assistant" and "训练脚本已更新，请重新运行代码！" in str(entry["content"])
        for entry in chat_history
    )

    if workflow_code:
        analyze_btn = st.button("🔡 生成模型建议代码", key="modeling_code")
        if analyze_btn:
            agent.save_code(workflow_code)
            st.chat_message("assistant").write("建模代码已从 `summary_4.code` 加载，请在下方执行。")
            agent.add_memory(
                {"role": "assistant", "content": "建模代码已从 `summary_4.code` 加载，请在下方执行。"}
            )
            st.rerun()
        return

    if suggest is not None:
        if debug or (auto and not already_generated):
            with st.spinner("建模 Agent 正在生成训练脚本..."):
                raw = agent.code_generation(df.head().to_string(), suggest)
                code = sanitize_code(raw)
                agent.save_code(code)
            st.chat_message("assistant").write("训练脚本已更新，请重新运行代码！")
            agent.add_memory({"role": "assistant", "content": "训练脚本已更新，请重新运行代码！"})
            st.rerun()

        analyze_btn = st.button("🔡 生成模型建议代码", key="modeling_code")
        if analyze_btn:
            with st.spinner("建模 Agent 正在生成训练脚本..."):
                raw = agent.code_generation(df.head().to_string(), suggest)
                code = sanitize_code(raw)
                agent.save_code(code)
            st.chat_message("assistant").write("训练脚本已更新，请重新运行代码！")
            agent.add_memory({"role": "assistant", "content": "训练脚本已更新，请重新运行代码！"})
            st.rerun()


def train_download_model(agent):
    model = agent.load_best_model_gz_bytes()
    if model is not None:
        st.download_button(
            label="⬇️ 下载最佳模型",
            data=model,
            file_name="best_model.pkl.gz",
            mime="application/gzip",
        )
