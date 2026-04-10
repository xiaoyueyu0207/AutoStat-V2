import sys, os
import streamlit as st
import warnings
from utils.coze_runtime import ensure_coze_session_defaults, COZE_REGION_OPTIONS


# 忽略警告
warnings.filterwarnings("ignore")
warnings.filterwarnings("ignore", message="missing ScriptRunContext")

# 基础路径配置
sys.path.append(os.path.dirname(__file__))

# 页面配置
st.set_page_config(
    page_title="Autostat",
    page_icon="🤖",
    layout="wide"
)

# 基本Agent类定义
class BaseAgent:
    def __init__(self):
        self.df = None
        self.memory = []
        self.code = None
        self.processed_df = None
        self.finish_auto_task = False
    
    def load_df(self):
        return self.df
    
    def add_df(self, df):
        self.df = df
    
    def load_memory(self):
        return self.memory
    
    def add_memory(self, entry):
        self.memory.append(entry)
    
    def load_code(self):
        return self.code

    def save_code(self, code):
        self.code = code
    
    def load_processed_df(self):
        return self.processed_df

    def save_processed_df(self, processed_df):
        self.processed_df = processed_df
    
    def clear_memory(self):
        self.memory = []
    
    def finish_auto(self):
        self.finish_auto_task = True

class DataLoadingAgent(BaseAgent):
    def __init__(self):
        super().__init__()
        self.file_names = []
        self.dfs = []
        self.loading_workflow_result = None
    
    def load_file_name(self):
        return self.file_names
    
    def save_file_name(self, name):
        self.file_names.append(name)
    
    def save_dfs(self, dfs):
        self.dfs = dfs
    
    def load_dfs(self):
        return self.dfs

    def save_loading_workflow_result(self, loading_workflow_result):
        self.loading_workflow_result = loading_workflow_result

    def load_loading_workflow_result(self):
        return self.loading_workflow_result
    
    def read_names_from_file(self, header_file, sample_df):
        return list(sample_df.columns)
    
    def do_data_description(self, df, user_input):
        return f"这是对'{user_input}'的响应"

class PlannerAgent(BaseAgent):
    def __init__(self):
        super().__init__()
        self.loading_auto = True
        self.prep_auto = False
        self.switched_prep = False
        self.vis_auto = False
        self.switched_vis = False
        self.modeling_auto = False
        self.switched_modeling = False
        self.report_auto = False
        self.plan = None
    
    def self_driving(self, df):
        self.loading_auto = True
        self.prep_auto = False
        self.vis_auto = False
        self.modeling_auto = False
        self.report_auto = False
        self.switched_prep = False
        self.switched_vis = False
        self.switched_modeling = False
        self.plan = "自动模式已启动，将执行完整的数据分析流程"
    
    def finish_loading_auto(self):
        self.loading_auto = False
        self.switched_prep = True
        self.prep_auto = True
    
    def finish_prep_auto(self):
        self.prep_auto = False
        self.switched_vis = True
        self.vis_auto = True
    
    def finish_vis_auto(self):
        self.vis_auto = False
        self.switched_modeling = True
        self.modeling_auto = True
    
    def finish_modeling_auto(self):
        self.modeling_auto = False
        self.report_auto = True

    def finish_report_auto(self):
        self.report_auto = False

    def stop_auto(self):
        self.loading_auto = False
        self.prep_auto = False
        self.vis_auto = False
        self.modeling_auto = False
        self.report_auto = False
        self.switched_prep = False
        self.switched_vis = False
        self.switched_modeling = False

class DataPreprocessAgent(BaseAgent):
    def __init__(self):
        super().__init__()
        self.preprocessing_suggestions = None
        self.user_input = None
        self.error = None
    
    def get_preprocessing_suggestions(self, user_input=None):
        return "这是预处理建议"
    
    def save_preprocessing_suggestions(self, suggestions):
        self.preprocessing_suggestions = suggestions
    
    def save_user_input(self, user_input):
        self.user_input = user_input
    
    def refine_suggestions(self, df_head):
        pass
    
    def save_error(self, error):
        self.error = error
    
    def code_generation(self, df_head, suggest):
        return "# 预处理代码示例\nimport pandas as pd\nimport numpy as np\nfrom sklearn.preprocessing import StandardScaler\n\n# 复制数据\nprocess_df = df.copy()\n\n# 标准化数值特征\nnumeric_cols = process_df.select_dtypes(include=['int64', 'float64']).columns\nscaler = StandardScaler()\nprocess_df[numeric_cols] = scaler.fit_transform(process_df[numeric_cols])\n\n# 处理缺失值\nprocess_df = process_df.fillna(process_df.mean())"
    
    def load_preprocessing_suggestions(self):
        return self.preprocessing_suggestions

class VisualizationAgent(BaseAgent):
    def __init__(self):
        super().__init__()
        self.visualization_suggestions = None
        self.fig_desc_list = []
        self.suggestion = None
        self.user_input = None
        self.color = None
        self.error = None
    
    def get_visualization_suggestions(self):
        return "这是可视化建议"
    
    def load_fig(self):
        return self.fig_desc_list
    
    def save_fig(self, fig_desc_list):
        self.fig_desc_list = fig_desc_list
    
    def save_suggestion(self, suggestion):
        self.suggestion = suggestion
    
    def load_suggestion(self):
        return self.suggestion
    
    def save_user_input(self, user_input):
        self.user_input = user_input
    
    def load_user_input(self):
        return self.user_input
    
    def save_color(self, color):
        self.color = color
    
    def load_color(self):
        return self.color

    def save_error(self, error):
        self.error = error

    def add_fig(self, fig, desc=None, base_fig=None):
        if base_fig is None:
            if hasattr(fig, "to_json"):
                try:
                    base_fig = fig.to_json()
                except Exception:
                    base_fig = fig
            else:
                base_fig = fig
        self.fig_desc_list.append({"fig": fig, "base_fig": base_fig, "desc": desc})

    def code_generation(self, df_head, suggest):
        return (
            "import plotly.express as px\n"
            "\n"
            "# 根据建议选择一列进行基础可视化\n"
            "plot_col = df.columns[0]\n"
            "fig = px.histogram(df, x=plot_col, title=f'{plot_col} 分布')\n"
            "fig_dict = {plot_col: fig}\n"
        )

    def desc_fig(self, fig, dtype_info):
        return f'图表已生成。字段类型概览: {dtype_info}'

class ModelingCodingAgent(BaseAgent):
    def __init__(self):
        super().__init__()
        self.model_suggestions = None
        self.suggestion = None
        self.user_input = None
        self.target = None
        self.user_selection = None
        self.history_train_code = None
        self.modeling_result = None
        self.inference_data = None
        self.inference_processed_df = None
        self.inference_code = None
        self.best_model = None
        self.best_model_gz_bytes = None
        self.error = None
    
    def get_model_suggestions(self):
        return "这是建模建议"
    
    def get_model_suggestion(self, user_input=None):
        return "这是建模建议"
    
    def save_suggestion(self, suggestion):
        self.suggestion = suggestion
    
    def load_suggestion(self):
        return self.suggestion
    
    def save_user_input(self, user_input):
        self.user_input = user_input

    def load_user_input(self):
        return self.user_input

    def save_target(self, target):
        self.target = target

    def load_target(self):
        return self.target

    def save_user_selection(self, user_selection):
        self.user_selection = user_selection

    def load_user_selection(self):
        return self.user_selection

    def save_history_train_code(self, history_train_code):
        self.history_train_code = history_train_code

    def load_history_train_code(self):
        return self.history_train_code

    def load_modeling_result(self):
        return self.modeling_result
    
    def save_modeling_result(self, modeling_result):
        self.modeling_result = modeling_result
    
    def load_inference_data(self):
        return self.inference_data
    
    def save_inference_data(self, inference_data):
        self.inference_data = inference_data
    
    def load_inference_processed_df(self):
        return self.inference_processed_df
    
    def save_inference_processed_df(self, inference_processed_df):
        self.inference_processed_df = inference_processed_df
    
    def load_inference_code(self):
        return self.inference_code
    
    def save_inference_code(self, inference_code):
        self.inference_code = inference_code
    
    def load_best_model(self):
        return self.best_model
    
    def save_best_model(self, best_model):
        self.best_model = best_model

    def load_best_model_gz_bytes(self):
        return self.best_model_gz_bytes

    def save_best_model_gz_bytes(self, best_model_gz_bytes):
        self.best_model_gz_bytes = best_model_gz_bytes

    def save_error(self, error):
        self.error = error

    def load_error(self):
        return self.error

    def result_format_prompt(self, result_json):
        return f"```json\n{result_json}\n```"
    
    def code_generation(self, df_head, selected_models):
        return '''# 建模代码示例
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

# 假设目标列是最后一列
target_col = df.columns[-1]
X = df.drop(target_col, axis=1)
y = df[target_col]

# 分割数据
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 训练模型
models = {}
models['Linear Regression'] = LinearRegression()
models['Random Forest'] = RandomForestRegressor()

for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    print(f"{name} MSE: {mse:.4f}")

# 保存最佳模型
best_model = min(models, key=lambda x: mean_squared_error(y_test, models[x].predict(X_test)))
print(f"最佳模型: {best_model}")'''
    
    def code_generation_for_inference(self, code, inference_data_head):
        return "# 推断代码示例\nimport pandas as pd\nimport numpy as np\n\n# 加载模型\n# 这里假设模型已经保存\n# model = joblib.load('best_model.joblib')\n\n# 进行预测\n# predictions = model.predict(inference_data)\n# print(predictions)"
    
    def refine_suggestions(self):
        pass

class ReportAgent(BaseAgent):
    def __init__(self):
        super().__init__()
        self.report_content = None
        self.report = None
        self.report_workflow_result = None
        self.report_format = "Word"
        self.gen_mode = "并行"
        self.outline_length = "标准"
        self.outline = None
        self.word = None
        self.html = None
        self.markdown = None
        self.user_input = None
    
    def generate_report(self):
        return "这是报告内容"
    
    def load_report_format(self):
        return self.report_format
    
    def save_report_format(self, report_format):
        self.report_format = report_format
    
    def load_gen_mode(self):
        return self.gen_mode
    
    def save_gen_mode(self, gen_mode):
        self.gen_mode = gen_mode
    
    def load_outline_length(self):
        return self.outline_length
    
    def save_outline_length(self, outline_length):
        self.outline_length = outline_length
    
    def load_outline(self):
        return self.outline
    
    def save_outline(self, outline):
        self.outline = outline
    
    def load_word(self):
        return self.word
    
    def save_word(self, word):
        self.word = word
    
    def load_html(self):
        return self.html
    
    def save_html(self, html):
        self.html = html
    
    def load_markdown(self):
        return self.markdown
    
    def save_markdown(self, markdown):
        self.markdown = markdown
    
    def save_user_input(self, user_input):
        self.user_input = user_input

    def load_user_input(self):
        return self.user_input

    def load_report(self):
        return self.report

    def save_report(self, report):
        self.report = report

    def load_report_content(self):
        return self.report_content

    def save_report_content(self, report_content):
        self.report_content = report_content

    def load_report_workflow_result(self):
        return self.report_workflow_result

    def save_report_workflow_result(self, report_workflow_result):
        self.report_workflow_result = report_workflow_result
    
    def generate_toc_from_summary(self, summaries):
        return "# 报告目录\n\n## 1. 数据导入\n## 2. 数据预处理\n## 3. 数据可视化\n## 4. 建模分析\n## 5. 结论与建议"
    
    def summary_html(self):
        return "数据可视化摘要"
    
    def summary_word(self):
        return "数据可视化摘要"

class Retriever:
    def __init__(self):
        self.learned_docs = []
    
    def add_uploaded_files(self, files):
        for file in files:
            self.learned_docs.append(file.name)
        return len(files)

def init_session_state():
    """初始化会话状态，移除复杂的本地 API 配置逻辑"""
    
    if 'auto_mode' not in st.session_state:
        st.session_state.auto_mode = False
    
    # 初始化各个agent
    if 'data_loading_agent' not in st.session_state:
        st.session_state.data_loading_agent = DataLoadingAgent()
    elif not hasattr(st.session_state.data_loading_agent, "load_loading_workflow_result"):
        old_agent = st.session_state.data_loading_agent
        new_agent = DataLoadingAgent()
        if hasattr(old_agent, "__dict__"):
            new_agent.__dict__.update(old_agent.__dict__)
        st.session_state.data_loading_agent = new_agent
    
    if 'planner_agent' not in st.session_state:
        st.session_state.planner_agent = PlannerAgent()
    
    if 'data_preprocess_agent' not in st.session_state:
        st.session_state.data_preprocess_agent = DataPreprocessAgent()
    
    if 'visualization_agent' not in st.session_state:
        st.session_state.visualization_agent = VisualizationAgent()
    
    if 'modeling_coding_agent' not in st.session_state:
        st.session_state.modeling_coding_agent = ModelingCodingAgent()
    elif (
        not hasattr(st.session_state.modeling_coding_agent, "load_target")
        or not hasattr(st.session_state.modeling_coding_agent, "save_history_train_code")
    ):
        old_agent = st.session_state.modeling_coding_agent
        new_agent = ModelingCodingAgent()
        if hasattr(old_agent, "__dict__"):
            new_agent.__dict__.update(old_agent.__dict__)
        st.session_state.modeling_coding_agent = new_agent
    
    if 'report_agent' not in st.session_state:
        st.session_state.report_agent = ReportAgent()
    elif (
        not hasattr(st.session_state.report_agent, "load_report_content")
        or not hasattr(st.session_state.report_agent, "load_report_workflow_result")
        or not hasattr(st.session_state.report_agent, "load_user_input")
    ):
        old_agent = st.session_state.report_agent
        new_agent = ReportAgent()
        if hasattr(old_agent, "__dict__"):
            new_agent.__dict__.update(old_agent.__dict__)
        st.session_state.report_agent = new_agent
    
    if 'retriever' not in st.session_state:
        st.session_state.retriever = Retriever()
    
    if 'additional_preference' not in st.session_state:
        st.session_state.additional_preference = None
    
    if 'preference_select' not in st.session_state:
        st.session_state.preference_select = None

    # Coze 鉴权与版本配置
    ensure_coze_session_defaults()


def run_app():
    """渲染 Streamlit 应用程序主入口"""
    init_session_state()

    def _reset_coze_auth() -> None:
        st.session_state.coze_api_key = ""
        st.session_state.coze_region = COZE_REGION_OPTIONS[0]
        st.session_state.coze_auth_saved = False

    def _reset_auto_agent_flags() -> None:
        for agent_key in (
            "data_loading_agent",
            "data_preprocess_agent",
            "visualization_agent",
            "modeling_coding_agent",
            "report_agent",
        ):
            agent = st.session_state.get(agent_key)
            if agent is not None:
                agent.finish_auto_task = False

    def _clear_auto_run_artifacts() -> None:
        for key in (
            "loading_workflow_result",
            "summary_1",
            "summary_2",
            "summary_3",
            "summary_4",
            "tu_title",
            "summary_1_title",
            "summary_1_desc",
            "summary_1_df",
            "abstract_1",
            "abstract_2",
            "abstract_3",
            "abstract_4",
            "suggestion",
            "prep_code_visible",
            "prep_result_from_summary_2",
            "viz_workflow_result",
            "viz_suggestion",
            "full",
            "visual_recommendatio",
            "final_code",
            "modeling_workflow_result",
            "modeling_suggestion",
            "model_suggestion",
            "modeling_summary_4",
            "modeling_abstract_4",
            "modeling_result_from_summary_4",
            "report_title",
            "history_train_code_input",
            "history_train_code_reset_pending",
        ):
            st.session_state.pop(key, None)

        load_agent = st.session_state.get("data_loading_agent")
        if load_agent is not None:
            load_agent.clear_memory()
            load_agent.code = None
            load_agent.processed_df = None
            load_agent.loading_workflow_result = None

        preproc_agent = st.session_state.get("data_preprocess_agent")
        if preproc_agent is not None:
            preproc_agent.clear_memory()
            preproc_agent.code = None
            preproc_agent.processed_df = None
            preproc_agent.preprocessing_suggestions = None
            preproc_agent.user_input = None
            preproc_agent.error = None

        viz_agent = st.session_state.get("visualization_agent")
        if viz_agent is not None:
            viz_agent.clear_memory()
            viz_agent.code = None
            viz_agent.suggestion = None
            viz_agent.user_input = None
            viz_agent.error = None
            viz_agent.fig_desc_list = []

        modeling_agent = st.session_state.get("modeling_coding_agent")
        if modeling_agent is not None:
            modeling_agent.clear_memory()
            modeling_agent.code = None
            modeling_agent.suggestion = None
            modeling_agent.user_input = None
            modeling_agent.target = None
            modeling_agent.user_selection = None
            modeling_agent.history_train_code = None
            modeling_agent.modeling_result = None
            modeling_agent.inference_data = None
            modeling_agent.inference_processed_df = None
            modeling_agent.inference_code = None
            modeling_agent.best_model = None
            modeling_agent.best_model_gz_bytes = None
            modeling_agent.error = None

        report_agent = st.session_state.get("report_agent")
        if report_agent is not None:
            report_agent.clear_memory()
            report_agent.report_content = None
            report_agent.report = None
            report_agent.report_workflow_result = None
            report_agent.outline = None
            report_agent.word = None
            report_agent.html = None
            report_agent.markdown = None
            report_agent.user_input = None

    def _start_auto_mode() -> None:
        _clear_auto_run_artifacts()
        st.session_state.auto_mode = True
        st.session_state.planner_agent.self_driving(st.session_state.data_loading_agent.load_df())
        _reset_auto_agent_flags()

    def _stop_auto_mode() -> None:
        st.session_state.auto_mode = False
        st.session_state.planner_agent.stop_auto()
        _reset_auto_agent_flags()

    # --- 侧边栏布局 ---
    with st.sidebar:
        st.markdown(
            """
            <style>
            section[data-testid="stSidebar"] hr {
                margin-top: 0.14rem !important;
                margin-bottom: 0.14rem !important;
            }
            section[data-testid="stSidebar"] div[data-testid="stExpander"] {
                margin-top: -0.24rem !important;
                margin-bottom: 0.22rem !important;
            }
            section[data-testid="stSidebar"] [data-testid="stSidebarNav"]::before {
                content: "AutoSTAT";
                display: block;
                margin: 0.06rem 0 0.28rem 0;
                color: #1e3a8a;
                font-weight: 700;
                font-size: 1.55rem;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        with st.expander("Coze 授权配置", expanded=True):
            st.selectbox(
                "Coze 版本",
                options=COZE_REGION_OPTIONS,
                key="coze_region",
            )
            st.text_input(
                "Coze Personal Access Token",
                key="coze_api_key",
                type="password",
                placeholder="pat_xxx",
            )

            st.markdown(
                """
                <style>
                .st-key-save_coze_auth button,
                .st-key-clear_coze_auth button {
                    font-size: 0.9rem !important;
                    white-space: nowrap !important;
                }
                .coze-status {
                    font-size: 1.05rem;
                    line-height: 1.45;
                    border-radius: 10px;
                    padding: 0.45rem 0.6rem;
                    margin-top: 0.3rem;
                }
                .coze-status-ok {
                    color: #1e3a8a;
                    background: #dbeafe;
                }
                .coze-status-warn {
                    color: #1e3a8a;
                    background: #dbeafe;
                }
                </style>
                """,
                unsafe_allow_html=True,
            )

            b1, b2 = st.columns([1.2, 1], gap="small")
            with b1:
                save_clicked = st.button("保存授权", key="save_coze_auth", use_container_width=True, type="primary")
            with b2:
                st.button(
                    "清空",
                    key="clear_coze_auth",
                    use_container_width=True,
                    on_click=_reset_coze_auth,
                )

            if save_clicked:
                st.session_state.coze_auth_saved = True

            if st.session_state.get("coze_auth_saved"):
                st.markdown('<div class="coze-status coze-status-ok">Coze 授权已保存</div>', unsafe_allow_html=True)
            elif not st.session_state.get("coze_api_key"):
                st.markdown('<div class="coze-status coze-status-warn">状态：未授权（将使用当前默认配置）</div>', unsafe_allow_html=True)

            st.write("")
            coze_endpoint = "https://api.coze.com/v1/workflow/run"
            st.caption(f"当前接口：{coze_endpoint}")

        st.divider()
        st.write("")

        # 清空数据按钮
        if st.button("🧹 清空所有数据", use_container_width=True):
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()

        # 自动模式逻辑（保留核心流程控制）
        df = st.session_state.data_loading_agent.load_df()
        if not st.session_state.auto_mode:
            if st.button("🚗 开启自动模式", use_container_width=True, type="primary"):
                _start_auto_mode()
                st.switch_page("workflow/dataloading/dataloading_render.py")
        else:
            if st.button("❌ 结束自动模式", use_container_width=True):
                _stop_auto_mode()
                st.rerun()

        # 检查logo目录是否存在
        if os.path.exists("logo/logo_big.png"):
            st.image("logo/logo_big.png", use_container_width=True)

    # --- 页面导航 (保持模块化) ---
    pages = {
        "分析流程": [
            st.Page("workflow/dataloading/dataloading_render.py", title="📥 数据导入"),
            st.Page("workflow/preprocessing/preprocessing_render.py", title="🛠️ 数据预处理"),
            st.Page("workflow/visualization/viz_render.py", title="📊 数据可视化"),
            st.Page("workflow/modeling/modeling_render.py", title="🧠 建模分析"),
            st.Page("workflow/report/report_render.py", title="📝 报告生成"),
        ],
        "系统配置": [
            st.Page("workflow/preference/pref_render.py", title="⚙️ 偏好设置"),
        ]
    }
    pg = st.navigation(pages, position="sidebar")
    pg.run()

if __name__ == "__main__":
    run_app()
