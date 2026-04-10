import time
import re
import html
import base64
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objs as go
import plotly.io as pio
import streamlit as st
import streamlit.components.v1 as components
import streamlit_antd_components as sac
from bs4 import BeautifulSoup, Tag
from cozepy import Coze, TokenAuth, WorkflowEventType

from utils.coze_runtime import resolve_coze_runtime
from workflow.visualization.viz_coding import (
    execute_visualization_code_once,
    generate_visualization_code_once,
)
from workflow.report.report_content_utils import (
    _split_markdown_heading_lines,
    build_markdown_preview_from_html,
    build_docx_from_html,
    build_docx_from_markdown,
    extract_report_html,
    extract_report_markdown,
    extract_report_text,
    extract_report_word_bytes,
    find_first_nested_field,
    html_to_markdown,
    maybe_json_loads,
    normalize_trailing_punctuation_before_figure_placeholder,
    normalize_toc_list,
    stringify_string,
)

DEFAULT_COZE_API_KEY = "pat_89vvp88v1WqjTMtIbHMncgz84FgjTS9Qlk5SAaWqcX8msiKyVcWctIwzqSi7wgXF"
COZE_SPACE_ID = "7594748927577554949"
WORKFLOW_ID = "7619618199978508341"
WORD_REPORT_WORKFLOW_ID = "7619618317418446901"
BOT_ID = "7595403958269575173"
MAX_POLL_SECONDS = 1800
REPORT_WORKFLOW_OUTPUT_FIELDS = (
    "title",
    "add_preference",
    "preference_select",
    "selected_full_conten",
    "toc_text",
    "load_abstract",
    "preproc_abstract",
    "visual_abstract",
    "coding_abstract",
)
FIG_PLACEHOLDER_PATTERN = r"(?<![A-Za-z0-9_])[\[\uFF3B\u3010]?\s*FIG\s*:?\s*(?:\d+)\s*[\]\uFF3D\u3011]?(?![A-Za-z0-9_])"
FIG_PLACEHOLDER_CAPTURE_PATTERN = r"(?<![A-Za-z0-9_])[\[\uFF3B\u3010]?\s*FIG\s*:?\s*(\d+)\s*[\]\uFF3D\u3011]?(?![A-Za-z0-9_])"
def _resolve_coze_base_url(coze_url: str) -> str:
    if "api.coze.cn" in coze_url:
        return "https://api.coze.cn"
    return "https://api.coze.com"


def _resolve_loading_field(load_agent, field_name: str, default: Any) -> Any:
    stored_value = st.session_state.get(field_name)
    if stored_value is not None:
        return stored_value

    memory_entries = getattr(load_agent, "load_memory", lambda: [])()
    for entry in reversed(memory_entries):
        content = entry.get("content") if isinstance(entry, dict) else None
        if isinstance(content, dict) and field_name in content:
            return content.get(field_name)

    return default


def _normalize_report_workflow_result(result: Any) -> dict[str, Any] | None:
    parsed_result = maybe_json_loads(result)

    if isinstance(parsed_result, dict):
        return parsed_result

    if isinstance(parsed_result, str):
        reparsed = maybe_json_loads(parsed_result)
        if isinstance(reparsed, dict):
            return reparsed

    return None


def _merge_report_workflow_results(results: list[dict[str, Any]]) -> dict[str, Any] | None:
    merged_result: dict[str, Any] = {}

    for result in results:
        if isinstance(result, dict):
            merged_result.update(result)

    return merged_result or None


def _extract_report_workflow_outputs(workflow_result: dict[str, Any]) -> dict[str, Any]:
    outputs: dict[str, Any] = {}

    for field_name in REPORT_WORKFLOW_OUTPUT_FIELDS:
        value = find_first_nested_field(workflow_result, [field_name])
        if value is not None:
            outputs[field_name] = value

    return outputs


def _extract_toc_text_from_result(workflow_result: dict[str, Any]) -> str:
    return stringify_string(find_first_nested_field(workflow_result, ["toc_text"])).replace("\\r\\n", "\n").replace("\\n", "\n")


def _normalize_multiline_text(value: Any) -> str:
    if isinstance(value, str):
        return stringify_string(value).replace("\\r\\n", "\n").replace("\\n", "\n")
    return "\n".join(normalize_toc_list(value))


def _normalize_visualization_titles(raw_titles: Any) -> list[str]:
    parsed_titles = maybe_json_loads(raw_titles)

    if parsed_titles is None:
        return []

    if isinstance(parsed_titles, str):
        text = stringify_string(parsed_titles)
        return [line.strip() for line in text.splitlines() if line.strip()]

    if isinstance(parsed_titles, dict):
        for key in ("tu_title", "titles", "data", "items"):
            if key in parsed_titles:
                return _normalize_visualization_titles(parsed_titles.get(key))
        return [
            str(value).strip()
            for value in parsed_titles.values()
            if str(value).strip()
        ]

    if isinstance(parsed_titles, list):
        normalized_titles: list[str] = []
        for item in parsed_titles:
            if isinstance(item, dict):
                candidate = (
                    item.get("tu_title")
                    or item.get("name")
                    or item.get("label")
                    or item.get("text")
                )
            else:
                candidate = item

            candidate_text = str(candidate).strip() if candidate is not None else ""
            if candidate_text:
                normalized_titles.append(candidate_text)
        return normalized_titles

    fallback = str(parsed_titles).strip()
    return [fallback] if fallback else []


def _build_figure_caption(display_number: int, fig_index: int, title_items: list[str]) -> str:
    title_text = ""
    if 0 <= fig_index < len(title_items):
        title_text = title_items[fig_index].strip()

    return f"图{display_number} {title_text}".strip()


def _extract_report_title(workflow_result: Any) -> str:
    title_value = find_first_nested_field(workflow_result, ["title"])
    parsed_title = maybe_json_loads(title_value)

    if isinstance(parsed_title, dict):
        for key in ("title", "text", "name", "label", "content"):
            candidate = stringify_string(parsed_title.get(key))
            if candidate:
                return candidate

    if isinstance(parsed_title, list):
        for item in parsed_title:
            candidate = stringify_string(item)
            if candidate:
                return candidate

    title_text = stringify_string(parsed_title)
    if title_text:
        return title_text
    return stringify_string(st.session_state.get("report_title"))


def _normalize_visual_figure(raw_figure: Any) -> go.Figure | None:
    if isinstance(raw_figure, go.Figure):
        return go.Figure(raw_figure)

    if isinstance(raw_figure, str):
        try:
            return pio.from_json(raw_figure)
        except Exception:
            return None

    return None


def _figure_to_data_uri(fig: go.Figure) -> str | None:
    try:
        image_bytes = pio.to_image(fig, format="png", width=1400, height=900, scale=2)
    except Exception:
        return None
    return f"data:image/png;base64,{base64.b64encode(image_bytes).decode('ascii')}"


def _inject_visualizations_into_html(final_html: str) -> str:
    visualization_agent = st.session_state.get("visualization_agent")
    if visualization_agent is None or not final_html:
        return final_html

    fig_desc_list = visualization_agent.load_fig() or []
    title_items = _normalize_visualization_titles(st.session_state.get("tu_title"))
    if not fig_desc_list:
        return final_html

    # Keep trailing punctuation attached to the sentence when a figure
    # placeholder is converted into a block-level chart.
    final_html = normalize_trailing_punctuation_before_figure_placeholder(final_html)

    def build_figure_html(fig_index: int, display_number: int) -> str | None:
        if fig_index < 0 or fig_index >= len(fig_desc_list):
            return None

        fig_item = fig_desc_list[fig_index]
        fig = _normalize_visual_figure(fig_item.get("fig") if isinstance(fig_item, dict) else fig_item)
        if fig is None:
            return None

        image_uri = _figure_to_data_uri(fig)
        if not image_uri:
            return None

        caption_text = _build_figure_caption(display_number, fig_index, title_items)
        return (
            f"<div class='report-figure-block' data-fig-index='{fig_index}' data-report-figure-number='{display_number}' "
            "style='margin: 24px 0; text-align: center;'>"
            f"<img src='{image_uri}' alt='Figure {display_number}' style='max-width: 100%; height: auto; border-radius: 12px;' />"
            f"<div class='report-figure-caption' style='margin-top: 10px; text-align: center; "
            "font-size: 0.96rem; color: #4b5563;'>"
            f"{html.escape(caption_text)}"
            "</div>"
            "</div>"
        )

    inserted_figure_count = 0

    def replace_placeholder(match: re.Match[str]) -> str:
        nonlocal inserted_figure_count
        figure_html = build_figure_html(int(match.group(1)), inserted_figure_count + 1)
        if figure_html:
            inserted_figure_count += 1
        return figure_html or match.group(0)

    injected_html, replacement_count = re.subn(
        FIG_PLACEHOLDER_CAPTURE_PATTERN,
        replace_placeholder,
        final_html,
        flags=re.IGNORECASE,
    )
    return injected_html


def _inject_report_base_style(final_html: str) -> str:
    if not final_html or "data-report-font-patch" in final_html:
        return final_html

    style_block = """
<style data-report-font-patch>
body, main, article, section, aside, nav, div, p, span, li, table, td, th, figcaption,
h1, h2, h3, h4, h5, h6, a, strong, em, b, i {
  font-family: "Times New Roman", "Microsoft YaHei", serif !important;
}
body {
  color: #111827;
}
main h1 {
  margin: 1.7em 0 0.7em 0 !important;
  font-size: 2.25rem !important;
  line-height: 1.28 !important;
  font-weight: 800 !important;
  color: #111827 !important;
  letter-spacing: 0.01em;
}
main h2 {
  margin: 1.45em 0 0.6em 0 !important;
  font-size: 1.7rem !important;
  line-height: 1.32 !important;
  font-weight: 700 !important;
  color: #111827 !important;
}
main h3,
main h4,
main h5,
main h6 {
  color: #111827 !important;
  line-height: 1.35 !important;
  font-weight: 700 !important;
}
main p,
main li,
main td,
main th {
  font-size: 1.02rem !important;
  line-height: 1.8 !important;
  color: #111827 !important;
}
.report-title-block h1 {
  margin: 0 !important;
  font-size: 2.6rem !important;
  line-height: 1.25 !important;
  font-weight: 800 !important;
  color: #111827 !important;
}
</style>
"""
    head_match = re.search(r"</head\s*>", final_html, flags=re.IGNORECASE)
    if head_match:
        insert_at = head_match.start()
        return final_html[:insert_at] + style_block + final_html[insert_at:]
    return style_block + final_html


def _normalize_markdown_headings_in_html(final_html: str) -> str:
    if not final_html:
        return final_html

    soup = BeautifulSoup(final_html, "html.parser")
    candidate_tags = soup.find_all(["p", "div", "span", "section", "article", "main"])

    for tag in candidate_tags:
        if not isinstance(tag, Tag):
            continue

        if tag.find(
            [
                "img",
                "table",
                "ul",
                "ol",
                "li",
                "h1",
                "h2",
                "h3",
                "h4",
                "h5",
                "h6",
                "p",
                "section",
                "article",
                "main",
            ]
        ):
            continue

        text_content = tag.get_text("\n", strip=True)
        if not text_content or "#" not in text_content:
            continue

        lines = [line.strip() for line in text_content.replace("\r\n", "\n").split("\n") if line.strip()]
        if not lines:
            continue

        replacement_nodes: list[Tag] = []
        for line in lines:
            parsed_segments = _split_markdown_heading_lines(line)
            if parsed_segments:
                for line_kind, line_text in parsed_segments:
                    if line_kind == "heading":
                        heading_tag = soup.new_tag("h1")
                        heading_tag.string = line_text
                        replacement_nodes.append(heading_tag)
                    else:
                        paragraph_tag = soup.new_tag("p")
                        paragraph_tag.string = line_text
                        replacement_nodes.append(paragraph_tag)
            else:
                paragraph_tag = soup.new_tag("p")
                paragraph_tag.string = line
                replacement_nodes.append(paragraph_tag)

        if not replacement_nodes or not any(node.name == "h1" for node in replacement_nodes):
            continue

        first_node = replacement_nodes[0]
        tag.replace_with(first_node)
        current_node = first_node
        for node in replacement_nodes[1:]:
            current_node.insert_after(node)
            current_node = node

    return str(soup)


def _inject_report_title_into_html(final_html: str, report_title: str) -> str:
    normalized_title = stringify_string(report_title)
    if not final_html or not normalized_title:
        return final_html

    visible_text = html.unescape(re.sub(r"<[^>]+>", " ", final_html))
    visible_text = re.sub(r"\s+", " ", visible_text).strip()
    if normalized_title in visible_text:
        return final_html

    title_html = (
        "<section class='report-title-block' style='text-align: center; margin: 0 0 28px 0;'>"
        f"<h1 style='margin: 0; font-size: 2.1rem; line-height: 1.3; color: #111827;'>{html.escape(normalized_title)}</h1>"
        "</section>"
    )

    main_match = re.search(r"<main[^>]*>", final_html, flags=re.IGNORECASE)
    if main_match:
        insert_at = main_match.end()
        return final_html[:insert_at] + title_html + final_html[insert_at:]

    return title_html + final_html


def _prepare_downloadable_reports(report_agent) -> dict[str, Any]:
    workflow_result = report_agent.load_report_workflow_result()
    html_content = report_agent.load_html()
    markdown_content = report_agent.load_markdown()
    word_bytes = report_agent.load_word()

    if not html_content and workflow_result:
        html_content = extract_report_html(workflow_result)
        if html_content:
            html_content = _normalize_markdown_headings_in_html(html_content)
            html_content = _inject_report_title_into_html(html_content, _extract_report_title(workflow_result))
            html_content = _inject_visualizations_into_html(html_content)
            html_content = _inject_report_base_style(html_content)
            report_agent.save_html(html_content)

    if html_content:
        normalized_html = _normalize_markdown_headings_in_html(html_content)
        if normalized_html != html_content:
            html_content = normalized_html
            report_agent.save_html(html_content)
            report_agent.save_report_content(html_content)
            word_bytes = None

    if not markdown_content:
        if html_content:
            markdown_content = html_to_markdown(html_content)
        elif workflow_result:
            markdown_content = extract_report_markdown(workflow_result) or extract_report_text(workflow_result)

        if markdown_content:
            report_agent.save_markdown(markdown_content)

    if html_content:
        try:
            word_bytes = build_docx_from_html(html_content)
        except Exception:
            word_bytes = None

    if word_bytes is None:
        markdown_source = markdown_content
        if not markdown_source and html_content:
            markdown_source = html_to_markdown(html_content)
        if markdown_source:
            word_bytes = build_docx_from_markdown(markdown_source)

    if word_bytes is None and workflow_result:
        word_bytes = extract_report_word_bytes(workflow_result)

    if word_bytes is not None:
        report_agent.save_word(word_bytes)

    return {
        "word": word_bytes,
        "html": html_content,
        "markdown": markdown_content,
    }


def _refresh_markdown_from_html(report_agent, html_content: str) -> str:
    markdown_content = html_to_markdown(html_content) if html_content else ""
    if markdown_content:
        report_agent.save_markdown(markdown_content)
    return markdown_content


def _build_markdown_preview(markdown_text: str) -> str:
    preview = re.sub(
        r"^\s*!\[[^\]]*\]\((?:data:image/[^)]+|embedded-image)\)\s*$",
        "",
        markdown_text,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    preview = re.sub(
        r"!\[([^\]]*)\]\(data:image/[^)]+\)",
        lambda match: f"![{match.group(1) or '图表'}](embedded-image)",
        preview,
        flags=re.IGNORECASE,
    )
    preview = re.sub(
        r"(?:!\[[^\]]*\]\(embedded-image\)\s*){2,}",
        "[图表已嵌入，预览中省略重复图片占位]\n\n",
        preview,
        flags=re.IGNORECASE,
    )
    preview = re.sub(r"\n{3,}", "\n\n", preview).strip()
    if not preview:
        preview = "[正文预览为空，下载的 Markdown 文件中仍包含完整图表内容]"
    if len(preview) > 60000:
        preview = preview[:60000].rstrip() + "\n\n...[预览已截断，下载文件中仍保留完整内容]"
    return preview


def _clear_generated_report_files(report_agent) -> None:
    report_agent.save_word(None)
    report_agent.save_html(None)
    report_agent.save_markdown(None)
    st.session_state.pop("report_final_html", None)


def _clear_report_workflow_outputs(report_agent) -> None:
    _clear_generated_report_files(report_agent)
    report_agent.save_report_workflow_result(None)
    report_agent.save_report(None)
    report_agent.save_report_content(None)

    for field_name in REPORT_WORKFLOW_OUTPUT_FIELDS:
        st.session_state.pop(f"report_{field_name}", None)

    st.session_state.pop("report_workflow_outputs", None)


def _save_report_workflow_outputs(report_agent, workflow_result: dict[str, Any]) -> None:
    extracted_outputs = _extract_report_workflow_outputs(workflow_result)

    report_agent.save_report_workflow_result(workflow_result)
    report_agent.save_report(workflow_result)
    report_agent.save_report_content(None)

    st.session_state.report_workflow_outputs = extracted_outputs
    for field_name in REPORT_WORKFLOW_OUTPUT_FIELDS:
        st.session_state[f"report_{field_name}"] = extracted_outputs.get(field_name)


def _complete_auto_report(report_agent) -> None:
    report_agent.finish_auto()
    st.session_state.auto_mode = False

    planner = st.session_state.get("planner_agent")
    if planner is not None:
        planner.finish_report_auto()


def _has_report_prerequisites() -> bool:
    return bool(
        st.session_state.get("summary_1")
        and st.session_state.get("summary_2")
        and st.session_state.get("summary_3")
        and st.session_state.get("summary_4")
    )


def _has_usable_visualization_source(source: Any) -> bool:
    if source is None:
        return False

    if isinstance(source, pd.DataFrame):
        return not source.empty

    if isinstance(source, np.ndarray):
        return source.size > 0

    if isinstance(source, str):
        return bool(source.strip())

    if isinstance(source, (list, dict)):
        return bool(source)

    return True


def _source_to_visualization_dataframe(source: Any) -> pd.DataFrame | None:
    if isinstance(source, pd.DataFrame):
        return source.copy()

    if isinstance(source, np.ndarray):
        return pd.DataFrame(source)

    if isinstance(source, str):
        parsed = maybe_json_loads(source)
        if isinstance(parsed, str):
            return None
        source = parsed

    if isinstance(source, list):
        try:
            return pd.DataFrame(source)
        except Exception:
            return None

    return None


def _resolve_visualization_dataframe_for_report(preproc_agent, load_agent) -> pd.DataFrame | None:
    processed_df = preproc_agent.load_processed_df()
    if _has_usable_visualization_source(processed_df):
        return _source_to_visualization_dataframe(processed_df)

    summary_2 = st.session_state.get("summary_2")
    if isinstance(summary_2, dict):
        summary_processed_df = summary_2.get("processed_df")
        if _has_usable_visualization_source(summary_processed_df):
            return _source_to_visualization_dataframe(summary_processed_df)

    cached_processed_df = st.session_state.get("prep_result_from_summary_2")
    if _has_usable_visualization_source(cached_processed_df):
        return _source_to_visualization_dataframe(cached_processed_df)

    raw_df = load_agent.load_df()
    if _has_usable_visualization_source(raw_df):
        return _source_to_visualization_dataframe(raw_df)

    return None


def _has_generated_outline(report_agent) -> bool:
    return bool(normalize_toc_list(report_agent.load_outline()))


def _has_generated_word_report(report_agent) -> bool:
    return bool(report_agent.load_report_content() or report_agent.load_html() or report_agent.load_word())


def _has_visualization_recommendation(visualization_agent) -> bool:
    if visualization_agent is None:
        return False

    suggestion = (
        st.session_state.get("visual_recommendatio")
        or st.session_state.get("viz_suggestion")
        or visualization_agent.load_suggestion()
    )
    return bool(stringify_string(suggestion))


def _ensure_visualization_ready_for_report(visualization_agent) -> bool:
    if visualization_agent is None or not _has_visualization_recommendation(visualization_agent):
        st.warning("请先完成可视化推荐部分。")
        return False

    if not visualization_agent.load_code():
        if not generate_visualization_code_once(visualization_agent):
            st.warning("未能自动生成可视化代码，请先前往可视化页面检查推荐结果。")
            return False

    if not visualization_agent.load_fig():
        if not execute_visualization_code_once(visualization_agent):
            st.warning("未能自动生成可视化结果，请先前往可视化页面检查代码或数据。")
            return False

    return True


def _build_report_inputs(load_agent, report_agent) -> dict[str, Any]:
    load_summary = maybe_json_loads(_resolve_loading_field(load_agent, "summary_1", {}))
    preproc_summary = maybe_json_loads(st.session_state.get("summary_2", {}))
    visual_summary = maybe_json_loads(st.session_state.get("summary_3", {}))
    coding_summary = maybe_json_loads(st.session_state.get("summary_4", {}))

    if not isinstance(load_summary, dict):
        load_summary = {}
    if not isinstance(preproc_summary, dict):
        preproc_summary = {}
    if not isinstance(visual_summary, dict):
        visual_summary = {}
    if not isinstance(coding_summary, dict):
        coding_summary = {}

    return {
        "load_summary": load_summary,
        "preproc_summary": preproc_summary,
        "visual_summary": visual_summary,
        "coding_summary": coding_summary,
        "selected_full_conten": stringify_string(st.session_state.get("full")),
        "load_abstract": stringify_string(_resolve_loading_field(load_agent, "abstract_1", "")),
        "preproc_abstract": stringify_string(st.session_state.get("abstract_2", "")),
        "visual_abstract": stringify_string(st.session_state.get("abstract_3", "")),
        "coding_abstract": stringify_string(st.session_state.get("abstract_4", "")),
        "toc_md": normalize_toc_list(report_agent.load_outline()),
        "outline_length": str(report_agent.load_outline_length() or ""),
        "preference_select": stringify_string(st.session_state.get("preference_select")),
        "add_preference": stringify_string(st.session_state.get("additional_preference")),
        "report_auto": True,
        "user_input": str(report_agent.load_user_input() or ""),
    }


def _build_word_report_inputs(report_agent) -> dict[str, Any]:
    return {
        "toc_text": _normalize_multiline_text(report_agent.load_outline()),
        "title": stringify_string(st.session_state.get("report_title")),
        "selected_full_conten": stringify_string(st.session_state.get("report_selected_full_conten")),
        "preference_select": stringify_string(st.session_state.get("report_preference_select")),
        "add_preference": stringify_string(st.session_state.get("report_add_preference")),
        "load_abstract": stringify_string(st.session_state.get("report_load_abstract")),
        "preproc_abstract": stringify_string(st.session_state.get("report_preproc_abstract")),
        "visual_abstract": stringify_string(st.session_state.get("report_visual_abstract")),
        "coding_abstract": stringify_string(st.session_state.get("report_coding_abstract")),
    }


def call_coze_workflow_report_stream(inputs: dict[str, Any]) -> dict[str, Any] | None:
    runtime = resolve_coze_runtime(default_api_key=DEFAULT_COZE_API_KEY)
    api_key = runtime["api_key"]
    coze_url = runtime.get("coze_url", "")

    if not api_key:
        st.error("请先在侧边栏填写 Coze Personal Access Token。")
        return None

    client = Coze(
        auth=TokenAuth(token=api_key),
        base_url=_resolve_coze_base_url(coze_url),
    )

    status_placeholder = st.empty()
    start_time = time.time()
    message_buffers: dict[str, str] = {}
    finished_messages: list[str] = []

    status_placeholder.info("正在调用目录工作流，请稍候。")

    try:
        workflow_stream = client.workflows.runs.stream(
            workflow_id=WORKFLOW_ID,
            parameters=inputs,
            bot_id=BOT_ID,
            ext={"space_id": COZE_SPACE_ID},
        )
    except Exception as exc:
        st.error(f"目录工作流启动失败：{exc}")
        return None

    try:
        for event in workflow_stream:
            elapsed = int(time.time() - start_time)
            if elapsed > MAX_POLL_SECONDS:
                status_placeholder.empty()
                st.error(f"目录工作流执行超时，已等待 {MAX_POLL_SECONDS} 秒。")
                return None

            if event.event == WorkflowEventType.ERROR and event.error is not None:
                status_placeholder.empty()
                st.error(
                    f"目录工作流执行失败：{event.error.error_code} {event.error.error_message}".strip()
                )
                return None

            if event.event == WorkflowEventType.INTERRUPT and event.interrupt is not None:
                status_placeholder.empty()
                st.error(f"目录工作流被中断，节点为 {event.interrupt.node_title}。")
                return None

            if event.event != WorkflowEventType.MESSAGE or event.message is None:
                continue

            node_key = f"{event.message.node_title}:{event.message.node_seq_id}"
            message_buffers[node_key] = message_buffers.get(node_key, "") + event.message.content

            if event.message.node_is_finish:
                finished_messages.append(message_buffers[node_key])
    except Exception as exc:
        status_placeholder.empty()
        st.error(f"目录工作流执行失败：{exc}")
        return None

    normalized_messages: list[dict[str, Any]] = []
    for candidate in finished_messages or list(message_buffers.values()):
        normalized = _normalize_report_workflow_result(candidate)
        if normalized is not None:
            normalized_messages.append(normalized)

    merged_result = _merge_report_workflow_results(normalized_messages)
    if merged_result is not None:
        status_placeholder.success("目录工作流执行完成。")
        return merged_result

    status_placeholder.empty()
    st.error("目录工作流已完成，但未解析到有效输出。")
    return None


def call_coze_workflow_word_stream(inputs: dict[str, Any]) -> dict[str, Any] | None:
    runtime = resolve_coze_runtime(default_api_key=DEFAULT_COZE_API_KEY)
    api_key = runtime["api_key"]
    coze_url = runtime.get("coze_url", "")

    if not api_key:
        st.error("请先在侧边栏填写 Coze Personal Access Token。")
        return None

    client = Coze(
        auth=TokenAuth(token=api_key),
        base_url=_resolve_coze_base_url(coze_url),
    )

    status_placeholder = st.empty()
    start_time = time.time()
    message_buffers: dict[str, str] = {}
    finished_messages: list[str] = []

    status_placeholder.info("正在调用 Word 报告工作流，请稍候。")

    try:
        workflow_stream = client.workflows.runs.stream(
            workflow_id=WORD_REPORT_WORKFLOW_ID,
            parameters=inputs,
            bot_id=BOT_ID,
            ext={"space_id": COZE_SPACE_ID},
        )
    except Exception as exc:
        st.error(f"Word 报告工作流启动失败：{exc}")
        return None

    try:
        for event in workflow_stream:
            elapsed = int(time.time() - start_time)
            if elapsed > MAX_POLL_SECONDS:
                status_placeholder.empty()
                st.error(f"Word 报告工作流执行超时，已等待 {MAX_POLL_SECONDS} 秒。")
                return None

            if event.event == WorkflowEventType.ERROR and event.error is not None:
                status_placeholder.empty()
                st.error(
                    f"Word 报告工作流执行失败：{event.error.error_code} {event.error.error_message}".strip()
                )
                return None

            if event.event == WorkflowEventType.INTERRUPT and event.interrupt is not None:
                status_placeholder.empty()
                st.error(f"Word 报告工作流被中断，节点为 {event.interrupt.node_title}。")
                return None

            if event.event != WorkflowEventType.MESSAGE or event.message is None:
                continue

            node_key = f"{event.message.node_title}:{event.message.node_seq_id}"
            message_buffers[node_key] = message_buffers.get(node_key, "") + event.message.content

            if event.message.node_is_finish:
                finished_messages.append(message_buffers[node_key])
    except Exception as exc:
        status_placeholder.empty()
        st.error(f"Word 报告工作流执行失败：{exc}")
        return None

    normalized_messages: list[dict[str, Any]] = []
    for candidate in finished_messages or list(message_buffers.values()):
        normalized = _normalize_report_workflow_result(candidate)
        if normalized is not None:
            normalized_messages.append(normalized)

    merged_result = _merge_report_workflow_results(normalized_messages)
    if merged_result is not None:
        status_placeholder.success("Word 报告工作流执行完成。")
        return merged_result

    status_placeholder.empty()
    st.error("Word 报告工作流已完成，但未解析到有效输出。")
    return None


def _generate_formatted_report(report_agent, action: str) -> None:
    workflow_result = call_coze_workflow_word_stream(_build_word_report_inputs(report_agent))
    if not workflow_result:
        return

    final_html = extract_report_html(workflow_result)
    if not final_html:
        st.error("Word 报告工作流未返回 `final_html`。")
        return
    report_title = _extract_report_title(workflow_result)
    if report_title:
        st.session_state.report_title = report_title
    final_html = _normalize_markdown_headings_in_html(final_html)
    final_html = _inject_report_title_into_html(final_html, report_title)
    final_html = _inject_visualizations_into_html(final_html)
    final_html = _inject_report_base_style(final_html)

    _clear_generated_report_files(report_agent)

    report_agent.save_report_workflow_result(workflow_result)
    report_agent.save_report(workflow_result)
    report_agent.save_report_content(final_html)
    report_agent.save_html(final_html)
    _prepare_downloadable_reports(report_agent)
    st.session_state.report_final_html = final_html
    st.success(f"{action} 报告已生成，已在下方展示。")


def report_basic_info(load_agent, report_agent, auto: bool) -> None:
    outline_length = sac.segmented(
        items=[
            sac.SegmentedItem(label="简要"),
            sac.SegmentedItem(label="标准"),
            sac.SegmentedItem(label="详细"),
        ],
        label="详细程度",
        index=1,
        align="center",
        size="sm",
        radius="sm",
        use_container_width=True,
    )
    report_agent.save_outline_length(outline_length)

    report_format = sac.chip(
        items=[
            sac.ChipItem(label="Word", icon=sac.BsIcon(name="file-earmark-word", size=16)),
            sac.ChipItem(label="HTML", icon=sac.BsIcon(name="filetype-html", size=16)),
            sac.ChipItem(label="Markdown", icon=sac.BsIcon(name="file-earmark-code", size=16)),
        ],
        label="选择报告生成格式",
        index=[0, 2],
        align="start",
        radius="md",
        multiple=False,
    )
    if auto:
        report_format = "Word"
    report_agent.save_report_format(report_format)

    user_input = st.text_input("报告生成要求", "默认")
    report_agent.save_user_input(user_input)
    visualization_agent = st.session_state.get("visualization_agent")

    if not auto and not _has_visualization_recommendation(visualization_agent):
        st.warning("请先完成可视化推荐部分。")

    not_generated = not _has_generated_outline(report_agent)
    if st.button("生成目录") or (auto and not_generated):
        if not auto and not _has_visualization_recommendation(visualization_agent):
            st.warning("请先完成可视化推荐部分。")
            return

        _clear_report_workflow_outputs(report_agent)
        report_agent.save_outline([])

        inputs = _build_report_inputs(load_agent, report_agent)
        workflow_result = call_coze_workflow_report_stream(inputs)

        if not workflow_result:
            return

        _save_report_workflow_outputs(report_agent, workflow_result)

        toc_text = _extract_toc_text_from_result(workflow_result)
        if not toc_text:
            st.error("报告工作流未返回 `toc_text`。")
            return

        report_agent.save_outline(toc_text)
        if auto:
            st.rerun()
        st.success("目录已生成，已在下方显示目录文本。")


def report_outline(report_agent) -> None:
    st.subheader("目录结构预览与编辑")

    outline_value = report_agent.load_outline()
    if isinstance(outline_value, str):
        default_toc = stringify_string(outline_value).replace("\\r\\n", "\n").replace("\\n", "\n")
    else:
        default_toc = "\n".join(normalize_toc_list(outline_value))
    toc_text = st.text_area(
        "您可以在此处编辑目录结构，每行一个目录项",
        value=default_toc,
        height=260,
        placeholder="# 数据分析报告\n## 1. 数据导入",
    )
    report_agent.save_outline(toc_text)


def report_save(report_agent, auto: bool) -> None:
    action = report_agent.load_report_format()
    visualization_agent = st.session_state.get("visualization_agent")

    outline_generated = _has_generated_outline(report_agent)
    report_generated = _has_generated_word_report(report_agent)
    not_generate = outline_generated and not report_generated

    if auto and report_generated and not report_agent.finish_auto_task:
        _complete_auto_report(report_agent)
        st.rerun()

    if st.button(f"生成 {action} 报告") or (auto and not_generate):
        if st.session_state.get("report_selected_full_conten") is None:
            st.warning("请先点击“生成目录”获取新 workflow 输出。")
            return

        if not auto and not _ensure_visualization_ready_for_report(visualization_agent):
            return

        _generate_formatted_report(report_agent, action)

        if auto:
            current_action = report_agent.load_report_format()
            generated = (
                report_agent.load_html() is not None
                if current_action == "Word"
                else report_agent.load_html() is not None
                if current_action == "HTML"
                else report_agent.load_html() is not None
            )
            if generated:
                _complete_auto_report(report_agent)
                st.rerun()


def report_execution(report_agent) -> None:
    action = report_agent.load_report_format()
    final_report_html = report_agent.load_report_content() or report_agent.load_html()
    if not final_report_html:
        return

    normalized_html = _normalize_markdown_headings_in_html(final_report_html)
    if normalized_html != final_report_html:
        final_report_html = normalized_html
        report_agent.save_report_content(final_report_html)
        report_agent.save_html(final_report_html)
        report_agent.save_word(None)

    downloadable_reports = _prepare_downloadable_reports(report_agent)
    preview_html = downloadable_reports["html"] or final_report_html

    if action == "Word":
        st.download_button(
            label="下载 Word 报告",
            data=downloadable_reports["word"] or b"",
            file_name="report.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            disabled=downloadable_reports["word"] is None,
        )
        components.html(preview_html, height=720, scrolling=True)
        return

    if action == "HTML":
        st.download_button(
            label="下载 HTML 报告",
            data=preview_html.encode("utf-8"),
            file_name="report.html",
            mime="text/html",
            disabled=not bool(preview_html),
        )
        components.html(preview_html, height=720, scrolling=True)
        return

    markdown_report = _refresh_markdown_from_html(report_agent, preview_html)
    if not markdown_report:
        markdown_report = downloadable_reports["markdown"]
    if markdown_report:
        markdown_preview = build_markdown_preview_from_html(preview_html)
        if not markdown_preview:
            markdown_preview = _build_markdown_preview(markdown_report)
        st.session_state["report_markdown_preview"] = markdown_preview
        st.download_button(
            label="下载 Markdown 报告",
            data=markdown_report.encode("utf-8"),
            file_name="report.md",
            mime="text/markdown",
            disabled=False,
        )
        st.text_area(
            "Markdown 预览",
            key="report_markdown_preview",
            height=720,
        )


if __name__ == "__main__":
    st.title("报告生成")
    st.markdown("---")

    load_agent = st.session_state.data_loading_agent
    preproc_agent = st.session_state.data_preprocess_agent
    planner = st.session_state.planner_agent
    auto = bool(st.session_state.auto_mode and planner.report_auto)

    if st.session_state.auto_mode and not _has_report_prerequisites():
        st.warning("自动模式需要在前序步骤都生成结果后，才会进入报告生成。")
        st.stop()

    processed_df = preproc_agent.load_processed_df()
    df = processed_df if processed_df is not None else load_agent.load_df()

    if df is None:
        st.warning("请先在数据导入页面加载数据。")
        st.stop()

    if isinstance(df, np.ndarray):
        df = pd.DataFrame(df)

    sampled_df = df.sample(frac=1, random_state=42).reset_index(drop=True)
    visualization_df = _resolve_visualization_dataframe_for_report(preproc_agent, load_agent)
    if isinstance(visualization_df, pd.DataFrame):
        visualization_df = visualization_df.sample(frac=1, random_state=42).reset_index(drop=True)
    else:
        visualization_df = sampled_df
    report_agent = st.session_state.report_agent
    report_agent.add_df(sampled_df)
    visualization_agent = st.session_state.get("visualization_agent")
    if visualization_agent is not None:
        visualization_agent.add_df(visualization_df)
    outline_generated = _has_generated_outline(report_agent)
    report_generated = _has_generated_word_report(report_agent)

    columns = st.columns(2)
    with columns[0].expander("报告设置", expanded=True):
        report_basic_info(load_agent, report_agent, auto)

    with columns[1].expander("报告大纲", expanded=True):
        report_outline(report_agent)
        report_save(report_agent, auto)
        report_execution(report_agent)
