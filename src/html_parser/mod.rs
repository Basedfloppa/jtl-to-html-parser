use crate::{ErrTypeOut, LabelOut, OverallOut};

const TEMPLATE: &str = include_str!("template.html");
const STYLES: &str = include_str!("styles.css");
const APP_JS: &str = include_str!("app.js");

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn json_for_script<T: serde::Serialize>(v: &T) -> String {
    serde_json::to_string(v)
        .expect("serialize json")
        .replace('<', "\\u003c")
}

pub fn render_html(
    overall: OverallOut,
    mut labels: Vec<LabelOut>,
    mut errs: Vec<ErrTypeOut>,
    title: &str,
) -> String {
    labels.sort_by(|a, b| b.count.cmp(&a.count));
    errs.sort_by(|a, b| b.count.cmp(&a.count));

    let title_escaped = escape_html(title);
    let overall_json = json_for_script(&overall);
    let labels_json = json_for_script(&labels);
    let errs_json = json_for_script(&errs);

    TEMPLATE
        .replace("__TITLE__", &title_escaped)
        .replace("__INLINE_CSS__", STYLES)
        .replace("__OVERALL_JSON__", &overall_json)
        .replace("__LABELS_JSON__", &labels_json)
        .replace("__ERRORS_JSON__", &errs_json)
        .replace("__INLINE_APP_JS__", APP_JS)
}
