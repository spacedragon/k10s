#![cfg(target_arch = "wasm32")]
//! Eframe WASM host for the shared k10s protocol application.
//!
//! The visible application is the same egui renderer used by desktop. Because
//! eframe 0.36 does not expose its AccessKit tree to browser automation, a
//! hidden semantic companion drives the very same [`K10sApp`] instance.
//! Credentials remain ephemeral.

use std::cell::RefCell;
use std::rc::Rc;

use k10s_ui::ui::dialogs::ActiveDialogKind;
use k10s_ui::workspace::{DetailTab, WindowId, WorkloadKind};
use k10s_ui::{AppView, ConnectionGate, K10sApp, derive_control_url};
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
#[cfg(target_arch = "wasm32")]
use web_sys::HtmlCanvasElement;
use web_sys::{Document, Element, HtmlInputElement};

const POLL_INTERVAL_MS: i32 = 50;
type RuntimeHandle = Rc<RefCell<Runtime>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Gate,
    Connecting,
    Ready,
    Failure,
}

#[derive(Debug)]
struct Runtime {
    stage: Stage,
    document: Document,
    root: Element,
    gate: ConnectionGate,
    app: Option<K10sApp>,
    active_kind: Option<WorkloadKind>,
    /// Whether the semantic Services panel is the active surface.
    active_services: bool,
    active_window: Option<WindowId>,
    render_key: String,
    action_status: String,
    egui_token: String,
}

#[derive(Debug)]
#[cfg(target_arch = "wasm32")]
struct WebApp {
    runtime: RuntimeHandle,
}

#[cfg(target_arch = "wasm32")]
impl eframe::App for WebApp {
    fn logic(&mut self, context: &eframe::egui::Context, _: &mut eframe::Frame) {
        self.runtime.borrow_mut().poll_once();
        context.request_repaint_after(std::time::Duration::from_millis(POLL_INTERVAL_MS as u64));
    }

    fn ui(&mut self, ui: &mut eframe::egui::Ui, _: &mut eframe::Frame) {
        let mut runtime = self.runtime.borrow_mut();
        if let Some(app) = runtime.app.as_mut() {
            app.render_ui(ui);
            return;
        }
        eframe::egui::CentralPanel::default().show(ui, |ui| {
            ui.heading("Connect to k10s");
            if let Some(error) = runtime.gate.error() {
                ui.colored_label(eframe::egui::Color32::LIGHT_RED, error);
            }
            ui.label("Access token");
            ui.add(eframe::egui::TextEdit::singleline(&mut runtime.egui_token).password(true));
            if ui.button("Connect").clicked() {
                let token = std::mem::take(&mut runtime.egui_token);
                runtime.begin_connection(token);
            }
        });
    }
}

impl Runtime {
    fn begin_connection(&mut self, token: String) {
        if matches!(self.stage, Stage::Ready | Stage::Connecting) {
            return;
        }
        self.gate.set_token_input(token);
        match K10sApp::connect(self.gate.begin_connection()) {
            Ok(app) => {
                self.app = Some(app);
                self.render_connecting(false);
            }
            Err(error) => {
                self.app = None;
                self.render_failure(&error.to_string());
            }
        }
    }

    fn poll_once(&mut self) {
        let Some(app) = self.app.as_mut() else {
            return;
        };
        app.poll();
        match app.view().clone() {
            AppView::Connecting => {
                if self.stage == Stage::Ready {
                    self.render_connecting(true);
                }
            }
            AppView::Ready { context_names, .. } => {
                self.gate.authentication_succeeded();
                self.render_ready(&context_names);
            }
            AppView::Failed { .. } if app.requires_connection_gate() => {
                self.app = None;
                self.active_kind = None;
                self.active_services = false;
                self.active_window = None;
                self.gate.authentication_rejected();
                self.render_gate();
            }
            AppView::Failed { message } => {
                self.app = None;
                self.active_services = false;
                self.render_failure(&message);
            }
        }
    }

    fn perform_action(&mut self, action: &str) {
        if action == "connect" {
            self.begin_connection(self.token_input_value());
            return;
        }
        let Some(app) = self.app.as_mut() else {
            return;
        };
        if let Some(value) = action.strip_prefix("workload:") {
            if let Some(kind) = parse_kind(value) {
                self.active_kind = Some(kind);
                self.active_services = false;
                self.active_window = app.web_activate_workload(kind);
            }
        } else if action == "services" {
            self.active_kind = None;
            self.active_services = true;
            self.active_window = app.web_activate_services();
        } else if let Some(uid) = action.strip_prefix("svcrow:") {
            if self.active_services
                && let Some(window) = self.active_window
            {
                app.web_select_service(window, uid);
            }
        } else if let Some(uid) = action.strip_prefix("row:") {
            if let (Some(kind), Some(window)) = (self.active_kind, self.active_window)
                && let Some(row) = app
                    .web_resource_rows(kind)
                    .into_iter()
                    .find(|row| row.identity.uid == uid)
            {
                app.web_select_resource(window, row.identity);
            }
        } else if let Some(tab) = action.strip_prefix("tab:") {
            if let (Some(window), Some(tab)) = (self.active_window, parse_tab(tab)) {
                app.web_set_detail_tab(window, tab);
            }
        } else if action == "connect-logs" {
            if let Some(window) = self.active_window {
                app.web_set_detail_tab(window, DetailTab::Logs);
                self.action_status = match app.web_connect_logs(window) {
                    Ok(()) => "Logs connection requested".to_owned(),
                    Err(error) => format!("Logs request failed: {error}"),
                };
            }
        } else if action == "reconnect" {
            self.action_status = "Control reconnect requested".to_owned();
            app.web_reconnect();
        } else if action == "open-scale"
            && let Some(window) = self.active_window
        {
            app.web_open_scale_dialog(window);
        } else if action == "open-delete"
            && let Some(window) = self.active_window
        {
            app.web_open_delete_dialog(window);
        }
        self.render_key.clear();
    }

    fn render_gate(&mut self) {
        self.render_stage(Stage::Gate);
        self.append_heading(1, "Connect to k10s");
        if let Some(error) = self.gate.error() {
            self.append_text("p", error);
        }
        let label = self.create_element("label");
        label.set_attribute("for", "access-token").unwrap();
        label.set_text_content(Some("Access token"));
        self.root.append_child(&label).unwrap();
        let input = self
            .document
            .create_element("input")
            .unwrap()
            .dyn_into::<HtmlInputElement>()
            .unwrap();
        input.set_id("access-token");
        input.set_attribute("type", "password").unwrap();
        input.set_attribute("autocomplete", "off").unwrap();
        self.root.append_child(&input).unwrap();
        self.append_button("Connect", "connect");
    }

    fn render_ready(&mut self, context_names: &[String]) {
        let Some(app) = self.app.as_ref() else {
            return;
        };
        let rows = if self.active_services {
            Vec::new()
        } else {
            self.active_kind
                .map(|kind| app.web_resource_rows(kind))
                .unwrap_or_default()
        };
        let service_rows = if self.active_services {
            app.web_service_rows()
        } else {
            Vec::new()
        };
        let selected = self
            .active_window
            .and_then(|window| app.web_selected_detail(window))
            .map(|(identity, detail)| (identity.clone(), detail.cloned()));
        // Service selections render their own projection-driven panel
        // instead of the generic sections.
        let service_detail = if self.active_services
            && let Some(window) = self.active_window
            && let Some((identity, _)) = app.web_selected_detail(window)
        {
            Some((identity.clone(), app.web_service_detail(window)))
        } else {
            None
        };

        let stream = self.active_window.map(|window| app.web_stream_text(window));
        let dialog = self
            .active_window
            .and_then(|window| app.web_dialog_kind(window));
        let key = format!(
            "{context_names:?}|{:?}|{rows:?}|{service_rows:?}|{selected:?}|{stream:?}|{dialog:?}|{}|{}",
            self.active_kind, self.active_services, self.action_status
        );
        if self.stage == Stage::Ready && self.render_key == key {
            return;
        }
        self.render_stage(Stage::Ready);
        self.render_key = key;
        self.append_heading(1, "k10s Workspace");
        self.append_text_with_attr("p", "Connected", "role", "status");
        self.append_button("Reconnect control connection", "reconnect");
        if !self.action_status.is_empty() {
            self.append_text_with_attr("p", &self.action_status, "role", "log");
        }
        self.append_heading(2, "Kubernetes contexts");
        let contexts = self.create_element("ul");
        for name in context_names {
            let item = self.create_element("li");
            item.set_text_content(Some(name));
            contexts.append_child(&item).unwrap();
        }
        self.root.append_child(&contexts).unwrap();

        let navigation = self.create_element("nav");
        navigation.set_attribute("aria-label", "Resources").unwrap();
        self.append_button_to(&navigation, "Services", "services");
        for kind in WorkloadKind::TAXONOMY {
            self.append_button_to(&navigation, kind.title(), &format!("workload:{kind:?}"));
        }
        self.root.append_child(&navigation).unwrap();

        if let Some(kind) = self.active_kind {
            self.append_heading(2, kind.title());
            if rows.is_empty() {
                self.append_text_with_attr("p", "Loading resources", "role", "status");
            } else {
                let table = self.create_element("table");
                table.set_attribute("aria-label", kind.title()).unwrap();
                let body = self.create_element("tbody");
                for row in rows {
                    let tr = self.create_element("tr");
                    let name = self.create_element("td");
                    self.append_button_to(
                        &name,
                        &row.identity.name,
                        &format!("row:{}", row.identity.uid),
                    );
                    let namespace = self.create_element("td");
                    namespace.set_text_content(Some(
                        row.identity.namespace.as_deref().unwrap_or("cluster"),
                    ));
                    let summary = self.create_element("td");
                    summary.set_text_content(Some(&row.summary));
                    tr.append_child(&name).unwrap();
                    tr.append_child(&namespace).unwrap();
                    tr.append_child(&summary).unwrap();
                    body.append_child(&tr).unwrap();
                }
                table.append_child(&body).unwrap();
                self.root.append_child(&table).unwrap();
            }
        }

        if self.active_services {
            self.append_heading(2, "Services");
            if service_rows.is_empty() {
                self.append_text_with_attr("p", "Loading resources", "role", "status");
            } else {
                let table = self.create_element("table");
                table.set_attribute("aria-label", "Services").unwrap();
                let body = self.create_element("tbody");
                for (uid, name, namespace, type_label, ports_label) in service_rows {
                    let tr = self.create_element("tr");
                    let name_cell = self.create_element("td");
                    self.append_button_to(&name_cell, &name, &format!("svcrow:{uid}"));
                    let namespace_cell = self.create_element("td");
                    namespace_cell.set_text_content(Some(&namespace));
                    let type_cell = self.create_element("td");
                    type_cell.set_text_content(Some(&type_label));
                    let ports_cell = self.create_element("td");
                    ports_cell.set_text_content(Some(&ports_label));
                    tr.append_child(&name_cell).unwrap();
                    tr.append_child(&namespace_cell).unwrap();
                    tr.append_child(&type_cell).unwrap();
                    tr.append_child(&ports_cell).unwrap();
                    body.append_child(&tr).unwrap();
                }
                table.append_child(&body).unwrap();
                self.root.append_child(&table).unwrap();
            }
        }

        if let Some((identity, detail)) = service_detail {
            self.append_heading(2, &format!("{} details", identity.name));
            match detail {
                Some(detail) => {
                    self.append_heading(3, "Overview");
                    for (label, value) in &detail.overview {
                        self.append_text("p", &format!("{label}: {value}"));
                    }
                    self.append_heading(3, "Ports");
                    for line in &detail.ports {
                        self.append_text("p", line);
                    }
                    // Port forwarding is desktop-only and arrives in a
                    // later task: deliberately no Start/Stop controls.
                }
                None => {
                    self.append_text_with_attr("p", "Loading details", "role", "status");
                }
            }
        } else if let Some((identity, detail)) = selected {
            self.append_heading(2, &format!("{} details", identity.name));
            let tabs = self.create_element("div");
            tabs.set_attribute("role", "tablist").unwrap();
            for (label, tab) in [
                ("Overview", "Overview"),
                ("YAML", "Yaml"),
                ("Events", "Events"),
                ("Logs", "Logs"),
            ] {
                self.append_button_to(&tabs, label, &format!("tab:{tab}"));
            }
            self.root.append_child(&tabs).unwrap();
            if let Some(detail) = detail {
                for section in &detail.sections {
                    self.append_heading(3, &section.title);
                    for row in &section.rows {
                        self.append_text("p", &format!("{}: {}", row.label, row.value));
                    }
                }
                if detail.capabilities.can_scale {
                    self.append_button("Scale workload", "open-scale");
                }
                if detail.capabilities.can_delete {
                    self.append_button("Delete resource", "open-delete");
                }
                if detail.capabilities.can_view_logs {
                    self.append_button("Connect logs", "connect-logs");
                }
            } else {
                self.append_text_with_attr("p", "Loading details", "role", "status");
            }
        }

        if let Some((log_phase, log_lines)) = stream {
            self.append_text("p", &format!("Logs: {log_phase}"));
            for line in log_lines {
                self.append_text("pre", &line);
            }
        }
        if dialog == Some(ActiveDialogKind::Scale) {
            let dialog = self.create_element("section");
            dialog.set_attribute("role", "dialog").unwrap();
            dialog
                .set_attribute("aria-label", "Scale workload")
                .unwrap();
            let title = self.create_element("h2");
            title.set_text_content(Some("Scale workload"));
            dialog.append_child(&title).unwrap();
            self.root.append_child(&dialog).unwrap();
        } else if dialog == Some(ActiveDialogKind::Delete) {
            let dialog = self.create_element("section");
            dialog.set_attribute("role", "dialog").unwrap();
            dialog
                .set_attribute("aria-label", "Delete resource")
                .unwrap();
            let title = self.create_element("h2");
            title.set_text_content(Some("Destructive confirmation"));
            dialog.append_child(&title).unwrap();
            self.root.append_child(&dialog).unwrap();
        }
    }

    fn render_failure(&mut self, message: &str) {
        self.render_stage(Stage::Failure);
        self.append_text("p", &format!("Connection failed: {message}"));
    }

    fn render_connecting(&mut self, recovering: bool) {
        self.render_stage(Stage::Connecting);
        self.append_text_with_attr(
            "p",
            if recovering {
                "Reconnecting and resyncing"
            } else {
                "Connecting"
            },
            "role",
            "status",
        );
    }

    fn render_stage(&mut self, stage: Stage) {
        self.stage = stage;
        self.render_key.clear();
        while let Some(child) = self.root.first_child() {
            self.root.remove_child(&child).unwrap();
        }
    }

    fn append_heading(&self, level: u8, text: &str) {
        self.append_text(&format!("h{level}"), text);
    }

    fn append_text(&self, tag: &str, text: &str) {
        let element = self.create_element(tag);
        element.set_text_content(Some(text));
        self.root.append_child(&element).unwrap();
    }

    fn append_text_with_attr(&self, tag: &str, text: &str, name: &str, value: &str) {
        let element = self.create_element(tag);
        element.set_attribute(name, value).unwrap();
        element.set_text_content(Some(text));
        self.root.append_child(&element).unwrap();
    }

    fn append_button(&self, label: &str, action: &str) {
        self.append_button_to(&self.root, label, action);
    }

    fn append_button_to(&self, parent: &Element, label: &str, action: &str) {
        let button = self.create_element("button");
        button.set_text_content(Some(label));
        button.set_attribute("data-action", action).unwrap();
        parent.append_child(&button).unwrap();
    }

    fn create_element(&self, tag: &str) -> Element {
        self.document.create_element(tag).unwrap()
    }

    fn token_input_value(&self) -> String {
        self.document
            .get_element_by_id("access-token")
            .and_then(|element| element.dyn_into::<HtmlInputElement>().ok())
            .map(|input| input.value())
            .unwrap_or_default()
    }
}

fn parse_kind(value: &str) -> Option<WorkloadKind> {
    WorkloadKind::TAXONOMY
        .into_iter()
        .find(|kind| format!("{kind:?}") == value)
}

fn parse_tab(value: &str) -> Option<DetailTab> {
    match value {
        "Overview" => Some(DetailTab::Overview),
        "Yaml" => Some(DetailTab::Yaml),
        "Events" => Some(DetailTab::Events),
        "Logs" => Some(DetailTab::Logs),
        _ => None,
    }
}

#[wasm_bindgen(start)]
#[cfg(target_arch = "wasm32")]
async fn main() -> Result<(), JsValue> {
    std::panic::set_hook(Box::new(console_error_panic_hook::hook));
    let window = web_sys::window().ok_or_else(|| JsValue::from_str("window is unavailable"))?;
    let document = window
        .document()
        .ok_or_else(|| JsValue::from_str("document is unavailable"))?;
    let location = window.location();
    let control_url =
        derive_control_url(&location.protocol()?, &location.host()?).map_err(JsValue::from_str)?;
    let root = document
        .get_element_by_id("app")
        .ok_or_else(|| JsValue::from_str("#app root element is missing"))?;
    let canvas = document
        .get_element_by_id("k10s-canvas")
        .ok_or_else(|| JsValue::from_str("#k10s-canvas is missing"))?
        .dyn_into::<HtmlCanvasElement>()?;
    let runtime = Rc::new(RefCell::new(Runtime {
        stage: Stage::Failure,
        gate: ConnectionGate::new(control_url),
        app: None,
        active_kind: None,
        active_services: false,
        active_window: None,
        render_key: String::new(),
        action_status: String::new(),
        egui_token: String::new(),
        root,
        document,
    }));
    runtime.borrow_mut().render_gate();
    attach_click_handler(&runtime)?;
    start_polling(&window, Rc::clone(&runtime))?;
    let runner = eframe::WebRunner::new();
    runner
        .start(
            canvas,
            eframe::WebOptions::default(),
            Box::new(move |_| {
                Ok(Box::new(WebApp {
                    runtime: Rc::clone(&runtime),
                }))
            }),
        )
        .await?;
    // The runner owns the browser callbacks for the lifetime of the page.
    std::mem::forget(runner);
    Ok(())
}

fn attach_click_handler(runtime: &RuntimeHandle) -> Result<(), JsValue> {
    let handler = Rc::clone(runtime);
    let on_click =
        Closure::<dyn FnMut(web_sys::Event)>::wrap(Box::new(move |event: web_sys::Event| {
            let Some(target) = event
                .target()
                .and_then(|target| target.dyn_into::<Element>().ok())
            else {
                return;
            };
            if let Some(action) = target.get_attribute("data-action") {
                let mut runtime = handler.borrow_mut();
                runtime.perform_action(&action);
                // Project state changes synchronously after an explicit action;
                // the interval remains responsible for asynchronous socket data.
                runtime.poll_once();
            }
        }));
    runtime
        .borrow()
        .root
        .add_event_listener_with_callback("click", on_click.as_ref().unchecked_ref())?;
    on_click.forget();
    Ok(())
}

fn start_polling(window: &web_sys::Window, runtime: RuntimeHandle) -> Result<(), JsValue> {
    let on_tick = Closure::<dyn FnMut()>::wrap(Box::new(move || {
        runtime.borrow_mut().poll_once();
    }));
    window.set_interval_with_callback_and_timeout_and_arguments_0(
        on_tick.as_ref().unchecked_ref(),
        POLL_INTERVAL_MS,
    )?;
    on_tick.forget();
    Ok(())
}
