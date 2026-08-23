//! WASM entry point hosting the connection gate and the foundation web view.
//!
//! Only the socket scheme and authority are taken from `window.location`; the
//! path is always replaced with the root-level control route. The access token
//! lives solely in the gate's ephemeral buffer and is handed straight to the
//! protocol client as connection state, never persisted or placed in a URL.

use std::cell::RefCell;
use std::rc::Rc;

use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen::{JsCast, JsValue, closure::Closure};
use web_sys::{Document, Element, HtmlInputElement};

use k10s_ui::{AppView, ConnectionGate, K10sApp, derive_control_url};

/// Foundation poll cadence in milliseconds.
const POLL_INTERVAL_MS: i32 = 50;

/// Shared web-host runtime handle.
type RuntimeHandle = Rc<RefCell<Runtime>>;

/// Which top-level view the web host currently renders.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Gate,
    Connecting,
    Ready,
    Failure,
}

/// Web-host runtime bridging the shared gate and protocol client to the DOM.
#[derive(Debug)]
struct Runtime {
    stage: Stage,
    document: Document,
    root: Element,
    gate: ConnectionGate,
    app: Option<K10sApp>,
}

impl Runtime {
    /// Submit the current input buffer through the gate and open the client.
    ///
    /// An empty buffer connects as-is so tokenless loopback development servers
    /// remain reachable from the served page.
    fn begin_connection(&mut self) {
        if self.stage == Stage::Ready || self.stage == Stage::Connecting {
            return;
        }
        let token = self.token_input_value();
        self.gate.set_token_input(token);
        let target = self.gate.begin_connection();
        match K10sApp::connect(target) {
            Ok(app) => {
                self.app = Some(app);
                self.render_connecting();
            }
            Err(error) => {
                self.app = None;
                self.render_failure(&error.to_string());
            }
        }
    }

    /// Drain transport events and follow the shared application view.
    fn poll_once(&mut self) {
        if self.app.is_none() {
            return;
        }
        let outcome = match self.app.as_mut() {
            Some(app) => {
                app.poll();
                Some(app.view().clone())
            }
            None => None,
        };
        match outcome {
            Some(AppView::Connecting) | None => {}
            Some(AppView::Ready { context_names, .. }) => {
                if self.stage != Stage::Ready {
                    // Authentication completed: mark the gate lifecycle done and
                    // discard any residual credential bytes in its buffer.
                    self.gate.authentication_succeeded();
                }
                self.render_ready(&context_names);
            }
            // Terminal authentication rejection returns the user to the gate.
            Some(AppView::Failed { .. })
                if self
                    .app
                    .as_ref()
                    .is_some_and(K10sApp::requires_connection_gate) =>
            {
                self.app = None;
                self.gate.authentication_rejected();
                self.render_gate();
            }
            Some(AppView::Failed { message }) => {
                self.app = None;
                self.render_failure(&message);
            }
        }
    }

    fn render_gate(&mut self) {
        self.render_stage(Stage::Gate);
        self.append_heading("Connect to k10s");
        if let Some(error) = self.gate.error() {
            self.append_text("p", error);
        }
        self.append_token_form();
    }

    fn render_ready(&mut self, context_names: &[String]) {
        if self.stage == Stage::Ready {
            return;
        }
        self.render_stage(Stage::Ready);
        self.append_heading("Kubernetes contexts");
        let list = self.create_element("ul");
        for name in context_names {
            let item = self.create_element("li");
            item.set_text_content(Some(name));
            list.append_child(&item)
                .expect("fresh list accepts children");
        }
        self.root
            .append_child(&list)
            .expect("root accepts children");
    }

    fn render_failure(&mut self, message: &str) {
        self.render_stage(Stage::Failure);
        self.append_text("p", &format!("Connection failed: {message}"));
    }

    fn render_connecting(&mut self) {
        self.render_stage(Stage::Connecting);
        self.append_text("p", "Connecting");
    }

    /// Re-render only when the top-level view actually changes.
    fn render_stage(&mut self, stage: Stage) {
        if self.stage == stage && stage != Stage::Gate {
            return;
        }
        self.stage = stage;
        while let Some(child) = self.root.first_child() {
            self.root
                .remove_child(&child)
                .expect("root child removal cannot fail");
        }
    }

    fn append_heading(&self, text: &str) {
        self.append_text("h1", text);
    }

    /// Append an element whose entire content is safe text, never markup.
    fn append_text(&self, tag: &str, text: &str) {
        let element = self.create_element(tag);
        element.set_text_content(Some(text));
        self.root
            .append_child(&element)
            .expect("root accepts children");
    }

    fn append_token_form(&self) {
        let label = self.create_element("label");
        label
            .set_attribute("for", "access-token")
            .expect("static attributes are valid");
        label.set_text_content(Some("Access token"));
        self.root
            .append_child(&label)
            .expect("root accepts children");

        let input = self
            .document
            .create_element("input")
            .expect("input is a valid tag name")
            .dyn_into::<HtmlInputElement>()
            .expect("input creates an HTML input element");
        input.set_id("access-token");
        input
            .set_attribute("type", "password")
            .expect("static attributes are valid");
        self.root
            .append_child(&input)
            .expect("root accepts children");

        let button = self.create_element("button");
        button.set_text_content(Some("Connect"));
        self.root
            .append_child(&button)
            .expect("root accepts children");
    }

    fn create_element(&self, tag: &str) -> Element {
        self.document
            .create_element(tag)
            .expect("all used tag names are valid")
    }

    /// Read the ephemeral credential buffer straight from the DOM input.
    fn token_input_value(&self) -> String {
        self.document
            .get_element_by_id("access-token")
            .and_then(|element| element.dyn_into::<HtmlInputElement>().ok())
            .map(|input| input.value())
            .unwrap_or_default()
    }
}

#[wasm_bindgen(start)]
fn main() -> Result<(), JsValue> {
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

    let runtime: RuntimeHandle = Rc::new(RefCell::new(Runtime {
        stage: Stage::Gate,
        gate: ConnectionGate::new(control_url),
        app: None,
        root,
        document,
    }));
    runtime.borrow_mut().render_gate();

    attach_click_handler(&runtime)?;
    start_polling(&window, runtime)?;
    Ok(())
}

/// Delegate Connect clicks through the stable root so re-renders need no rebinding.
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
            if target.tag_name() != "BUTTON" {
                return;
            }
            handler.borrow_mut().begin_connection();
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
