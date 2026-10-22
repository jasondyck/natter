mod api;
mod call;
mod passkey;

use call::{Call, CallState, PeerQuality, Phase, Quality};
use leptos::prelude::*;
use leptos::task::spawn_local;
use natter_protocol::{
    CeremonyFinish, CeremonyStart, InviteCreated, LoginStart, Me, RegisterStart,
};

/// localStorage key for the last name signed in with, so login is one click.
const NAME_KEY: &str = "natter.name";

fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(App);
}

#[component]
fn App() -> impl IntoView {
    // None while the first /api/me is in flight.
    let me = RwSignal::new(None::<Option<Me>>);
    let invite = RwSignal::new(invite_from_hash());

    spawn_local(async move {
        match api::get::<Me>("/api/me").await {
            Ok(m) => me.set(Some(Some(m))),
            Err(_) => me.set(Some(None)),
        }
    });

    let signed_in = Callback::new(move |m: Me| {
        remember_name(&m.name);
        clear_hash();
        invite.set(None);
        me.set(Some(Some(m)));
    });

    view! {
        <main class="card">
            {move || match me.get() {
                None => view! { <p class="hint">"Loading…"</p> }.into_any(),
                Some(None) => match invite.get() {
                    Some(token) => view! { <SignUp token signed_in /> }.into_any(),
                    None => view! { <SignIn signed_in /> }.into_any(),
                },
                Some(Some(m)) => {
                    view! { <Room me=m on_sign_out=move || me.set(Some(None)) /> }.into_any()
                }
            }}
        </main>
    }
}

#[component]
fn SignIn(signed_in: Callback<Me>) -> impl IntoView {
    view! {
        <header><h1>"natter"</h1></header>
        <PasskeyForm
            label="Name"
            button="Sign in with passkey"
            name=RwSignal::new(remembered_name())
            autofocus=false
            signed_in
            act=sign_in
        />
        <p class="hint">"New here? Ask someone for an invite link."</p>
    }
}

#[component]
fn SignUp(token: String, signed_in: Callback<Me>) -> impl IntoView {
    let act = move |name: String| {
        let req = RegisterStart {
            invite: Some(token.clone()),
            name: Some(name),
        };
        async move { register(&req).await }
    };
    view! {
        <header><h1>"Join natter"</h1></header>
        <PasskeyForm
            label="Pick a name"
            button="Create passkey"
            name=RwSignal::new(String::new())
            autofocus=true
            signed_in
            act
        />
        <p class="hint">
            "Your device will save a passkey for this site. There's no password."
        </p>
    }
}

/// A name and a passkey button, as signing in and up both are: `act` runs
/// the passkey ceremony for the name, and its account is signed in.
#[component]
fn PasskeyForm<F, Fut>(
    label: &'static str,
    button: &'static str,
    name: RwSignal<String>,
    autofocus: bool,
    signed_in: Callback<Me>,
    act: F,
) -> impl IntoView
where
    F: Fn(String) -> Fut + 'static,
    Fut: std::future::Future<Output = Result<Me, String>> + 'static,
{
    let busy = RwSignal::new(false);
    let error = RwSignal::new(None::<String>);

    let submit = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        busy.set(true);
        error.set(None);
        let attempt = act(name.get_untracked());
        spawn_local(async move {
            match attempt.await {
                Ok(m) => signed_in.run(m),
                Err(e) => error.set(Some(e)),
            }
            busy.set(false);
        });
    };

    view! {
        <ErrorLine error />
        <form on:submit=submit>
            <label>
                {label}
                <input bind:value=name required autocomplete="username" autofocus=autofocus />
            </label>
            <button type="submit" disabled=move || busy.get()>
                {move || if busy.get() { "Waiting for passkey…" } else { button }}
            </button>
        </form>
    }
}

#[component]
fn Room(me: Me, on_sign_out: impl Fn() + Clone + 'static) -> impl IntoView {
    let room = RwSignal::new("lobby".to_owned());
    let muted = RwSignal::new(false);
    let in_call = RwSignal::new(false);
    let state = CallState::new();
    // JS objects aren't Send, so the call lives in local storage.
    let call = StoredValue::new_local(None::<Call>);
    let notice = RwSignal::new(None::<String>);
    let invite_url = RwSignal::new(None::<String>);

    let join = move |ev: leptos::ev::SubmitEvent| {
        ev.prevent_default();
        state.phase.set(Phase::Connecting);
        let room = room.get_untracked();
        spawn_local(async move {
            match Call::start(room, state).await {
                Ok(c) => {
                    call.set_value(Some(c));
                    in_call.set(true);
                }
                Err(e) => state.phase.set(Phase::Failed(e)),
            }
        });
    };
    let leave = move || {
        call.set_value(None);
        in_call.set(false);
        muted.set(false);
        state.reset();
    };
    let toggle_mute = move |_| {
        let m = !muted.get_untracked();
        muted.set(m);
        call.with_value(|c| {
            if let Some(c) = c {
                c.set_muted(m);
            }
        });
    };
    let invite = move |_| {
        spawn_local(async move {
            match api::post::<_, InviteCreated>("/api/invites", &()).await {
                Ok(i) => invite_url.set(Some(i.url)),
                Err(e) => notice.set(Some(e)),
            }
        });
    };
    let add_passkey = move |_| {
        notice.set(None);
        spawn_local(async move {
            let msg = match register(&RegisterStart::default()).await {
                Ok(_) => "Passkey added.".to_owned(),
                Err(e) => e,
            };
            notice.set(Some(msg));
        });
    };
    let sign_out = move |_| {
        leave();
        let on_sign_out = on_sign_out.clone();
        spawn_local(async move {
            let _ = api::post_empty("/api/logout").await;
            on_sign_out();
        });
    };

    let error = move || match state.phase.get() {
        Phase::Failed(e) => Some(e),
        _ => None,
    };
    let status = move || match state.phase.get() {
        Phase::Idle => "Idle",
        Phase::Connecting => "Connecting…",
        Phase::Connected => "Connected",
        Phase::Failed(_) => "Disconnected",
    };

    view! {
        <header>
            <h1>"natter"</h1>
            <Show when=move || in_call.get()>
                <span class="status" class:live=move || state.phase.get() == Phase::Connected>
                    {status}
                </span>
            </Show>
        </header>
        <ErrorLine error=Signal::derive(error) />
        <Show
            when=move || in_call.get()
            fallback=move || {
                view! {
                    <form on:submit=join>
                        <label>"Room" <input bind:value=room required /></label>
                        <button
                            type="submit"
                            disabled=move || state.phase.get() == Phase::Connecting
                        >
                            {move || {
                                if state.phase.get() == Phase::Connecting {
                                    "Joining…"
                                } else {
                                    "Join"
                                }
                            }}
                        </button>
                    </form>
                }
            }
        >
            <p class="room">"Room " <strong>{move || room.get()}</strong></p>
            <ul class="peers">
                <For each=move || state.peers.get() key=|p| p.sid.clone() let:peer>
                    <li class:speaking={
                        let sid = peer.sid.clone();
                        move || state.speaking.get().contains(&sid)
                    }>
                        <span class="dot"></span>
                        {peer.name.clone()}
                        <Show when={
                            let sid = peer.sid.clone();
                            move || state.me.get().as_ref() == Some(&sid)
                        }>
                            <span class="you">"you"</span>
                        </Show>
                        <QualityBars quality={
                            let sid = peer.sid.clone();
                            move || state.quality.get().get(&sid).cloned()
                        } />
                    </li>
                </For>
            </ul>
            <div class="actions">
                <button on:click=toggle_mute class:muted=move || muted.get()>
                    {move || if muted.get() { "Unmute" } else { "Mute" }}
                </button>
                <button class="secondary" on:click=move |_| leave()>"Leave"</button>
            </div>
        </Show>
        <Show when=move || invite_url.get().is_some()>
            <div class="invite">
                <p class="hint">"Single use, expires in 7 days:"</p>
                <div class="copy">
                    <input readonly prop:value=move || invite_url.get().unwrap_or_default() />
                    <button
                        class="secondary"
                        on:click=move |_| copy(&invite_url.get_untracked().unwrap_or_default())
                    >
                        "Copy"
                    </button>
                </div>
            </div>
        </Show>
        <Show when=move || notice.get().is_some()>
            <p class="hint">{move || notice.get()}</p>
        </Show>
        <footer>
            <span>{me.name}</span>
            <button class="link" on:click=invite>"Invite"</button>
            <button class="link" on:click=add_passkey>"Add passkey"</button>
            <button class="link" on:click=sign_out>"Sign out"</button>
        </footer>
    }
}

#[component]
fn ErrorLine(#[prop(into)] error: Signal<Option<String>>) -> impl IntoView {
    move || error.get().map(|e| view! { <p class="error">{e}</p> })
}

async fn sign_in(name: String) -> Result<Me, String> {
    let start: CeremonyStart = api::post("/api/login/start", &LoginStart { name }).await?;
    let credential = passkey::get(&start.options).await?;
    api::post(
        "/api/login/finish",
        &CeremonyFinish {
            ceremony: start.ceremony,
            credential,
        },
    )
    .await
}

/// Creates a passkey: a new account when `req` carries an invite, otherwise
/// another passkey for the signed-in account.
async fn register(req: &RegisterStart) -> Result<Me, String> {
    let start: CeremonyStart = api::post("/api/register/start", req).await?;
    let credential = passkey::create(&start.options).await?;
    api::post(
        "/api/register/finish",
        &CeremonyFinish {
            ceremony: start.ceremony,
            credential,
        },
    )
    .await
}

/// The token from an invite link, `/#invite=<token>`. The fragment never
/// reaches server logs.
fn invite_from_hash() -> Option<String> {
    let hash = web_sys::window()?.location().hash().ok()?;
    hash.strip_prefix("#invite=")
        .filter(|t| !t.is_empty())
        .map(str::to_owned)
}

fn clear_hash() {
    if let Some(history) = web_sys::window().and_then(|w| w.history().ok()) {
        let _ = history.replace_state_with_url(&wasm_bindgen::JsValue::NULL, "", Some("/"));
    }
}

fn storage() -> Option<web_sys::Storage> {
    web_sys::window()?.local_storage().ok().flatten()
}

fn remembered_name() -> String {
    storage()
        .and_then(|s| s.get_item(NAME_KEY).ok().flatten())
        .unwrap_or_default()
}

fn remember_name(name: &str) {
    if let Some(s) = storage() {
        let _ = s.set_item(NAME_KEY, name);
    }
}

fn copy(text: &str) {
    if let Some(w) = web_sys::window() {
        let _ = w.navigator().clipboard().write_text(text);
    }
}

/// Three signal bars for a peer's connection, empty until the first report.
#[component]
fn QualityBars(quality: impl Fn() -> Option<PeerQuality> + Send + Sync + 'static) -> impl IntoView {
    let quality = Signal::derive(quality);
    let class = move || {
        let level = match quality.get().map(|q| q.quality) {
            None => "unknown",
            Some(Quality::Excellent) => "excellent",
            Some(Quality::Good) => "good",
            Some(Quality::Poor) => "poor",
            Some(Quality::Lost) => "lost",
        };
        format!("quality {level}")
    };
    let title = move || match quality.get() {
        None => "Connection quality: measuring".to_owned(),
        Some(q) => format!("Connection quality: {:?} (MOS {:.1})", q.quality, q.mos),
    };
    view! {
        <span class=class title=title aria-label=title role="img">
            <i></i>
            <i></i>
            <i></i>
        </span>
    }
}
