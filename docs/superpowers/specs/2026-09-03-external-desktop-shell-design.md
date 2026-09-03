# External Desktop Shell Design

Date: 2026-09-03

## Goal

Replace Finback's embedded terminal with a desktop-only action that opens an
interactive `kubectl exec` session in the user's system terminal. Web builds
and desktop clients connected to a non-embedded server do not expose shell
functionality.

This design deliberately delegates terminal emulation, keyboard protocols,
ANSI rendering, selection, and process lifetime to an installed terminal and
`kubectl`.

## Availability and trust boundary

The `Open shell` action is present only when all of these conditions hold:

- the frontend is the desktop application;
- the desktop application created and is connected to its embedded local
  server;
- the selected resource is a live Pod with context, namespace, pod name, UID,
  and container information; and
- a usable `kubectl` executable and platform terminal launcher are available.

The application must carry an explicit capability established when it creates
the embedded server. A loopback hostname is not proof of this capability: a
desktop client manually connected to another localhost service must not gain
the action.

The external process uses the user's local `kubectl` and Kubernetes identity.
No Finback access token, certificate, or kubeconfig content is written to the
script. Same-machine execution alone is not sufficient: when the embedded
server starts, the desktop records a `KubectlLaunchDescriptor` containing the
resolved kubectl executable, the exact ordered kubeconfig sources used to build
the server client, the selected context, and any supported non-secret client
options that kubectl must reproduce. Environment values required by kubectl or
an exec-auth plugin are classified at descriptor construction. Only explicitly
allowed non-secret values may be rendered as platform-quoted assignments in
the private script; if faithful reproduction requires a credential or other
sensitive environment value, the capability is withheld. This is necessary on
macOS, where Launch Services may reuse a terminal process and cannot reliably
forward the environment supplied to `open`.

The capability is withheld when the server configuration cannot be faithfully
expressed as a local kubectl invocation, including an in-memory or unavailable
kubeconfig. Exec-auth plugins remain kubectl's responsibility, but their
executable environment must be preserved by the descriptor. This prevents a
same-named context in a different kubeconfig from selecting another cluster or
identity.

## User experience

Shell is no longer a detail tab. A supported Pod detail exposes an `Open shell`
button in its action area. Its tooltip is `Open an interactive kubectl shell in
your system terminal`.

Clicking the button launches one independent terminal window. Finback does not
track it as a connected session. Closing the detail, changing context, or
quitting Finback does not close the terminal and requires no navigation guard.

When a Pod has multiple containers, the action uses the currently selected
container where one exists, otherwise the typed Pod projection's default
container. The action must make the chosen container visible before launch;
the existing container chooser may be reused or a compact chooser may be
placed beside the button.

Web builds and desktop connections without the explicit embedded-local
capability omit the button, shortcuts, command-palette entries, and Shell tab
entirely. They do not show a disabled or promotional placeholder.

Launch failures appear as an in-application error that says whether `kubectl`,
the terminal launcher, or temporary-script creation failed. The UI shows the
structured target but does not offer a reduced copyable command: omitting the
launch descriptor could run against a same-named context in another cluster.
Kubernetes-side failures remain visible in the launched terminal.

## Architecture

Shared UI code produces a structured request and never writes scripts or
starts processes:

```rust
struct ExternalShellTarget {
    namespace: String,
    pod: String,
    uid: String,
    container: String,
    program: String,
}

trait ExternalShellLauncher {
    fn availability(&self) -> Availability;
    fn launch(
        &self,
        target: ExternalShellTarget,
    ) -> Result<(), ExternalShellError>;
}
```

Context exists only in the immutable `KubectlLaunchDescriptor`; target data
cannot override it. `launch` requires the target and descriptor to carry the
same connection generation.

`program` initially defaults to `/bin/sh`. This version does not retry with
`/bin/bash`: an automatic retry would create a second exec attempt and obscure
the first failure. A future explicit shell-program choice can extend the
structured request.

Only the desktop composition root supplies an `ExternalShellLauncher`, and
only for the embedded-local connection. The capability and every request carry
the active connection generation. It is synchronously invalidated when the
connection is replaced, reconnects into a new generation, or switches away
from the embedded server. `launch` revalidates the generation and the target's
current detail provenance; hiding the button is presentation, not the security
check. The web composition root has no launcher.

The desktop implementation has three focused units:

1. `KubectlExecCommand` validates the target and renders a platform-safe
   script from structured fields.
2. `TemporaryShellScript` creates a private random file, owns cleanup policy,
   and never exposes credentials.
3. `SystemTerminalLauncher` selects the platform launcher and starts it using
   an executable plus argv, never an interpolated host-shell command.

Process execution is injectable so tests can inspect the selected executable
and argv without opening a real terminal.

## Generated command and Pod identity check

The interactive command is logically:

```text
kubectl --context <context> --namespace <namespace>
  exec -it <pod> --container <container> -- /bin/sh
```

Before exec, the script fetches the current Pod UID using the exact launch
descriptor, context, and namespace. It compares that value with the selected
Pod's immutable UID and refuses to continue if the Pod disappeared or was
already replaced.

This is a best-effort stale-selection check, not an atomic identity guarantee.
The Kubernetes Pod exec subresource accepts a Pod name but no UID or
resource-version precondition, so deletion and same-name recreation can race
between the check and `kubectl exec`. The external-terminal design explicitly
accepts this residual race. A strict immutable-identity guarantee would require
retaining a trusted mediated exec path and is outside this design.

Every resource value is rendered with a dedicated, unit-tested platform
quoting function. No user-controlled field is concatenated into an unquoted
command. Empty values, embedded newlines, NULs, and values that cannot be
represented safely on the target platform are rejected before a file is
created.

## Platform launch behavior

### macOS

Create an executable `.command` file with mode `0700` and launch it with
`open <absolute-script-path>`. The file association opens the user's configured
application for command files, normally a terminal. A user can change that
association, and successful `open` means only that Launch Services accepted the
request. This best-effort behavior is accepted; smoke tests verify a marker
written by the script instead of treating `open` success as proof of execution.
Failure to invoke `open` is reported immediately.

### Linux

Create an executable `.sh` file with mode `0700`. Use this ordered adapter
table; `<script>` is always one argv element:

| Probe | Executable and argv |
| --- | --- |
| Freedesktop proposal | `xdg-terminal-exec -- <script>` |
| Debian alternative | `x-terminal-emulator -e <script>` |
| GNOME Terminal | `gnome-terminal -- <script>` |
| Konsole | `konsole -e <script>` |
| Kitty | `kitty -- <script>` |

Missing executables and synchronous spawn errors fall through to the next row,
with errors retained for the final diagnostic. The first successful spawn is
accepted without waiting or trying another launcher; this avoids duplicate
windows because graphical terminal processes have inconsistent parent-process
lifetimes. Acceptance cannot prove that a graphical terminal ran the script,
so smoke tests verify a marker produced by the script. If no row is accepted,
report the problem; do not run the script invisibly.

Because Linux has no universally deployed default-terminal API, this ordered
fallback is explicitly best-effort and covered by adapter tests.

### Windows

Create a UTF-8-with-BOM, CRLF `.ps1` script rather than a batch file. Resource
values are PowerShell single-quoted literals with embedded single quotes
doubled. Start `powershell.exe` directly with
`-NoLogo -NoProfile -ExecutionPolicy Bypass -File <script>` and the Windows
`CREATE_NEW_CONSOLE` creation flag. A configured default terminal host receives
the new console on modern Windows; otherwise the system console host opens it.
The script waits on failure, so no `cmd.exe /k` or nested batch parser is
needed.

The child directory and filename use `[A-Za-z0-9_-]` only. The script path is
an argv element passed through `std::process::Command`, never part of a cmd.exe
command string. Windows CI executes the script against a fake kubectl under
real Windows PowerShell; string snapshots alone are insufficient.

## Script lifecycle

Scripts live in a stable Finback-owned private parent under the operating-system
temporary directory, with one random child per launch. Before every use the
parent must be owned by the current user, be a real directory rather than a
symlink/reparse point, and have Unix mode `0700` or an owner-only Windows ACL;
otherwise shell launch and cleanup are refused. Directory and filenames
use the fixed safe ASCII alphabet and contain no resource names. Unix
directories are mode `0700`; files use atomic create-new/no-follow semantics
and mode `0700`, without a window in which broader permissions are observable.
Windows creates the directory with an owner-only ACL and rejects reparse-point
substitution before file creation and cleanup.

The script performs these steps:

1. verify the selected Pod UID;
2. run `kubectl exec -it`;
3. capture and print a non-zero exit status;
4. wait for acknowledgement when an error would otherwise close the terminal;
5. remove itself and exit with the kubectl status.

If terminal launch fails synchronously, Finback removes the script immediately.
Because the terminal process is detached, Finback does not infer session exit
from the launcher process. On desktop startup, Finback sorts validated direct
children by age and examines the oldest 128, removing launch directories older than
24 hours only when they are owned by the current user, are not symlinks/reparse
points, and contain only the expected manifest and `.command`, `.sh`, or `.ps1`
regular file. Cleanup is non-recursive outside each validated launch directory
and never targets the general system temporary directory or an unvalidated
lookalike.

At the end, Unix scripts unlink the script and manifest and remove their now
empty launch directory. PowerShell uses `Remove-Item -LiteralPath
$PSCommandPath` after command completion, removes the validated manifest with
the same literal-path operation, rechecks that their parent is not a reparse
point, and removes the empty launch directory while preserving the captured
kubectl exit status. PowerShell reads the script before executing these final
statements, so no nested `-Command` or deferred cleanup process is required.
Startup cleanup remains the backstop when self-cleanup fails. Windows execution
tests assert removal of the script, manifest, and launch directory.

## Removal of embedded exec

The old embedded shell is removed rather than left as an unreachable second
implementation:

- remove the Shell detail tab, line input, scrollback, session state, and
  connected-shell navigation guard;
- remove shell shortcuts and command-palette actions;
- remove client exec stream-ticket, WebSocket, stdin, resize, and signal
  projection paths;
- remove the server exec WebSocket route and Kubernetes attached-exec session
  management;
- remove protocol messages used only by exec while retaining the log-stream
  contract.

Finback protocol major 1 promises compatibility with the current and previous
minor, so removal occurs at a protocol-minor boundary without reusing old
numeric or binary discriminants. For one compatibility window, legacy decode
support retains `EXEC_PATH`, `StreamType::Exec`, the exec stream-ticket request
shape, and a tombstone route. Ticket requests receive the existing typed
`unsupportedMessage` control error; direct `/api/v1/exec` upgrades authenticate
and then close with the existing typed unsupported-feature stream error. No
ticket is issued and no Kubernetes exec can start. Old clients therefore fail
closed. The advertised exec capability is removed immediately.

Active functionality removed immediately includes `StreamRoute::Exec`, client
ticket construction and socket handling, stdin/resize/TTY-output production,
server exec upgrade dispatch beyond the tombstone, backend `ExecSessions`, fake
exec state, shell UI/session/guard state, and their execution tests. Shared log
ticket and binary-frame code remains. After the compatibility window, the
legacy constants, discriminants, decode branch, and tombstone route are removed.
Tests cover old-client/new-server typed rejection, new-client/old-server
non-use, current/previous minor negotiation, and the absence of every path from
the tombstone to Kubernetes exec.

## Error handling

Errors before a terminal opens are typed and shown by Finback. They distinguish
invalid target data, temporary storage failure, missing kubectl, unsupported
terminal environment, and launcher failure. Any created file is cleaned up on
these paths.

Errors after launch are printed by the script. Guaranteed categories are UID
lookup failure, UID mismatch, and exec failure; kubectl's stderr is otherwise
passed through verbatim rather than parsed into unstable categories. The
script does not contain secrets or echo environment variables. Application
logs record only an error category and platform, not the script body or
sensitive paths.

No runnable copy fallback is provided. Structured context, namespace, Pod, and
container values may be shown for diagnosis, but the UI does not construct a
command that omits descriptor state.

## Verification

Automated coverage includes:

- capability projection: supported desktop-local views show the action, while
  web and remote desktop views have no shell UI or shortcut;
- capability generation rejection after embedded-to-remote switching,
  reconnect, and delayed action delivery;
- descriptor context authority and generation mismatch rejection;
- exact kubeconfig reproduction, including two kubeconfig files containing the
  same context name and an unreproducible in-memory configuration;
- container selection and complete structured target construction;
- POSIX and Windows rendering with spaces, quotes, `$()`, backticks, `%`, `&`,
  newlines, Unicode, and other injection-oriented inputs;
- hermetic script execution against a fake kubectl that records argv and
  environment, covering Pod UID match, mismatch, missing Pod, kubectl failure,
  no exec after mismatch, and exit-status preservation;
- launcher selection and exact argv for macOS, Linux fallbacks, and Windows;
- secure file creation, synchronous-failure cleanup, eventual self-cleanup,
  symlink/reparse substitution, attacker-created lookalikes, expired and live
  entries, and bounded removal of owned files;
- application navigation and shutdown no longer being guarded by an external
  shell;
- typed behavior of the compatibility tombstone, proof that it cannot reach
  Kubernetes exec, and removal of active exec paths while log streaming
  continues to pass its existing tests.

POSIX scripts execute under the supported system shell in Unix CI; PowerShell
scripts execute under real Windows PowerShell in Windows CI. Platform smoke
tests open a real terminal and verify a script-written marker before release
on macOS, representative GNOME and KDE Linux desktops, and Windows with modern
and legacy console hosts.

## Non-goals

- Embedding a terminal emulator in egui.
- Providing shell access in the web client or over a remote Finback server.
- Bundling kubectl, a terminal application, kubeconfig, or credentials.
- Monitoring, reconnecting, or terminating the external shell from Finback.
- Automatically choosing among `/bin/sh`, `/bin/bash`, or application-specific
  shells after an exec failure.
