"""
app.py — Transducible Functions Editor (Streamlit)
===================================================
A browser-based UI for defining, composing, and running transducible
functions as live Kafka listeners.

Run with:
    cd examples/transducible_functions_editor
    streamlit run app.py

Tabs
----
1. **Registry** — browse subjects registered in the schema registry.
2. **Functions** — define transducible functions in a code editor and
   register them in the in-memory ``FunctionStore``.
3. **Pipelines** — compose registered functions into pipelines using the
   ``<<`` operator.
4. **Listeners** — start / stop background Kafka listener threads and
   watch their live log output.
"""

from __future__ import annotations

import sys
import textwrap
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

import streamlit as st

# ---------------------------------------------------------------------------
# Path setup — allow importing sibling modules without installing the package
# ---------------------------------------------------------------------------
_HERE = Path(__file__).parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

# Add the src/ directory so `agentics` is importable when running from the
# examples directory without a full `pip install -e .`
_SRC = _HERE.parent.parent / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

from function_store import FunctionStore
from listener_manager import ListenerManager
from registry_client import RegistryClient

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Transducible Functions Editor",
    page_icon="⚡",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Session-state initialisation
# ---------------------------------------------------------------------------


def _init_state() -> None:
    if "registry_url" not in st.session_state:
        st.session_state.registry_url = "http://localhost:8081"
    if "kafka_server" not in st.session_state:
        st.session_state.kafka_server = "localhost:9092"
    if "rc" not in st.session_state:
        st.session_state.rc = RegistryClient(
            registry_url=st.session_state.registry_url,
            kafka_server=st.session_state.kafka_server,
        )
    if "store" not in st.session_state:
        st.session_state.store = FunctionStore()
    if "mgr" not in st.session_state:
        st.session_state.mgr = ListenerManager(registry_client=st.session_state.rc)
    if "listener_logs" not in st.session_state:
        st.session_state.listener_logs = {}  # {listener_id: [str]}


_init_state()

rc: RegistryClient = st.session_state.rc
store: FunctionStore = st.session_state.store
mgr: ListenerManager = st.session_state.mgr

# ---------------------------------------------------------------------------
# Sidebar — connection settings
# ---------------------------------------------------------------------------

with st.sidebar:
    st.title("⚡ TF Editor")
    st.markdown("---")
    st.subheader("Connection")

    new_registry_url = st.text_input(
        "Schema Registry URL", value=st.session_state.registry_url
    )
    new_kafka_server = st.text_input(
        "Kafka Bootstrap Server", value=st.session_state.kafka_server
    )

    if st.button("🔄 Reconnect"):
        st.session_state.registry_url = new_registry_url
        st.session_state.kafka_server = new_kafka_server
        st.session_state.rc = RegistryClient(
            registry_url=new_registry_url,
            kafka_server=new_kafka_server,
        )
        st.session_state.mgr = ListenerManager(registry_client=st.session_state.rc)
        rc = st.session_state.rc
        mgr = st.session_state.mgr
        st.rerun()

    reachable = rc.is_reachable()
    if reachable:
        st.success("✓ Registry reachable")
    else:
        st.error("✗ Registry unreachable")

    st.markdown("---")
    st.subheader("Store summary")
    for kind, name, inp, tgt in store.summary():
        icon = "🔧" if kind == "function" else "🔗"
        st.markdown(f"{icon} **{name}**  \n`{inp}` → `{tgt}`")

    running = mgr.running_ids()
    if running:
        st.markdown("---")
        st.subheader(f"🟢 Running listeners ({len(running)})")
        for lid in running:
            info = mgr.get_info(lid)
            if info:
                st.markdown(f"• `{info.name}`")

# ---------------------------------------------------------------------------
# Main tabs
# ---------------------------------------------------------------------------

tab_registry, tab_functions, tab_simple, tab_pipelines, tab_listeners, tab_send = (
    st.tabs(
        [
            "📋 Registry",
            "🔧 Functions",
            "⚡ Simple Listeners",
            "🔗 Pipelines",
            "📡 Listeners",
            "📨 Send Message",
        ]
    )
)

# ===========================================================================
# TAB 1 — Registry
# ===========================================================================

with tab_registry:
    st.header("Schema Registry")

    col_refresh, col_search = st.columns([1, 3])
    with col_refresh:
        refresh = st.button("🔄 Refresh subjects")
    with col_search:
        search_term = st.text_input("Filter subjects", placeholder="e.g. Movie")

    subjects = rc.list_subjects() if reachable else []
    if search_term:
        subjects = [s for s in subjects if search_term.lower() in s.lower()]

    if not subjects:
        st.info("No subjects found (or registry unreachable).")
    else:
        st.markdown(f"**{len(subjects)} subject(s) found**")
        selected_subject = st.selectbox("Select subject to inspect", subjects)

        if selected_subject:
            versions = rc.list_versions(selected_subject)
            st.markdown(f"Versions: `{versions}`")

            schema_json = rc.get_schema_json(selected_subject)
            if schema_json:
                with st.expander("📄 JSON Schema", expanded=False):
                    st.json(schema_json)

                atype = rc.get_atype(selected_subject)
                if atype:
                    st.success(f"✓ Reconstructed Pydantic model: `{atype.__name__}`")
                    fields_info = {
                        k: str(v.annotation) for k, v in atype.model_fields.items()
                    }
                    st.table([{"field": k, "type": v} for k, v in fields_info.items()])
                else:
                    st.warning("Could not reconstruct Pydantic model from schema.")

# ===========================================================================
# TAB 2 — Functions
# ===========================================================================

with tab_functions:
    st.header("Define Transducible Functions")

    st.markdown(
        """
        Paste or write a Python snippet that defines one or more
        ``@transducible``-decorated async functions.  Click **▶ Execute**
        to evaluate the code and register the functions in the store.

        **Example**
        ```python
        from pydantic import BaseModel
        from agentics.core.transducible_functions import transducible

        class Movie(BaseModel):
            title: str
            genre: str

        class MovieSummary(BaseModel):
            title: str
            one_line_summary: str

        # Without registry enforcement (default):
        @transducible()
        async def summarise_movie(movie: Movie) -> MovieSummary:
            \"\"\"Summarise a movie in one line.\"\"\"
            return MovieSummary(
                title=movie.title,
                one_line_summary=f"{movie.title} is a {movie.genre} film.",
            )

        # With registry enforcement — raises RuntimeError if types not registered:
        # @transducible(schema_registry_url="http://localhost:8081")
        # async def summarise_movie(movie: Movie) -> MovieSummary: ...

        # With auto-registration — registers types if missing:
        # @transducible(schema_registry_url="http://localhost:8081", auto_register=True)
        # async def summarise_movie(movie: Movie) -> MovieSummary: ...
        ```
        > **Note:** `@transducible` infers `input_model` and `target_model`
        > automatically from the function's type annotations — do **not** pass
        > them as decorator arguments.
        """
    )

    default_code = textwrap.dedent(
        """\
        # Define your @transducible functions here.
        # All names defined at module level will be scanned for
        # TransducibleFunction instances and auto-registered.
        #
        # IMPORTANT: @transducible infers input/output types from the
        # function's type annotations — do NOT pass input_model or
        # target_model as decorator arguments.

        from pydantic import BaseModel
        from agentics.core.transducible_functions import transducible

        # --- define your models ---

        # class MyInput(BaseModel):
        #     ...

        # class MyOutput(BaseModel):
        #     ...

        # --- define your function ---

        # @transducible()
        # async def my_function(item: MyInput) -> MyOutput:
        #     ...
        """
    )

    code = st.text_area(
        "Function code",
        value=st.session_state.get("fn_code", default_code),
        height=350,
        key="fn_code_area",
    )

    fn_name_override = st.text_input(
        "Register as name (leave blank to auto-detect)",
        placeholder="e.g. summarise_movie",
    )

    if st.button("▶ Execute & Register"):
        try:
            local_ns: Dict[str, Any] = {}
            exec(compile(code, "<editor>", "exec"), local_ns)  # noqa: S102

            # Auto-detect TransducibleFunction instances
            from agentics.core.transducible_functions import TransducibleFunction

            registered: List[str] = []
            for var_name, obj in local_ns.items():
                if var_name.startswith("_"):
                    continue
                if isinstance(obj, TransducibleFunction):
                    reg_name = fn_name_override.strip() or var_name
                    store.add_function(reg_name, obj)
                    registered.append(reg_name)

            if registered:
                st.success(f"✓ Registered: {', '.join(registered)}")
                st.session_state.fn_code = code
            else:
                st.warning(
                    "No TransducibleFunction instances found in the executed code. "
                    "Make sure you use the @transducible decorator."
                )
        except Exception:
            st.error("❌ Execution error:")
            st.code(traceback.format_exc(), language="python")

    # Show registered functions
    fn_names = store.list_functions()
    if fn_names:
        st.markdown("---")
        st.subheader("Registered functions")
        rows = [
            {
                "name": name,
                "input_model": store.function_info(name)["input_model"],
                "target_model": store.function_info(name)["target_model"],
                "doc": store.function_info(name)["doc"],
            }
            for name in fn_names
        ]
        st.table(rows)

        remove_fn = st.selectbox("Remove function", ["—"] + fn_names, key="rm_fn")
        if st.button("🗑 Remove selected function") and remove_fn != "—":
            store.remove_function(remove_fn)
            st.rerun()

# ===========================================================================
# TAB 3 — Simple Listeners
# ===========================================================================

with tab_simple:
    st.header("⚡ Simple Transduction Listeners")

    st.markdown(
        """
        Define a **schema-enforcement listener** directly from registry types —
        no Python code required.

        Pick a **source type** and a **target type** from the schema registry,
        provide optional LLM instructions, configure the Kafka topics, and click
        **▶ Start**.  The listener will:

        1. Consume messages from the **input topic**.
        2. Validate each message against the **source schema** in the registry.
        3. Transduce each message to the **target type** using the LLM and your
           instructions (via ``AGStream.listener_with_schema_enforcement``).
        4. Produce the result to the **output topic** with schema enforcement.
        """
    )

    subjects_simple = rc.list_subjects() if reachable else []

    if not reachable:
        st.warning("⚠️ Schema registry is unreachable — cannot list types.")
    elif not subjects_simple:
        st.info("No subjects found in the registry.")
    else:
        col_s1, col_s2 = st.columns(2)

        with col_s1:
            st.subheader("Source & Target Types")
            sl_source = st.selectbox(
                "Source type (registry subject)",
                ["— none —"] + subjects_simple,
                key="sl_source",
                help="The schema subject for incoming messages (e.g. 'Movie-value').",
            )
            sl_target = st.selectbox(
                "Target type (registry subject)",
                ["— none —"] + subjects_simple,
                key="sl_target",
                help="The schema subject for outgoing messages (e.g. 'MovieSummary-value').",
            )

            # Show field previews for selected types
            if sl_source != "— none —":
                src_atype = rc.get_atype(sl_source)
                if src_atype:
                    with st.expander(
                        f"📄 Source fields: `{src_atype.__name__}`", expanded=False
                    ):
                        st.table(
                            [
                                {"field": k, "type": str(v.annotation)}
                                for k, v in src_atype.model_fields.items()
                            ]
                        )
                else:
                    st.warning(f"Could not reconstruct model for `{sl_source}`.")

            if sl_target != "— none —":
                tgt_atype = rc.get_atype(sl_target)
                if tgt_atype:
                    with st.expander(
                        f"📄 Target fields: `{tgt_atype.__name__}`", expanded=False
                    ):
                        st.table(
                            [
                                {"field": k, "type": str(v.annotation)}
                                for k, v in tgt_atype.model_fields.items()
                            ]
                        )
                else:
                    st.warning(f"Could not reconstruct model for `{sl_target}`.")

        with col_s2:
            st.subheader("Listener Configuration")
            sl_input_topic = st.text_input(
                "Input topic", value="agentics-stream", key="sl_in"
            )
            sl_output_topic = st.text_input(
                "Output topic", value="agentics-output", key="sl_out"
            )
            sl_instructions = st.text_area(
                "LLM instructions (optional)",
                placeholder=(
                    "e.g. Summarise the movie review in one sentence, "
                    "keeping the title unchanged."
                ),
                height=120,
                key="sl_instructions",
            )
            sl_verbose = st.checkbox("Verbose logging", value=True, key="sl_verbose")
            sl_produce = st.checkbox(
                "Produce results to output topic", value=True, key="sl_produce"
            )
            sl_validate = st.checkbox(
                "Validate incoming schema", value=True, key="sl_validate"
            )

        sl_name = st.text_input(
            "Listener display name (optional)",
            placeholder="e.g. movie-review-summariser",
            key="sl_name",
        )

        can_start = sl_source != "— none —" and sl_target != "— none —"
        if not can_start:
            st.info(
                "Select both a **source type** and a **target type** to enable the Start button."
            )

        if st.button("▶ Start simple listener", disabled=not can_start):
            src_val = None if sl_source == "— none —" else sl_source
            tgt_val = None if sl_target == "— none —" else sl_target
            display = sl_name.strip() or f"{src_val}→{tgt_val}"

            lid = mgr.start_simple_listener(
                input_topic=sl_input_topic,
                output_topic=sl_output_topic,
                source_atype_name=src_val,
                target_atype_name=tgt_val,
                instructions=sl_instructions.strip(),
                validate_schema=sl_validate,
                produce_results=sl_produce,
                verbose=sl_verbose,
                name=display,
            )
            st.session_state.listener_logs[lid] = []
            st.success(f"✓ Simple listener started: `{lid}`")
            st.rerun()

# ===========================================================================
# TAB 4 — Pipelines
# ===========================================================================

with tab_pipelines:
    st.header("Compose Pipelines")

    all_names = store.list_functions() + store.list_pipelines()

    if len(all_names) < 2:
        st.info(
            "Register at least **2 functions** in the Functions tab before "
            "composing a pipeline."
        )
    else:
        st.markdown(
            "Select functions in **left-to-right application order** "
            "(first function receives the input, last produces the output)."
        )

        selected = st.multiselect(
            "Functions / pipelines to compose (ordered)",
            options=all_names,
        )

        pipeline_name = st.text_input(
            "Pipeline name", placeholder="e.g. classify_then_summarise"
        )

        if st.button("🔗 Compose") and len(selected) >= 2 and pipeline_name.strip():
            try:
                store.compose(selected, pipeline_name.strip())
                st.success(f"✓ Pipeline '{pipeline_name}' created.")
                st.rerun()
            except Exception:
                st.error("❌ Composition error:")
                st.code(traceback.format_exc(), language="python")

    # Show registered pipelines
    pl_names = store.list_pipelines()
    if pl_names:
        st.markdown("---")
        st.subheader("Registered pipelines")
        rows = [
            {
                "name": name,
                "input_model": store.pipeline_info(name)["input_model"],
                "target_model": store.pipeline_info(name)["target_model"],
            }
            for name in pl_names
        ]
        st.table(rows)

        remove_pl = st.selectbox("Remove pipeline", ["—"] + pl_names, key="rm_pl")
        if st.button("🗑 Remove selected pipeline") and remove_pl != "—":
            store.remove_pipeline(remove_pl)
            st.rerun()

# ===========================================================================
# TAB 4 — Listeners
# ===========================================================================

with tab_listeners:
    st.header("Kafka Listeners")

    all_callables = store.all_callables()

    if not all_callables:
        st.info(
            "No functions or pipelines registered yet. "
            "Go to the **Functions** tab to define one."
        )
    else:
        st.subheader("Start a new listener")

        subjects = rc.list_subjects() if reachable else []

        col1, col2 = st.columns(2)
        with col1:
            fn_choice = st.selectbox(
                "Function / pipeline", list(all_callables.keys()), key="ls_fn"
            )
            input_topic = st.text_input(
                "Input topic", value="agentics-stream", key="ls_in"
            )
            output_topic = st.text_input(
                "Output topic", value="agentics-output", key="ls_out"
            )
        with col2:
            source_atype_name = st.selectbox(
                "Source atype (registry subject)",
                ["— none —"] + subjects,
                key="ls_src",
            )
            target_atype_name = st.selectbox(
                "Target atype (registry subject)",
                ["— none —"] + subjects,
                key="ls_tgt",
            )
            verbose = st.checkbox("Verbose logging", value=True, key="ls_verbose")
            produce_results = st.checkbox(
                "Produce results to output topic", value=True, key="ls_produce"
            )

        if st.button("▶ Start listener"):
            fn = all_callables[fn_choice]
            src = None if source_atype_name == "— none —" else source_atype_name
            tgt = None if target_atype_name == "— none —" else target_atype_name

            lid = mgr.start(
                fn=fn,
                input_topic=input_topic,
                output_topic=output_topic,
                name=f"{fn_choice}@{input_topic}",
                source_atype_name=src,
                target_atype_name=tgt,
                verbose=verbose,
                produce_results=produce_results,
            )
            st.session_state.listener_logs[lid] = []
            st.success(f"✓ Listener started: `{lid}`")
            st.rerun()

    # ------------------------------------------------------------------
    # Running / stopped listeners
    # ------------------------------------------------------------------
    all_listeners = mgr.list_listeners()
    if all_listeners:
        st.markdown("---")
        st.subheader("Active listeners")

        for info in all_listeners:
            status_icon = "🟢" if info.is_alive else "🔴"
            with st.expander(
                f"{status_icon} `{info.name}` — {info.status}", expanded=info.is_alive
            ):
                col_a, col_b, col_c = st.columns([2, 2, 1])
                with col_a:
                    st.markdown(
                        f"**Topics:** `{info.input_topic}` → `{info.output_topic}`"
                    )
                    st.markdown(
                        f"**Source atype:** `{info.source_atype_name or '—'}`  \n"
                        f"**Target atype:** `{info.target_atype_name or '—'}`"
                    )
                with col_b:
                    st.markdown(f"**Function:** `{info.fn_name}`")
                    elapsed = time.time() - info.started_at
                    st.markdown(f"**Elapsed:** {elapsed:.0f}s")
                with col_c:
                    if info.is_alive:
                        if st.button("⏹ Stop", key=f"stop_{info.listener_id}"):
                            mgr.stop(info.listener_id)
                            st.rerun()
                    else:
                        btn_col1, btn_col2 = st.columns(2)
                        with btn_col1:
                            if st.button(
                                "▶ Restart", key=f"restart_{info.listener_id}"
                            ):
                                new_lid = mgr.restart(info.listener_id)
                                if new_lid:
                                    st.session_state.listener_logs[new_lid] = []
                                    st.success(f"✓ Restarted as `{new_lid}`")
                                else:
                                    st.error("Could not restart listener.")
                                st.rerun()
                        with btn_col2:
                            if st.button("🗑 Remove", key=f"rm_{info.listener_id}"):
                                mgr.remove(info.listener_id)
                                st.rerun()

                # Drain and display logs
                new_lines = mgr.drain_logs(info.listener_id)
                if info.listener_id not in st.session_state.listener_logs:
                    st.session_state.listener_logs[info.listener_id] = []
                st.session_state.listener_logs[info.listener_id].extend(new_lines)

                log_text = "".join(
                    st.session_state.listener_logs[info.listener_id][-200:]
                )
                st.text_area(
                    "Logs",
                    value=log_text,
                    height=200,
                    key=f"log_{info.listener_id}",
                    disabled=True,
                )

        # Auto-refresh while any listener is running
        if mgr.running_ids():
            time.sleep(1)
            st.rerun()

# ===========================================================================
# TAB 6 — Send Message
# ===========================================================================

with tab_send:
    st.header("📨 Send Message to Listener")

    st.markdown(
        """
        Pick a **running listener**, fill in the source-type form, and click
        **▶ Send**.  The message is produced to the listener's input topic and
        the tab waits for the transduced result on the output topic.
        """
    )

    # ── Helper: produce one JSON message to a Kafka topic ──────────────────
    def _kafka_produce(bootstrap: str, topic: str, payload: dict) -> None:
        """Produce *payload* as a JSON-encoded Kafka message to *topic*."""
        import json as _json

        from kafka import KafkaProducer as _KP

        bs = bootstrap.replace("localhost", "127.0.0.1")
        producer = _KP(
            bootstrap_servers=bs,
            value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
            request_timeout_ms=10_000,
        )
        producer.send(topic, value=payload)
        producer.flush(timeout=10)
        producer.close()

    # ── Helper: poll output topic for the latest message ───────────────────
    def _kafka_poll_latest(
        bootstrap: str,
        topic: str,
        timeout_s: float = 30.0,
        poll_interval_s: float = 0.5,
    ) -> Optional[dict]:
        """
        Seek to the end of *topic*, then wait up to *timeout_s* seconds for
        a new message to arrive.  Returns the decoded JSON dict or ``None``.
        """
        import json as _json
        import time as _t
        import uuid as _uuid

        from kafka import KafkaConsumer as _KC
        from kafka import TopicPartition as _TP

        bs = bootstrap.replace("localhost", "127.0.0.1")
        group = f"editor-send-{_uuid.uuid4()}"

        consumer = _KC(
            bootstrap_servers=bs,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            group_id=group,
            value_deserializer=lambda m: _json.loads(m.decode("utf-8")),
            consumer_timeout_ms=int(timeout_s * 1000),
            request_timeout_ms=40_000,
            session_timeout_ms=30_000,
            heartbeat_interval_ms=3_000,
        )

        # Assign partitions and seek to end *before* the message is produced
        # so we only capture messages that arrive after this point.
        consumer.subscribe([topic])
        consumer.poll(timeout_ms=500, max_records=1)  # trigger assignment
        consumer.seek_to_end()

        deadline = _t.time() + timeout_s
        try:
            while _t.time() < deadline:
                batch = consumer.poll(
                    timeout_ms=int(poll_interval_s * 1000), max_records=1
                )
                for _tp, msgs in batch.items():
                    if msgs:
                        return msgs[-1].value
                _t.sleep(poll_interval_s)
        finally:
            consumer.close()
        return None

    # ── Listener picker ─────────────────────────────────────────────────────
    col_sm_refresh, _ = st.columns([1, 4])
    with col_sm_refresh:
        if st.button("🔄 Refresh listeners", key="sm_refresh"):
            st.rerun()

    all_listeners_send = mgr.list_listeners()

    if not all_listeners_send:
        st.info(
            "No listeners found.  Start one in the **⚡ Simple Listeners** "
            "or **📡 Listeners** tab first, then click **🔄 Refresh listeners** above."
        )
    else:
        listener_options = {
            f"{'🟢' if li.is_alive else '🔴'} {li.name} ({li.listener_id})": li
            for li in all_listeners_send
        }
        chosen_label = st.selectbox(
            "Select listener", list(listener_options.keys()), key="sm_listener"
        )
        chosen_info = listener_options[chosen_label]

        st.markdown(
            f"**Topics:** `{chosen_info.input_topic}` → `{chosen_info.output_topic}`  \n"
            f"**Source:** `{chosen_info.source_atype_name or '—'}`  \n"
            f"**Target:** `{chosen_info.target_atype_name or '—'}`"
        )

        # ── Fetch source schema and build dynamic form ──────────────────────
        src_subject = chosen_info.source_atype_name
        src_schema: Optional[dict] = None
        src_fields: dict = {}

        if src_subject and reachable:
            src_schema = rc.get_schema_json(src_subject)
            if src_schema:
                src_fields = src_schema.get("properties", {})

        # ── Response timeout (shown before the form) ────────────────────────
        send_timeout = st.number_input(
            "Wait for response (seconds)",
            min_value=5,
            max_value=120,
            value=30,
            key="sm_timeout",
        )

        st.markdown("---")
        st.subheader("Message fields")

        form_data: dict = {}
        json_parse_ok = True
        do_send = False

        if not src_fields:
            # Fallback: free-form JSON editor
            st.info(
                "No source schema found — enter raw JSON below."
                if src_subject
                else "No source type configured for this listener — enter raw JSON below."
            )
            raw_json = st.text_area(
                "Raw JSON payload",
                value="{}",
                height=150,
                key="sm_raw_json",
            )
            try:
                import json as _j

                form_data = _j.loads(raw_json)
            except Exception as _je:
                st.error(f"Invalid JSON: {_je}")
                json_parse_ok = False

            do_send = st.button("▶ Send", key="sm_send_raw") and json_parse_ok

        else:
            # Render one input widget per field inside a Streamlit form
            required_fields = src_schema.get("required", []) if src_schema else []

            with st.form("sm_form"):
                for field_name, field_def in src_fields.items():
                    json_type = field_def.get("type", "string")
                    label = f"{field_name}" + (
                        " *" if field_name in required_fields else ""
                    )
                    description = field_def.get("description", "")
                    help_text = description or None

                    if json_type == "boolean":
                        val = st.checkbox(
                            label, value=False, key=f"sm_{field_name}", help=help_text
                        )
                    elif json_type in ("integer", "number"):
                        val = st.number_input(
                            label, value=0, key=f"sm_{field_name}", help=help_text
                        )
                    elif json_type == "array":
                        raw = st.text_input(
                            label + " (JSON array)",
                            value="[]",
                            key=f"sm_{field_name}",
                            help=help_text,
                        )
                        try:
                            import json as _j2

                            val = _j2.loads(raw)
                        except Exception:
                            val = []
                    elif json_type == "object":
                        raw = st.text_area(
                            label + " (JSON object)",
                            value="{}",
                            height=80,
                            key=f"sm_{field_name}",
                            help=help_text,
                        )
                        try:
                            import json as _j3

                            val = _j3.loads(raw)
                        except Exception:
                            val = {}
                    else:
                        val = st.text_input(
                            label, value="", key=f"sm_{field_name}", help=help_text
                        )

                    form_data[field_name] = val

                do_send = st.form_submit_button("▶ Send")

        if do_send:
            bootstrap = st.session_state.kafka_server
            input_topic = chosen_info.input_topic
            output_topic = chosen_info.output_topic

            # Wrap in AGStream envelope so the listener can parse it
            envelope = {"states": [form_data]}

            with st.spinner(
                f"Sending to `{input_topic}` and waiting for response on `{output_topic}`…"
            ):
                try:
                    # Start polling BEFORE producing so we don't miss the reply
                    import threading as _threading

                    result_holder: List[Optional[dict]] = [None]
                    poll_done = _threading.Event()

                    def _poll_thread():
                        result_holder[0] = _kafka_poll_latest(
                            bootstrap, output_topic, timeout_s=float(send_timeout)
                        )
                        poll_done.set()

                    t = _threading.Thread(target=_poll_thread, daemon=True)
                    t.start()

                    # Small delay to let the consumer seek to end before we produce
                    time.sleep(0.8)

                    _kafka_produce(bootstrap, input_topic, envelope)
                    st.info(f"✓ Message sent to `{input_topic}`. Waiting for response…")

                    poll_done.wait(timeout=float(send_timeout) + 5)
                    result = result_holder[0]

                    if result is None:
                        st.warning(
                            f"⏳ No response received on `{output_topic}` within "
                            f"{send_timeout}s.  The listener may still be processing."
                        )
                    else:
                        st.success("✅ Response received!")
                        # Pretty-print the result
                        import json as _jout

                        # Unwrap AGStream envelope if present
                        if (
                            isinstance(result, dict)
                            and "states" in result
                            and result["states"]
                        ):
                            display_result = result["states"]
                        else:
                            display_result = result
                        st.json(display_result)

                except Exception:
                    st.error("❌ Error sending/receiving message:")
                    st.code(traceback.format_exc(), language="python")

# Made with Bob
