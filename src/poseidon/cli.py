"""Command-line interface.

    poseidon run                    start the platform (24/7 service entry point)
    poseidon doctor                 self-diagnostics
    poseidon vault init|unlock-check|set|rm|rekey|list
    poseidon config validate|example
    poseidon audit verify|tail
    poseidon update check|apply
    poseidon cycle                  trigger one review cycle and exit
    poseidon research factors       offline point-in-time IC/IR factor ranking
"""

from __future__ import annotations

import argparse
import asyncio
import getpass
import sys
from importlib.resources.abc import Traversable
from pathlib import Path

import httpx

from . import __version__
from .core.config import AIConfig, AppConfig, default_config_dir, load_config
from .core.errors import ConfigError, PoseidonError, VaultError
from .core.logging import configure_logging
from .security.vault import Vault


def _load(args: argparse.Namespace) -> AppConfig:
    return load_config(Path(args.config) if args.config else None)


def _vault_for(config: AppConfig) -> Vault:
    return Vault(config.data_dir / "vault.bin")


def _unlock(vault: Vault, *, interactive_ok: bool = True) -> None:
    if vault.unlock_from_environment():
        return
    if not interactive_ok:
        raise VaultError(
            "no vault passphrase available — set POSEIDON_VAULT_PASSPHRASE(_FILE) "
            "or a systemd credential (docs/security.md)"
        )
    vault.unlock(getpass.getpass("Vault passphrase: "))


# ---------------------------------------------------------------- commands


def cmd_run(args: argparse.Namespace) -> int:
    config = _load(args)
    configure_logging(config.data_dir / "logs", config.log_level)
    vault = _vault_for(config)
    if not vault.exists:
        print("No vault found. Run `poseidon vault init` first.", file=sys.stderr)
        return 2
    _unlock(vault, interactive_ok=sys.stdin.isatty())

    from .app import ApplicationKernel

    async def main() -> None:
        kernel = ApplicationKernel(config, vault)
        await kernel.start()
        await kernel.run_forever()

    asyncio.run(main())
    return 0


def cmd_app(args: argparse.Namespace) -> int:
    """Open the dashboard as a desktop window (starting the engine's systemd
    service if needed). The engine keeps trading when the window closes."""
    config = _load(args)
    from .gui import launch

    host = config.dashboard.host
    if host in ("0.0.0.0", "::"):  # wildcard bind — connect via loopback
        host = "127.0.0.1"
    elif ":" in host:  # bare IPv6 literal (e.g. ::1) must be bracketed in a URL
        host = f"[{host}]"

    from .core.config import dashboard_token_from_env

    token = dashboard_token_from_env()
    if token is None and config.dashboard.auth_token_credential:
        vault = _vault_for(config)
        if not vault.exists:
            print("The dashboard requires an auth token but no vault exists. "
                  "Run `poseidon vault init` first.", file=sys.stderr)
            return 2
        _unlock(vault, interactive_ok=sys.stdin.isatty())
        token = vault.get(config.dashboard.auth_token_credential)
    return launch(f"http://{host}:{config.dashboard.port}", token=token)


def cmd_cycle(args: argparse.Namespace) -> int:
    config = _load(args)
    configure_logging(config.data_dir / "logs", config.log_level)
    vault = _vault_for(config)
    _unlock(vault)

    from .app import ApplicationKernel

    async def main() -> None:
        kernel = ApplicationKernel(config, vault)
        await kernel.start()
        try:
            await kernel.run_review_cycle()
        finally:
            await kernel.stop()

    asyncio.run(main())
    return 0


def _symbols_from_lines(lines: list[str]) -> list[str]:
    """Parse a one-symbol-per-line list: strip whitespace, skip blank and ``#``-comment
    lines (a leading ``#`` header would otherwise load as a bogus symbol), upcase, and
    order-preservingly dedupe. Shared by --symbols-file and the bundled --universe file."""
    seen: dict[str, None] = {}
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        seen.setdefault(s.upper())
    return list(seen)


def _universe_file(name: str) -> Traversable:
    """Resolve a bundled research universe (e.g. ``sp500``) to its packaged data file.
    Read at the CLI edge only — research/ never reads it — so the offline lab stays
    severed from I/O. Returns a Traversable via importlib.resources (the wheel ships the
    non-.py data files under poseidon/research/data/); ``.read_text`` works whether the
    install is an unpacked tree or a zip."""
    from importlib.resources import files
    return files("poseidon.research") / "data" / f"{name}.txt"


def _research_symbols(args: argparse.Namespace, config: AppConfig) -> list[str]:
    """Resolve the research universe from --symbols, --symbols-file, --universe, or
    --watchlist (checked in that order; the first one supplied wins). Empty list means
    the caller should print a usage message and exit non-zero."""
    if args.symbols:
        return [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    if args.symbols_file:
        try:
            lines = Path(args.symbols_file).read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            print(f"cannot read --symbols-file {args.symbols_file}: {exc}", file=sys.stderr)
            return []
        return _symbols_from_lines(lines)
    if args.universe:
        try:
            lines = _universe_file(args.universe).read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            print(f"cannot read bundled universe {args.universe!r}: {exc}", file=sys.stderr)
            return []
        return _symbols_from_lines(lines)
    if args.watchlist:
        return config.all_watchlist_symbols()
    return []


def cmd_research(args: argparse.Namespace) -> int:
    config = _load(args)
    configure_logging(config.data_dir / "logs", config.log_level)

    symbols = _research_symbols(args, config)
    if not symbols:
        print(
            "no symbols to research — pass --symbols A,B,C, --symbols-file PATH "
            "(one symbol per line), or --watchlist (needs a configured watchlist)",
            file=sys.stderr,
        )
        return 2

    vault = _vault_for(config)
    _unlock(vault)

    from .app import ApplicationKernel
    from .research.factors import ALL_FACTORS
    from .research.ic import NullSpec
    from .research.loader import load_history
    from .research.report import run_report

    # Random-control null configuration (design §4.7): seeds/threshold/min_n_eff come
    # from config.research; --train-frac (a per-run knob) overrides config.train_frac
    # when supplied. Every field is an explicit config value — never wall-clock.
    rc = config.research
    null = NullSpec(
        n_seeds=rc.null_seeds,
        base_seed=rc.null_base_seed,
        train_frac=(args.train_frac if args.train_frac is not None else rc.train_frac),
        alpha_t_threshold=rc.alpha_t_threshold,
        min_n_eff=rc.verdict_min_n_eff,
    )

    async def main() -> int:
        kernel = ApplicationKernel(config, vault)
        # Reuses the kernel's provider wiring without the side effects of a full
        # start() (DB open, audit-chain verify, broker connect) — this command
        # only ever reads bars. _build_router only touches config + vault, both
        # already constructed above, so calling it standalone is safe.
        router = kernel._build_router()  # noqa: SLF001 — CLI is a trusted caller
        days = args.days if args.days is not None else rc.lookback_days
        hist = await load_history(router, symbols, days)
        if len(hist) < 2:
            print(
                "not enough symbols with usable history to compute cross-sectional IC",
                file=sys.stderr,
            )
            return 1
        rep = run_report(
            ALL_FACTORS,
            hist,
            horizon=args.horizon if args.horizon is not None else rc.horizon,
            rebalance_every=(args.rebalance_every if args.rebalance_every is not None
                             else rc.rebalance_every),
            horizons=rc.horizons,
            min_cross=rc.min_cross,
            null=null,
            n_groups=rc.n_groups,
            # --universe labels the report with a survivorship caveat (§4.8); "" stays silent.
            universe_note=args.universe,
        )
        print(rep.render())
        return 0

    return asyncio.run(main())


def _listed_model_ids(response: httpx.Response) -> set[str]:
    """Model ids from an OpenAI-compatible ``/v1/models`` body.

    Returns an empty set when the body is not the expected shape, so a server
    that shapes its listing differently degrades to a plain reachability check
    rather than reporting a false failure.
    """
    try:
        payload = response.json()
    except ValueError:
        return set()
    if not isinstance(payload, dict):
        return set()
    data = payload.get("data")
    if not isinstance(data, list):
        return set()
    return {m["id"] for m in data if isinstance(m, dict) and isinstance(m.get("id"), str)}


def probe_model_backend(
    ai: AIConfig,
    api_key: str | None,
    *,
    transport: httpx.BaseTransport | None = None,
) -> tuple[bool, str]:
    """Reachability liveness probe for the configured model backend.

    Returns ``(ok, detail)``. ``transport`` lets tests inject an
    ``httpx.MockTransport`` (a fake backend) so no network call is made.
    Read-only: issues a single GET and never places or triggers any order.
    """
    if ai.backend == "openai_compatible":
        base = (ai.base_url or "").rstrip("/")
        try:
            with httpx.Client(timeout=5.0, transport=transport) as c:
                response = c.get(f"{base}/models")
                response.raise_for_status()
            # Reachability alone is a FALSE GREEN. /v1/models lists the models
            # the server has DOWNLOADED, not the one it has LOADED — so a
            # backend whose VRAM is held by a different model still answers 200
            # and lists the configured id, while every completion returns 400.
            # Observed live: doctor reported OK while no cycle could run.
            served = _listed_model_ids(response)
            if served and ai.model not in served:
                return False, (
                    f"backend reachable at {base} but it is not serving "
                    f"{ai.model!r} — available: {', '.join(sorted(served)[:6])}. "
                    "Load the configured model (or change ai.model)"
                )
            return True, f"reachable at {base}, serving {ai.model!r}"
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            # Connect phase failed — nothing is listening. Actionable: start it.
            return False, (
                f"unreachable at {base} — start LM Studio / the model server ({exc})"
            )
        except httpx.HTTPStatusError as exc:
            # We connected and got a response, just a bad status (e.g. a running
            # LM Studio returning 500) — the server is up, don't say "start it".
            return False, (
                f"backend reachable at {base} but returned {exc.response.status_code}"
            )
        except httpx.HTTPError as exc:
            # Any other transport-level failure (read timeout, protocol error…).
            return False, f"backend probe to {base} failed ({exc})"
    try:  # anthropic: cheap authed liveness
        with httpx.Client(timeout=5.0, transport=transport) as c:
            r = c.get(
                "https://api.anthropic.com/v1/models",
                headers={"x-api-key": api_key or "", "anthropic-version": "2023-06-01"},
            )
        if r.status_code == 401:
            return False, "Anthropic API reachable but key rejected (401)"
        return True, "Anthropic API reachable"
    except httpx.HTTPError as exc:
        return False, f"cannot reach Anthropic API ({exc})"


def cmd_doctor(args: argparse.Namespace) -> int:
    """Self-diagnostics: config, vault, calendar, DB, providers, broker, AI key."""
    problems = 0

    def check(label: str, ok: bool, detail: str = "") -> None:
        nonlocal problems
        mark = "OK " if ok else "FAIL"
        print(f"[{mark}] {label}" + (f" — {detail}" if detail else ""))
        if not ok:
            problems += 1

    try:
        config = _load(args)
        check("configuration parses", True, f"mode={config.mode.value}")
    except ConfigError as exc:
        check("configuration parses", False, str(exc))
        return 1

    from .core.clock import MarketClock, calendar_covers

    clock = MarketClock()
    check("holiday calendar covers today", calendar_covers(clock.now_eastern().date()))
    check("data providers configured", bool(config.data.providers),
          ", ".join(p.name for p in config.data.providers if p.enabled) or "none")
    primary = config.primary_broker()
    check("primary broker configured",
          config.mode.value == "research" or primary is not None,
          (primary.name if primary is not None else "none"))

    vault = _vault_for(config)
    check("vault exists", vault.exists, str(config.data_dir / "vault.bin"))
    if vault.exists:
        try:
            _unlock(vault)
            check("vault unlocks", True)
            names = set(vault.names())
            check("anthropic api key stored", config.ai.api_key_credential in names,
                  config.ai.api_key_credential)
            key_or_none = (
                vault.get(config.ai.api_key_credential)
                if config.ai.backend == "anthropic"
                and config.ai.api_key_credential in names
                else None
            )
            ok, detail = probe_model_backend(config.ai, key_or_none)
            check(f"model backend reachable ({config.ai.backend})", ok, detail)
            for provider in config.data.providers:
                if provider.enabled and provider.credential:
                    check(f"credential '{provider.credential}' (provider {provider.name})",
                          provider.credential in names)
            broker_cfg = config.primary_broker()
            if broker_cfg and broker_cfg.credential:
                check(f"credential '{broker_cfg.credential}' (broker {broker_cfg.name})",
                      broker_cfg.credential in names)
        except VaultError as exc:
            check("vault unlocks", False, str(exc))

    async def db_check() -> bool:
        from .storage.db import Database

        db = Database(config.data_dir / "poseidon.db")
        try:
            await db.open()
            await db.close()
            return True
        except Exception:
            return False

    check("database opens", asyncio.run(db_check()))
    print(f"\n{problems} problem(s) found." if problems else "\nAll checks passed.")
    return 1 if problems else 0


def cmd_vault(args: argparse.Namespace) -> int:
    config = _load(args)
    vault = _vault_for(config)
    action: str = args.vault_action
    if action == "init":
        if vault.exists:
            print("Vault already exists.", file=sys.stderr)
            return 2
        p1 = getpass.getpass("New vault passphrase (min 8 chars): ")
        p2 = getpass.getpass("Repeat: ")
        if p1 != p2:
            print("Passphrases do not match.", file=sys.stderr)
            return 2
        vault.create(p1)
        print(f"Vault created at {config.data_dir / 'vault.bin'}")
        return 0
    _unlock(vault)
    if action == "unlock-check":
        print("Vault unlocked successfully.")
    elif action == "set":
        value = args.value
        if value is None:
            value = getpass.getpass(f"Value for '{args.name}' (input hidden): ")
        vault.set(args.name, value)
        print(f"Stored credential '{args.name}'.")
    elif action == "rm":
        vault.delete(args.name)
        print(f"Removed credential '{args.name}'.")
    elif action == "rekey":
        # _unlock above already proved the OLD passphrase, so this rotates a
        # vault the operator can currently open — it can never recover a
        # forgotten one.
        p1 = getpass.getpass("New vault passphrase (min 8 chars): ")
        p2 = getpass.getpass("Repeat: ")
        if p1 != p2:
            print("Passphrases do not match.", file=sys.stderr)
            return 2
        vault.rekey(p1)
        print(f"Vault re-encrypted under the new passphrase "
              f"({len(vault.names())} credential(s) preserved).")
        print("Update POSEIDON_VAULT_PASSPHRASE / the passphrase file if you use one.")
    elif action == "list":
        for name in vault.names():
            print(name)
    return 0


def cmd_config(args: argparse.Namespace) -> int:
    if args.config_action == "validate":
        try:
            config = _load(args)
        except ConfigError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        # AIConfig accepts any model string (a local/openai_compatible id is
        # legitimate), so a broken Anthropic id validates fine at the pydantic
        # layer and would otherwise 400 on every completion with nothing here
        # to say why — warn (never fail) for a known-broken id.
        if config.ai.backend == "anthropic":
            from .ai.backends import KNOWN_BROKEN_CLAUDE_MODELS
            for field, model_id in (("ai.model", config.ai.model),
                                    ("ai.utility_model", config.ai.utility_model)):
                reason = KNOWN_BROKEN_CLAUDE_MODELS.get(model_id or "")
                if reason:
                    print(f"WARNING: {field}={model_id} {reason}", file=sys.stderr)
        ai_model = f"ai_model={config.ai.model}"
        if config.ai.utility_model:
            ai_model += f" (utility={config.ai.utility_model})"
        print(f"Configuration valid. mode={config.mode.value}, {ai_model}, "
              f"providers={len([p for p in config.data.providers if p.enabled])}, "
              f"brokers={len([b for b in config.brokers if b.enabled])}, "
              f"strategies={len([s for s in config.strategies if s.enabled])}")
        return 0
    if args.config_action == "example":
        # Packaged inside poseidon for installed builds (wheel force-include),
        # or the repo-root config/ for a source/editable checkout (same
        # resolution as app.py's bundled example algorithms).
        example = Path(__file__).resolve().parent / "config" / "poseidon.example.yaml"
        if not example.is_file():
            example = Path(__file__).resolve().parents[2] / "config" / "poseidon.example.yaml"
        if not example.is_file():
            print("starter config not found in this installation; "
                  "see docs/configuration.md for a template", file=sys.stderr)
            return 2
        target = default_config_dir() / "poseidon.yaml"
        if target.exists():
            print(f"{target} already exists; not overwriting.", file=sys.stderr)
            return 2
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(example.read_text(encoding="utf-8"), encoding="utf-8")
        print(f"Wrote starter config to {target}")
        return 0
    return 2


def cmd_audit(args: argparse.Namespace) -> int:
    config = _load(args)

    async def main() -> int:
        from .security.audit import AuditLog
        from .storage.db import Database

        db = Database(config.data_dir / "poseidon.db")
        await db.open()
        try:
            audit = AuditLog(db)
            if args.audit_action == "verify":
                ok, bad = await audit.verify_chain()
                print("Audit chain verified — no tampering detected." if ok
                      else f"AUDIT CHAIN BROKEN at seq {bad}!")
                return 0 if ok else 1
            for record in reversed(await audit.tail(args.n)):
                print(f"{record.seq:>6}  {record.at.isoformat()}  {record.actor:<8} "
                      f"{record.action:<24} {record.payload}")
            return 0
        finally:
            await db.close()

    return asyncio.run(main())


def cmd_update(args: argparse.Namespace) -> int:
    config = _load(args)

    async def main() -> int:
        from .core.events import EventBus
        from .updater import UpdateService

        service = UpdateService(config.updates, EventBus())
        if not service.is_git_checkout:
            print("Self-update requires a git checkout (git clone + pip install -e .); "
                  "this installation is not one.", file=sys.stderr)
            return 2
        if args.update_action == "check":
            remote = await service.check_once()
            print(f"Update available: {remote[:12]}" if remote else "Up to date.")
            return 0
        applied = await service._apply()  # noqa: SLF001 — CLI is a trusted caller
        print("Update applied. Restart the service." if applied else "Update failed; see logs.")
        return 0 if applied else 1

    return asyncio.run(main())


# ---------------------------------------------------------------- parser


def _positive_int(text: str) -> int:
    """argparse type for flags that must be >= 1. An explicit 0 is a usage error —
    silently substituting the config default would run a different experiment than
    the user asked for — and a negative value must die here, not as a traceback."""
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be an integer >= 1")
    return value


def _unit_fraction(text: str) -> float:
    """argparse type for --train-frac: a float in the half-open unit interval [0, 1).
    0 disables the split; 1 is rejected because a whole-history "split" leaves no test
    segment. Out-of-range or non-numeric input dies as a usage error, not a traceback."""
    value = float(text)
    if not (0.0 <= value < 1.0):
        raise argparse.ArgumentTypeError("must be a float in [0, 1)")
    return value


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="poseidon", description="Poseidon — autonomous AI trading platform")
    parser.add_argument("--version", action="version", version=f"poseidon {__version__}")
    parser.add_argument("--config", "-c", help="path to poseidon.yaml (default: ~/.config/poseidon/poseidon.yaml)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="start the platform").set_defaults(func=cmd_run)
    sub.add_parser("app", help="open the dashboard as a desktop window").set_defaults(func=cmd_app)
    sub.add_parser("cycle", help="run a single review cycle and exit").set_defaults(func=cmd_cycle)
    sub.add_parser("doctor", help="self-diagnostics").set_defaults(func=cmd_doctor)

    research = sub.add_parser("research", help="offline factor research")
    research_sub = research.add_subparsers(dest="research_action", required=True)
    fac = research_sub.add_parser("factors", help="rank factors by point-in-time IC/IR")
    fac.add_argument("--symbols", default="", help="comma-separated symbols, e.g. AAA,BBB")
    fac.add_argument("--symbols-file", default="",
                     help="path to a file, one symbol per line (# comment lines skipped, "
                          "duplicates removed)")
    fac.add_argument("--universe", choices=["sp500"], default="",
                     help="use a bundled universe snapshot (adds a survivorship caveat to "
                          "the report)")
    fac.add_argument("--watchlist", action="store_true", help="use all configured watchlist symbols")
    fac.add_argument("--days", type=_positive_int, default=None,
                     help="history window (default: research.lookback_days)")
    fac.add_argument("--horizon", type=_positive_int, default=None,
                     help="forward-return horizon in bars (default: research.horizon)")
    fac.add_argument("--rebalance-every", dest="rebalance_every", type=_positive_int, default=None,
                     help="trading days between IC samples (default: research.rebalance_every)")
    fac.add_argument("--train-frac", dest="train_frac", type=_unit_fraction, default=None,
                     help="chronological OOS split fraction in [0, 1); 0 disables "
                          "(default: research.train_frac)")
    fac.set_defaults(func=cmd_research)

    vault = sub.add_parser("vault", help="manage the encrypted credential vault")
    vault_sub = vault.add_subparsers(dest="vault_action", required=True)
    vault_sub.add_parser("init")
    vault_sub.add_parser("unlock-check")
    set_parser = vault_sub.add_parser("set")
    set_parser.add_argument("name")
    set_parser.add_argument("value", nargs="?", help="omit to enter interactively (hidden)")
    rm_parser = vault_sub.add_parser("rm")
    rm_parser.add_argument("name")
    vault_sub.add_parser(
        "rekey", help="change the vault passphrase (re-encrypts in place)")
    vault_sub.add_parser("list")
    vault.set_defaults(func=cmd_vault)

    config_parser = sub.add_parser("config", help="configuration helpers")
    config_sub = config_parser.add_subparsers(dest="config_action", required=True)
    config_sub.add_parser("validate")
    config_sub.add_parser("example", help="write the starter config")
    config_parser.set_defaults(func=cmd_config)

    audit = sub.add_parser("audit", help="inspect/verify the audit log")
    audit_sub = audit.add_subparsers(dest="audit_action", required=True)
    audit_sub.add_parser("verify")
    tail = audit_sub.add_parser("tail")
    tail.add_argument("-n", type=int, default=50)
    audit.set_defaults(func=cmd_audit)

    update = sub.add_parser("update", help="check for / apply updates")
    update_sub = update.add_subparsers(dest="update_action", required=True)
    update_sub.add_parser("check")
    update_sub.add_parser("apply")
    update.set_defaults(func=cmd_update)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return int(args.func(args))
    except PoseidonError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
