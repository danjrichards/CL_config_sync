#!/usr/bin/env python3
"""
dr_config_sync.py — Reconcile topic configurations from a Cluster Linking source
cluster onto its mirror topics on a DR (destination) cluster.

Why this exists
---------------
Confluent Cluster Linking only syncs a *subset* of topic configs to mirror topics:

  * "Always" configs      - always synced, immutable on the mirror.
  * "Configurable" configs- synced ONLY if listed in the link's
                            `topic.config.sync.include`.
  * "Independent" configs - NEVER synced, and CANNOT be added to
                            `topic.config.sync.include` (the link config
                            validator rejects them). e.g. schema validation,
                            subject name strategy, schema context.

For a DR clone you usually want the mirror to match the source for *all*
meaningful configs. This tool closes the two gaps CL leaves:

  1. Configs that *can* be synced by CL but currently aren't (because they are
     not in `topic.config.sync.include`)  ->  emitted as a RECOMMENDED include
     list you can apply to the link (a link-level, all-topics change).
  2. Configs that CL will *never* sync ("Independent")  ->  copied directly onto
     each mirror topic via the Kafka REST v3 API (mirror topics accept local
     overrides for Independent configs even while ACTIVE).

Everything is done over REST (Kafka REST v3), so the same flow is trivial to
reimplement in any language. Requires only the Python standard library.

Configuration
-------------
Every connection parameter can be supplied either as a CLI flag or an
environment variable (flag wins). Put the env vars in a .env file and source it:

    set -a; source .env; set +a
    python3 dr_config_sync.py --topics orders payments        # dry-run
    python3 dr_config_sync.py --topics orders payments --apply --apply-include-list

Category lists below are taken from ce-kafka
`core/.../kafka/server/link/MirrorTopicConfigSyncRules.scala`. Keep them in sync
with the Confluent Platform version your clusters run.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field

# Common CA bundle locations, used if the interpreter has no default trust store
# (e.g. python.org macOS builds). SSL verification stays ON either way.
_CA_CANDIDATES = (
    "/etc/ssl/cert.pem",
    "/opt/homebrew/etc/ca-certificates/cert.pem",
    "/usr/local/etc/openssl@3/cert.pem",
)


def build_ssl_context(ca_bundle: str | None) -> ssl.SSLContext:
    if ca_bundle:
        return ssl.create_default_context(cafile=ca_bundle)
    env = os.environ.get("SSL_CERT_FILE")
    if env and os.path.exists(env):
        return ssl.create_default_context(cafile=env)
    ctx = ssl.create_default_context()
    if ctx.get_ca_certs():  # interpreter already has a trust store
        return ctx
    for cand in _CA_CANDIDATES:
        if os.path.exists(cand):
            return ssl.create_default_context(cafile=cand)
    return ctx

# --------------------------------------------------------------------------- #
# Cluster Linking config categories (from MirrorTopicConfigSyncRules.scala).
# --------------------------------------------------------------------------- #

# Always synced by CL and immutable on the mirror. MUST be present in any custom
# topic.config.sync.include. Never copy these directly (the mirror rejects them).
ALWAYS_CONFIGS = {
    "message.timestamp.type",
    "message.timestamp.difference.max.ms",
    "cleanup.policy",
    "max.message.bytes",
}

# Synced by CL only if listed in topic.config.sync.include. These are the configs
# that *should* be added to the include list rather than copied directly.
CONFIGURABLE_CONFIGS = {
    "min.compaction.lag.ms",
    "max.compaction.lag.ms",
    "compression.type",
    "segment.bytes",
    "segment.ms",
    "min.insync.replicas",
    "segment.jitter.ms",
    "segment.index.bytes",
    "flush.messages",
    "flush.ms",
    "index.interval.bytes",
    "min.cleanable.dirty.ratio",
    "file.delete.delay.ms",
    "preallocate",
    "confluent.segment.speculative.prefetch.enable",
    "unclean.leader.election.enable",
    "message.downconversion.enable",
    "delete.retention.ms",
    "retention.bytes",
    "retention.ms",
    "message.timestamp.before.max.ms",
    "message.timestamp.after.max.ms",
}

# Configs that CAN legally appear in topic.config.sync.include.
INCLUDABLE_CONFIGS = ALWAYS_CONFIGS | CONFIGURABLE_CONFIGS

# Cluster/topology-local configs we never copy onto the mirror even though they
# are technically "Independent" — copying them verbatim to a different cluster is
# wrong or harmful. Edit to taste for your environment.
DEFAULT_SKIP_CONFIGS = {
    "leader.replication.throttled.replicas",
    "follower.replication.throttled.replicas",
    "confluent.placement.constraints",
    "internal.segment.bytes",
}


# --------------------------------------------------------------------------- #
# Minimal Kafka REST v3 client (stdlib only).
# --------------------------------------------------------------------------- #

class KafkaRest:
    def __init__(self, base_url: str, cluster_id: str, api_key: str, api_secret: str,
                 ssl_context: ssl.SSLContext | None = None):
        self.base = base_url.rstrip("/")
        self.cluster = cluster_id
        token = base64.b64encode(f"{api_key}:{api_secret}".encode()).decode()
        self.auth = f"Basic {token}"
        self.ssl_context = ssl_context

    def _request(self, method: str, path: str, body: dict | None = None) -> dict | None:
        url = f"{self.base}/kafka/v3/clusters/{self.cluster}{path}"
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", self.auth)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, context=self.ssl_context) as resp:
                raw = resp.read()
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            detail = e.read().decode(errors="replace")
            raise RuntimeError(f"HTTP {e.code} {method} {path}: {detail}") from None

    def get_topic_configs(self, topic: str) -> dict[str, dict]:
        """Return {name: config_entry} for a topic. config_entry has value/is_default/..."""
        out = self._request("GET", f"/topics/{topic}/configs")
        return {c["name"]: c for c in out.get("data", [])}

    def alter_topic_configs(self, topic: str, values: dict[str, str]) -> None:
        """SET a batch of {name: value} on a topic (POST .../configs:alter)."""
        body = {"data": [{"name": n, "value": v} for n, v in values.items()]}
        self._request("POST", f"/topics/{topic}/configs:alter", body)

    def get_link_configs(self, link: str) -> dict[str, dict]:
        out = self._request("GET", f"/links/{link}/configs")
        return {c["name"]: c for c in out.get("data", [])}

    def alter_link_configs(self, link: str, values: dict[str, str]) -> None:
        """SET a batch of {name: value} on a link (PUT .../configs:alter)."""
        body = {"data": [{"name": n, "value": v} for n, v in values.items()]}
        self._request("PUT", f"/links/{link}/configs:alter", body)


# --------------------------------------------------------------------------- #
# Reconciliation.
# --------------------------------------------------------------------------- #

@dataclass
class Report:
    # Independent configs to copy onto the mirror, per topic: {mirror_topic: {name: value}}
    to_copy: dict[str, dict[str, str]] = field(default_factory=dict)
    # Configurable configs set on source but not synced by CL (recommend include-list).
    recommend_include: set[str] = field(default_factory=set)
    # Configs skipped as cluster-local: {topic: {name}}
    skipped: dict[str, set[str]] = field(default_factory=dict)
    # Always/synced configs whose mirror value doesn't match (CL lag or issue).
    managed_mismatch: dict[str, dict[str, tuple[str, str]]] = field(default_factory=dict)
    copy_errors: dict[str, dict[str, str]] = field(default_factory=dict)


def source_set_configs(entries: dict[str, dict]) -> dict[str, str]:
    """Configs explicitly set on the source (is_default == False)."""
    return {n: c.get("value") for n, c in entries.items()
            if not c.get("is_default", False) and c.get("value") is not None}


def reconcile_topic(src: KafkaRest, dst: KafkaRest, source_topic: str, mirror_topic: str,
                    current_include: set[str], skip: set[str], report: Report) -> None:
    src_cfg = source_set_configs(src.get_topic_configs(source_topic))
    dst_entries = dst.get_topic_configs(mirror_topic)
    dst_cfg = {n: c.get("value") for n, c in dst_entries.items()}

    for name, sval in sorted(src_cfg.items()):
        mval = dst_cfg.get(name)

        if name in ALWAYS_CONFIGS or name in current_include:
            # CL manages these; they're immutable on the mirror. Just verify.
            if mval != sval:
                report.managed_mismatch.setdefault(mirror_topic, {})[name] = (sval, mval)
            continue

        if name in CONFIGURABLE_CONFIGS:
            # Syncable by CL, but only if added to topic.config.sync.include.
            report.recommend_include.add(name)
            continue

        if name in skip:
            report.skipped.setdefault(mirror_topic, set()).add(name)
            continue

        # Independent config -> copy directly onto the mirror if it differs.
        if mval != sval:
            report.to_copy.setdefault(mirror_topic, {})[name] = sval


def recommended_include_list(current_include: set[str], recommend: set[str]) -> list[str]:
    """Mandatory Always + whatever is already included + configurable configs in use."""
    return sorted(ALWAYS_CONFIGS | current_include | recommend)


# --------------------------------------------------------------------------- #
# CLI.
# --------------------------------------------------------------------------- #

def parse_pairs(topics_arg: list[str], prefix: str) -> list[tuple[str, str]]:
    """Each item is 'source_topic' or 'source_topic=mirror_topic'."""
    pairs = []
    for item in topics_arg:
        if "=" in item:
            s, m = item.split("=", 1)
        else:
            s, m = item, f"{prefix}{item}"
        pairs.append((s.strip(), m.strip()))
    return pairs


def main() -> int:
    e = os.environ.get
    p = argparse.ArgumentParser(
        description="Reconcile source topic configs onto CL mirror topics for DR. "
                    "Flags override the matching CL_* environment variables.")
    # Connection params: default from env, validated below.
    p.add_argument("--source-rest", default=e("CL_SOURCE_REST"), help="[CL_SOURCE_REST] source REST endpoint")
    p.add_argument("--source-cluster", default=e("CL_SOURCE_CLUSTER"), help="[CL_SOURCE_CLUSTER]")
    p.add_argument("--source-api-key", default=e("CL_SOURCE_API_KEY"), help="[CL_SOURCE_API_KEY]")
    p.add_argument("--source-api-secret", default=e("CL_SOURCE_API_SECRET"), help="[CL_SOURCE_API_SECRET]")
    p.add_argument("--dest-rest", default=e("CL_DEST_REST"), help="[CL_DEST_REST] destination REST endpoint")
    p.add_argument("--dest-cluster", default=e("CL_DEST_CLUSTER"), help="[CL_DEST_CLUSTER]")
    p.add_argument("--dest-api-key", default=e("CL_DEST_API_KEY"), help="[CL_DEST_API_KEY]")
    p.add_argument("--dest-api-secret", default=e("CL_DEST_API_SECRET"), help="[CL_DEST_API_SECRET]")
    p.add_argument("--link", default=e("CL_LINK"), help="[CL_LINK] cluster link name on the destination")
    p.add_argument("--topics", nargs="+", default=(e("CL_TOPICS", "").split() or None),
                   help="[CL_TOPICS] source topics, or 'source=mirror' pairs")
    p.add_argument("--mirror-prefix", default=e("CL_MIRROR_PREFIX", ""),
                   help="[CL_MIRROR_PREFIX] prefix for mirror names if the link uses one")
    p.add_argument("--skip", nargs="*", default=(e("CL_SKIP").split() if e("CL_SKIP") else None),
                   help="[CL_SKIP] Independent configs to NOT copy (defaults to cluster-local set)")
    p.add_argument("--ca-bundle", default=e("CL_CA_BUNDLE"),
                   help="[CL_CA_BUNDLE] path to a CA bundle (PEM). Auto-detected if omitted.")
    p.add_argument("--apply", action="store_true",
                   help="Actually copy Independent configs onto mirrors (default: dry-run)")
    p.add_argument("--apply-include-list", action="store_true",
                   help="Also PUT the recommended topic.config.sync.include onto the link")
    args = p.parse_args()

    required = {
        "--source-rest / CL_SOURCE_REST": args.source_rest,
        "--source-cluster / CL_SOURCE_CLUSTER": args.source_cluster,
        "--source-api-key / CL_SOURCE_API_KEY": args.source_api_key,
        "--source-api-secret / CL_SOURCE_API_SECRET": args.source_api_secret,
        "--dest-rest / CL_DEST_REST": args.dest_rest,
        "--dest-cluster / CL_DEST_CLUSTER": args.dest_cluster,
        "--dest-api-key / CL_DEST_API_KEY": args.dest_api_key,
        "--dest-api-secret / CL_DEST_API_SECRET": args.dest_api_secret,
        "--link / CL_LINK": args.link,
        "--topics / CL_TOPICS": args.topics,
    }
    missing = [name for name, val in required.items() if not val]
    if missing:
        p.error("missing required config (flag or env var):\n  " + "\n  ".join(missing))

    ctx = build_ssl_context(args.ca_bundle)
    src = KafkaRest(args.source_rest, args.source_cluster, args.source_api_key, args.source_api_secret, ctx)
    dst = KafkaRest(args.dest_rest, args.dest_cluster, args.dest_api_key, args.dest_api_secret, ctx)
    skip = set(args.skip) if args.skip is not None else set(DEFAULT_SKIP_CONFIGS)

    link_cfg = dst.get_link_configs(args.link)
    raw_include = (link_cfg.get("topic.config.sync.include", {}) or {}).get("value") or ""
    current_include = {c.strip() for c in raw_include.split(",") if c.strip()}

    report = Report()
    for source_topic, mirror_topic in parse_pairs(args.topics, args.mirror_prefix):
        reconcile_topic(src, dst, source_topic, mirror_topic, current_include, skip, report)

    # ---- Independent config copies (REST onto mirrors) ---- #
    print("=" * 70)
    print("INDEPENDENT CONFIGS (not syncable by CL) -> copy onto mirror via REST")
    print("=" * 70)
    if not report.to_copy:
        print("  (nothing to copy; mirrors already match source)")
    for mirror, vals in sorted(report.to_copy.items()):
        for name, val in sorted(vals.items()):
            print(f"  {mirror}: {name} = {val}")
        if args.apply:
            try:
                dst.alter_topic_configs(mirror, vals)
                print(f"  -> APPLIED to {mirror}")
            except RuntimeError as ex:
                for name in vals:
                    report.copy_errors.setdefault(mirror, {})[name] = str(ex)
                print(f"  -> ERROR applying to {mirror}: {ex}")
    if not args.apply and report.to_copy:
        print("  (dry-run; re-run with --apply to copy)")

    # ---- Recommended topic.config.sync.include ---- #
    recommended = recommended_include_list(current_include, report.recommend_include)
    missing_include = sorted(report.recommend_include - current_include)
    print("\n" + "=" * 70)
    print("SYNCABLE-BY-CL CONFIGS -> belong in topic.config.sync.include")
    print("=" * 70)
    print(f"  current include list: {sorted(current_include) or '(unset -> CL default)'}")
    if missing_include:
        print(f"  set on source but NOT synced: {missing_include}")
        print(f"  RECOMMENDED topic.config.sync.include:\n    {','.join(recommended)}")
        if args.apply_include_list:
            try:
                dst.alter_link_configs(args.link, {"topic.config.sync.include": ",".join(recommended)})
                print("  -> APPLIED recommended include list to link")
            except RuntimeError as ex:
                print(f"  -> ERROR applying include list: {ex}")
        else:
            print("  (re-run with --apply-include-list to apply, or set it via the CLI/UI)")
    else:
        print("  include list already covers all syncable configs in use. Nothing to add.")

    # ---- Warnings ---- #
    if report.skipped:
        print("\nSKIPPED (cluster-local; edit --skip / CL_SKIP to override):")
        for mirror, names in sorted(report.skipped.items()):
            print(f"  {mirror}: {sorted(names)}")
    if report.managed_mismatch:
        print("\nWARNING - CL-managed config mismatch (sync lag or link misconfig):")
        for mirror, d in sorted(report.managed_mismatch.items()):
            for name, (sval, mval) in sorted(d.items()):
                print(f"  {mirror}: {name} source={sval} mirror={mval}")

    return 1 if report.copy_errors else 0


if __name__ == "__main__":
    sys.exit(main())
