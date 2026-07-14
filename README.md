# CL_config_sync

Reconcile topic configurations from a **Confluent Cluster Linking** source cluster
onto its **mirror topics** on a DR (destination) cluster — closing the gaps that
Cluster Linking leaves.

## The problem

Cluster Linking only syncs a *subset* of topic configs to mirror topics:

| Category | Behaviour | This tool |
|----------|-----------|-----------|
| **Always** | Always synced, immutable on the mirror. | Verified only (flags drift). |
| **Configurable** | Synced **only if** listed in the link's `topic.config.sync.include`. | Recommends (and can apply) the include list. |
| **Independent** | **Never** synced, and **cannot** be added to `topic.config.sync.include` — the link config validator rejects them (e.g. `confluent.value.schema.validation`, `confluent.*.subject.name.strategy`, schema context). | Copied directly onto the mirror via REST. |

For a DR clone you usually want the mirror to match the source for *all* meaningful
configs. `dr_config_sync.py` does this in two ways:

1. **CL-syncable but not synced** → emits a recommended `topic.config.sync.include`
   (optionally applies it with `--apply-include-list`).
2. **Never syncable by CL** → copies the value directly onto each mirror topic
   (`--apply`). Mirror topics accept local overrides for Independent configs even
   while `ACTIVE`.

Everything is done over the **Kafka REST v3 API** (stdlib only), so the flow ports
to any language.

> **Note:** Cluster Linking requires the **destination** cluster to be **Dedicated**
> (a Standard destination returns "Cluster linking is disabled in this cluster").
> The source may be Standard, but Independent configs such as schema validation are
> only exposed via the topic API on **Dedicated** clusters — on Standard they return
> HTTP 404. For this tool to read them from the source, the source should be Dedicated.

## Requirements

- Python 3.10+ (standard library only — no `pip install`).

## Configuration

Every parameter is a CLI flag or a `CL_*` environment variable (flag wins). Copy the
template and fill it in:

```bash
cp .env.example .env
$EDITOR .env
```

| Env var | Flag | Meaning |
|---------|------|---------|
| `CL_SOURCE_REST` | `--source-rest` | Source cluster REST endpoint (`https://…:443`) |
| `CL_SOURCE_CLUSTER` | `--source-cluster` | Source `lkc-…` |
| `CL_SOURCE_API_KEY` / `CL_SOURCE_API_SECRET` | `--source-api-key` / `--source-api-secret` | Source credentials (need `DescribeConfigs` on the topics) |
| `CL_DEST_REST` | `--dest-rest` | Destination cluster REST endpoint |
| `CL_DEST_CLUSTER` | `--dest-cluster` | Destination `lkc-…` |
| `CL_DEST_API_KEY` / `CL_DEST_API_SECRET` | `--dest-api-key` / `--dest-api-secret` | Destination credentials (need topic `AlterConfigs` + link describe/alter) |
| `CL_LINK` | `--link` | Cluster link name on the destination |
| `CL_TOPICS` | `--topics` | Space-separated source topics, or `source=mirror` pairs |
| `CL_MIRROR_PREFIX` | `--mirror-prefix` | Prefix for mirror names if the link uses `cluster.link.prefix` |
| `CL_SKIP` | `--skip` | Independent configs to *not* copy (defaults to a cluster-local set) |
| `CL_CA_BUNDLE` | `--ca-bundle` | CA bundle path; auto-detected if omitted |

## Usage

```bash
# Load config
set -a; source .env; set +a

# Dry-run: report what would change (no writes)
python3 dr_config_sync.py

# Apply: copy Independent configs onto the mirrors
python3 dr_config_sync.py --apply

# Apply everything: Independent copies + update the link's include list
python3 dr_config_sync.py --apply --apply-include-list

# Override topics / add pairs on the CLI (flags beat env)
python3 dr_config_sync.py --topics orders payments legacy=legacy-mirror --apply
```

Exit code is non-zero if any Independent-config copy failed.

## Caveats

- **Reconciliation, not one-time.** CL never syncs the Independent configs, so run
  this on a schedule to catch source-side drift.
- **Category lists are CP-version-sensitive.** `ALWAYS_CONFIGS` / `CONFIGURABLE_CONFIGS`
  mirror `MirrorTopicConfigSyncRules` in ce-kafka. Re-verify them against the
  Confluent Platform version your clusters run.
- **`.env` holds live secrets** and is gitignored. Never commit it.
