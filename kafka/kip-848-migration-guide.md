<!-- TODO: Move this guide to kafka package docs present in kafka.go -->

Starting with **confluent-kafka-go 2.12.0** (GA release), the next generation consumer group rebalance protocol defined in [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) is **production-ready**.

**Note:** The new consumer group protocol defined in [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) is not enabled by default. There are a few contract changes associated with the new protocol and might cause breaking changes. `group.protocol` configuration property dictates whether to use the new `consumer` protocol or older `classic` protocol. It defaults to `classic` if not provided.

# Overview

- **What changed:**

  The **Group Leader role** (consumer member) is removed. Assignments are calculated by the **Group Coordinator (broker)** and distributed via **heartbeats**.

- **Requirements:**

  - Broker version **4.0.0+**
  - confluent-kafka-go version **2.12.0+**: GA (production-ready)

- **Enablement (client-side):**

  - `group.protocol=consumer`
  - `group.remote.assignor=<assignor>` (optional; broker-controlled if unset; default broker assignor is `uniform`)

# Available Features

All [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) features are supported including:

- Subscription to one or more topics, including **regular expression (regex) subscriptions**
- Rebalance callbacks (**incremental only**)
- Static group membership
- Configurable remote assignor
- Enforced max poll interval
- Upgrade from `classic` protocol or downgrade from `consumer` protocol
- AdminClient changes as per KIP

# Contract Changes

## Client Configuration changes

| Classic Protocol (Deprecated Configs in KIP-848) | KIP-848 / Next-Gen Replacement                        |
|--------------------------------------------------|-------------------------------------------------------|
| `partition.assignment.strategy`                  | `group.remote.assignor`                               |
| `session.timeout.ms`                             | Broker config: `group.consumer.session.timeout.ms`    |
| `heartbeat.interval.ms`                          | Broker config: `group.consumer.heartbeat.interval.ms` |
| `group.protocol.type`                            | Not used in the new protocol                          |

**Note:** The properties listed under “Classic Protocol (Deprecated Configs in KIP-848)” are **no longer used** when using the KIP-848 consumer protocol.

## Rebalance Callback Changes

- The **protocol is fully incremental** in KIP-848.
- In the **rebalance callbacks**, you **must only use** (optional - if not used, client will handle it internally):
  - `consumer.IncrementalAssign(e.Partitions)` to assign new partitions
  - `consumer.IncrementalUnassign(e.Partitions)` to revoke partitions
- **Do not** use `consumer.Assign()` or `consumer.Unassign()` after subscribing with `group.protocol='consumer'` (KIP-848).
- If you don't call `IncrementalAssign()`/`IncrementalUnassign()` inside rebalance callbacks, the client will automatically use `IncrementalAssign()`/`IncrementalUnassign()` internally.
- ⚠️ The `e.Partitions` list passed to `IncrementalAssign()` and `IncrementalUnassign()` contains only the **incremental changes** — partitions being **added** or **revoked** — **not the full assignment**, as was the case with `Assign()` in the classic protocol.
- All assignors under KIP-848 are now **sticky**, including `range`, which was **not sticky** in the classic protocol.

## Static Group Membership

- Duplicate `group.instance.id` handling:
  - **Newly joining member** is fenced with **ErrUnreleasedInstanceID (fatal)**.
  - (Classic protocol fenced the **existing** member instead.)
- Implications:
  - Ensure only **one active instance per** `group.instance.id`.
  - Consumers must shut down cleanly to avoid blocking replacements until session timeout expires.

## Session Timeout & Fetching

- **Session timeout is broker-controlled**:
  - If the Coordinator is unreachable, a consumer **continues fetching messages** but cannot commit offsets.
  - Consumer is fenced once a heartbeat response is received from the Coordinator.
- In the classic protocol, the client stopped fetching when session timeout expired.

## Closing / Auto-Commit

- On `Close()` or `Unsubscribe()` with auto-commit enabled:
  - Member retries committing offsets until a timeout expires.
  - Currently uses the **default remote session timeout**.
  - Future **KIP-1092** will allow custom commit timeouts.

## Error Handling Changes

- `ErrUnknownTopicOrPart` (**subscription case**):
  - No longer returned if a topic is missing in the **local cache** when subscribing; the subscription proceeds.
- `ErrTopicAuthorizationFailed`:
  - Reported once per heartbeat or subscription change, even if only one topic is unauthorized.

## Summary of Key Differences (Classic vs Next-Gen)

- **Assignment:** Classic protocol calculated by **Group Leader (consumer)**; KIP-848 calculated by **Group Coordinator (broker)**
- **Assignors:** Classic range assignor was **not sticky**; KIP-848 assignors are **sticky**, including range
- **Deprecated configs:** Classic client configs are replaced by `group.remote.assignor` and broker-controlled session/heartbeat configs
- **Static membership fencing:** KIP-848 fences **new member** on duplicate `group.instance.id`
- **Session timeout:** Classic: enforced on client; KIP-848: enforced on broker
- **Auto-commit on close:** Classic: stops at client session timeout; KIP-848: retries until remote timeout
- **Unknown topics:** KIP-848 does not return error on subscription if topic is missing
- **Upgrade/Downgrade:** KIP-848 supports upgrade/downgrade from/to `classic` and `consumer` protocols

# Minimal Example Config

## Classic Protocol

``` properties
# Optional; default is 'classic'
group.protocol=classic

partition.assignment.strategy=<range,roundrobin,sticky>
session.timeout.ms=45000
heartbeat.interval.ms=15000
```

## Next-Gen Protocol / KIP-848

``` properties
group.protocol=consumer

# Optional: select a remote assignor
# Valid options currently: 'uniform' or 'range'
#   group.remote.assignor=<uniform,range>
# If unset, broker chooses the assignor (default: 'uniform')

# Session & heartbeat now controlled by broker:
#   group.consumer.session.timeout.ms
#   group.consumer.heartbeat.interval.ms
```

# Rebalance Callback Migration

## Range Assignor (Classic)

```go
// Classic protocol: Full partition list provided on assign
func onRebalanceClassic(consumer *kafka.Consumer, ev kafka.Event) {
    switch e := ev.(type) {
    case kafka.AssignedPartitions:
        fmt.Printf("[Classic] Assigned partitions: %v\n", e.Partitions)
        // Optional: client handles assign if not done manually
        consumer.Assign(e.Partitions)

    case kafka.RevokedPartitions:
        fmt.Printf("[Classic] Revoked partitions: %v\n", e.Partitions)
        // Optional: client handles unassign if not done manually
        consumer.Unassign()
    }
}
```

## Incremental Assignor (Including Range in Consumer / KIP-848, Any Protocol)

```go
// Incremental assignor
func onRebalanceCooperative(consumer *kafka.Consumer, ev kafka.Event) {
    switch e := ev.(type) {
    case kafka.AssignedPartitions:
        fmt.Printf("[KIP-848] Incrementally assigning: %v\n", e.Partitions)
        // Optional, client handles if omitted
        consumer.IncrementalAssign(e.Partitions)

    case kafka.RevokedPartitions:
        fmt.Printf("[KIP-848] Incrementally revoking: %v\n", e.Partitions)
        // Optional, client handles if omitted
        consumer.IncrementalUnassign(e.Partitions)
    }
}
```

**Note:** The `e.Partitions` list contains **only partitions being added or revoked**, not the full partition list as in the classic `consumer.Assign()`.

# Upgrade and Downgrade

- A group made up entirely of `classic` consumers runs under the classic protocol.
- The group is **upgraded to the consumer protocol** as soon as at least one `consumer` protocol member joins.
- The group is **downgraded back to the classic protocol** if the last `consumer` protocol member leaves while `classic` members remain.
- Both **rolling upgrade** (classic → consumer) and **rolling downgrade** (consumer → classic) are supported.

# Migration Checklist (Next-Gen Protocol / [KIP-848](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol))

1.  Upgrade to **confluent-kafka-go ≥ 2.12.0** (GA release)
2.  Run against **Kafka brokers ≥ 4.0.0**
3.  Set `group.protocol=consumer`
4.  Optionally set `group.remote.assignor`; leave unspecified for broker-controlled (default: `uniform`), valid options: `uniform` or `range`
5.  Replace deprecated configs with new ones
6.  Update rebalance callbacks to **incremental APIs only** (if used)
7.  Review static membership handling (`group.instance.id`)
8.  Ensure proper shutdown to avoid fencing issues
9.  Adjust error handling for unknown topics and authorization failures
