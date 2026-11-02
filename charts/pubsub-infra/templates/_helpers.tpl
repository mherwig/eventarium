{{/*
Release-wide name. Two charts install into one namespace, so the release
name has to be part of every object name - otherwise pubsub-infra and pubsub
would both try to own a Secret called "config".
*/}}
{{- define "pubsub-infra.fullname" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Chart labels, on every object. */}}
{{- define "pubsub-infra.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "pubsub-infra.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels: a subset of the above, and stable forever. */}}
{{- define "pubsub-infra.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pubsub-infra.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Client service name. Kafka advertises this to every consumer, so the value
has to be a DNS name the app pods can resolve - changing it on an existing
release means brokers advertising a name that no longer exists.
*/}}
{{- define "pubsub-infra.kafkaService" -}}
{{ include "pubsub-infra.fullname" . }}-kafka
{{- end -}}

{{/*
Headless companion to the client Service. Two jobs, both load-bearing:

  - it gives each pod a stable DNS name of its own
    (<pod>.<headless>), which KAFKA_ADVERTISED_LISTENERS and
    KAFKA_CONTROLLER_QUORUM_VOTERS both need;
  - publishNotReadyAddresses makes those names resolve *before* the pod is
    Ready.

That second one is the difference between a cluster that forms and one that
deadlocks. A normal Service only has endpoints once its pods pass readiness,
and a KRaft broker only passes readiness once it has registered with the
controller quorum - which it reaches through those same endpoints. The
single-node chart sidestepped this with 1@localhost:9093; three nodes cannot,
because a node cannot reach two other nodes on localhost.
*/}}
{{- define "pubsub-infra.kafkaHeadlessService" -}}
{{ include "pubsub-infra.fullname" . }}-kafka-headless
{{- end -}}

{{- define "pubsub-infra.postgresService" -}}
{{ include "pubsub-infra.fullname" . }}-postgres
{{- end -}}

{{/*
CloudNativePG creates this Service and keeps its endpoints pointed at
whichever instance is currently primary, rewriting it on every failover.
This - not postgresService - is what PG_DSN must use: the bare cluster name
resolves to no Service at all, because CNPG owns the naming.
*/}}
{{- define "pubsub-infra.postgresWriteService" -}}
{{ include "pubsub-infra.postgresService" . }}-rw
{{- end -}}

{{- define "pubsub-infra.clickhouseService" -}}
{{ include "pubsub-infra.fullname" . }}-clickhouse
{{- end -}}

{{/*
Headless companion for ClickHouse: per-pod DNS for Keeper's Raft peers and for
the ON CLUSTER member list, resolving before the pods are Ready - Keeper
cannot form if its members cannot name each other first.
*/}}
{{- define "pubsub-infra.clickhouseHeadlessService" -}}
{{ include "pubsub-infra.clickhouseService" . }}-headless
{{- end -}}

{{/*
Pod spread for a component of this chart.

Preferred rather than required on purpose. Required anti-affinity refuses to
schedule when there is nowhere else to go - which on a single-node cluster
(a laptop, a staging box, and the kind cluster these charts are validated
against) means the store never starts at all. Preferred gets real node-level
spread the moment a second node exists and degrades to "no preference" when
there is only one, which is the correct fallback: three replicas on one node
are still better than none.
*/}}
{{- define "pubsub-infra.spread" -}}
{{- $root := .root -}}
{{- $component := .component -}}
podAntiAffinity:
  preferredDuringSchedulingIgnoredDuringExecution:
    - weight: 100
      podAffinityTerm:
        topologyKey: kubernetes.io/hostname
        labelSelector:
          matchLabels:
            {{- include "pubsub-infra.selectorLabels" $root | nindent 12 }}
            app.kubernetes.io/component: {{ $component }}
{{- end -}}

{{- define "pubsub-infra.credentialsSecret" -}}
{{ include "pubsub-infra.fullname" . }}-credentials
{{- end -}}
