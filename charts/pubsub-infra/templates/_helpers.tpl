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

{{- define "pubsub-infra.postgresService" -}}
{{ include "pubsub-infra.fullname" . }}-postgres
{{- end -}}

{{- define "pubsub-infra.clickhouseService" -}}
{{ include "pubsub-infra.fullname" . }}-clickhouse
{{- end -}}

{{- define "pubsub-infra.credentialsSecret" -}}
{{ include "pubsub-infra.fullname" . }}-credentials
{{- end -}}
