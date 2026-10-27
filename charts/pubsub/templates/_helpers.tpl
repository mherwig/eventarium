{{/*
Object names. The infra chart prefixes its objects with its own chart name,
so "pubsub-*" here never collides with "pubsub-infra-*" there - but the
release name is still part of the selector labels, so two installs of this
chart in one namespace stay apart.
*/}}
{{- define "pubsub.fullname" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "pubsub.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "pubsub.fullname" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Stable subset: name, instance and the component - a pod template may not
     change any of these without replacing every pod. */}}
{{- define "pubsub.componentSelectorLabels" -}}
app.kubernetes.io/name: {{ include "pubsub.fullname" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .name }}
{{- end -}}

{{/*
The secret every pod reads its configuration from: the one the chart
renders, or the one the operator pointed at with existingSecret.
*/}}
{{- define "pubsub.configSecret" -}}
{{- if .Values.existingSecret -}}
{{- .Values.existingSecret -}}
{{- else -}}
{{- printf "%s-config" (include "pubsub.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* Per-component settings merged over the chart defaults. */}}
{{- define "pubsub.componentImage" -}}
{{ printf "%s:%s" .root.Values.image.repository (.root.Values.image.tag | toString) }}
{{- end -}}

{{- define "pubsub.componentReplicas" -}}
{{- .c.replicas | default .root.Values.replicaCount -}}
{{- end -}}

{{- define "pubsub.componentResources" -}}
{{- toYaml (.c.resources | default .root.Values.resources) -}}
{{- end -}}
