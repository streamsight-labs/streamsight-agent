{{- define "streamsight-agent.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "streamsight-agent.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "streamsight-agent.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{ include "streamsight-agent.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: metrics-agent
app.kubernetes.io/part-of: streamsight
{{- end }}

{{- define "streamsight-agent.selectorLabels" -}}
app.kubernetes.io/name: {{ include "streamsight-agent.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "streamsight-agent.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "streamsight-agent.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
validate rejects, at render time, the combinations the agent would reject at
startup -- plus the two that Kubernetes alone would turn into a silent failure:
an EXPORT_FILE outside the writable mount, and a rotation budget larger than
the volume. Failing here means `helm template` in CI catches it, rather than a
CrashLoopBackOff catching it.
*/}}
{{- define "streamsight-agent.validate" -}}
{{- if not .Values.kafka.brokers }}
{{- fail "kafka.brokers is required (comma separated host:port list)" }}
{{- end }}

{{- $mode := .Values.export.mode | toString }}
{{- if not (has $mode (list "file" "http" "stdout")) }}
{{- fail (printf "export.mode %q is not one of file, http, stdout" $mode) }}
{{- end }}

{{- if eq $mode "http" }}
  {{- if not .Values.export.http.endpoint }}
  {{- fail "export.mode is http, so export.http.endpoint is required" }}
  {{- end }}
  {{- if and (not .Values.export.http.apiKey) (not .Values.export.http.existingSecret) }}
  {{- fail "export.mode is http, so set export.http.apiKey or export.http.existingSecret" }}
  {{- end }}
{{- end }}

{{- if eq $mode "file" }}
  {{- $mount := .Values.export.file.mountPath | toString | trimSuffix "/" }}
  {{- if not $mount }}
  {{- fail "export.file.mountPath is required in file mode" }}
  {{- end }}
  {{- if not (hasPrefix (printf "%s/" $mount) (.Values.export.file.path | toString)) }}
  {{- fail (printf "export.file.path %q must live under export.file.mountPath %q -- the container root filesystem is read-only, so nothing outside the mount is writable" .Values.export.file.path $mount) }}
  {{- end }}
{{- end }}

{{- if .Values.kafka.sasl.enabled }}
  {{- if not (has (.Values.kafka.sasl.mechanism | toString) (list "PLAIN" "SCRAM-SHA-256" "SCRAM-SHA-512")) }}
  {{- fail (printf "kafka.sasl.mechanism %q is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512" .Values.kafka.sasl.mechanism) }}
  {{- end }}
  {{- if not .Values.kafka.sasl.username }}
  {{- fail "kafka.sasl.enabled is true, so kafka.sasl.username is required" }}
  {{- end }}
  {{- if and (not .Values.kafka.sasl.password) (not .Values.kafka.sasl.existingSecret) }}
  {{- fail "kafka.sasl.enabled is true, so set kafka.sasl.password or kafka.sasl.existingSecret" }}
  {{- end }}
{{- end }}

{{- if not (has (.Values.agent.logLevel | toString) (list "debug" "info" "warn" "warning" "error")) }}
{{- fail (printf "agent.logLevel %q is not one of debug, info, warn, error" .Values.agent.logLevel) }}
{{- end }}
{{- end }}
