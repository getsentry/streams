{{- define "streaming-operator.name" -}}
{{- .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "streaming-operator.fullname" -}}
{{- include "streaming-operator.name" . -}}
{{- end -}}

{{- define "streaming-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "streaming-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "streaming-operator.labels" -}}
{{ include "streaming-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- with .Values.labels }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{- define "streaming-operator.serviceAccountName" -}}
{{- default (include "streaming-operator.fullname" .) .Values.serviceAccount.name -}}
{{- end -}}

{{- define "streaming-operator.workloadNamespace" -}}
{{- required "workloadNamespace must be set" .Values.workloadNamespace -}}
{{- end -}}
