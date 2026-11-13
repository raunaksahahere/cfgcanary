{{- define "cfgcanary.name" -}}{{ .Chart.Name }}{{- end -}}
{{- define "cfgcanary.fullname" -}}{{ default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}{{- end -}}
{{- define "cfgcanary.image" -}}{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}{{- end -}}
{{- define "cfgcanary.labels" -}}
app.kubernetes.io/name: {{ include "cfgcanary.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}
