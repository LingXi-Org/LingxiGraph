{{- define "lingxigraph.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lingxigraph.fullname" -}}
{{- printf "%s-%s" .Release.Name (include "lingxigraph.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "lingxigraph.labels" -}}
app.kubernetes.io/name: {{ include "lingxigraph.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "lingxigraph.env" -}}
- name: LINGXIGRAPH_POSTGRES_URL
  valueFrom:
    secretKeyRef:
      name: {{ .Values.secrets.existingSecret }}
      key: postgres-url
- name: LINGXIGRAPH_REDIS_URL
  value: {{ .Values.config.redisUrl | quote }}
- name: LINGXIGRAPH_OIDC_ISSUER
  value: {{ .Values.config.oidcIssuer | quote }}
- name: LINGXIGRAPH_OIDC_AUDIENCE
  value: {{ .Values.config.oidcAudience | quote }}
- name: LINGXIGRAPH_OIDC_JWKS_URL
  value: {{ .Values.config.oidcJwksUrl | quote }}
- name: LINGXIGRAPH_TENANT_CLAIM
  value: {{ .Values.config.tenantClaim | quote }}
- name: LINGXIGRAPH_ROLES_CLAIM
  value: {{ .Values.config.rolesClaim | quote }}
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: {{ .Values.config.otelEndpoint | quote }}
{{- end -}}
