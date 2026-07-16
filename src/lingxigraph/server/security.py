"""OIDC/JWT authentication, tenant mapping, and fixed-role RBAC."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

ROLES = {"viewer", "operator", "developer", "tenant-admin"}


@dataclass(frozen=True, slots=True)
class Principal:
    subject: str
    tenant_id: str
    roles: frozenset[str] = field(default_factory=frozenset)
    claims: Mapping[str, Any] = field(default_factory=dict)

    def require(self, *roles: str) -> None:
        if "tenant-admin" in self.roles or self.roles.intersection(roles):
            return
        raise PermissionError("required role: " + " or ".join(roles))


@dataclass(frozen=True, slots=True)
class AuthSettings:
    issuer: str | None = None
    audience: str | None = None
    jwks_url: str | None = None
    tenant_claim: str = "tenant_id"
    roles_claim: str = "roles"
    allow_insecure_dev: bool = False
    dev_api_keys: Mapping[str, tuple[str, tuple[str, ...]]] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> AuthSettings:
        keys: dict[str, tuple[str, tuple[str, ...]]] = {}
        raw_key = os.getenv("LINGXIGRAPH_DEV_API_KEY")
        if raw_key:
            keys[raw_key] = (
                os.getenv("LINGXIGRAPH_DEV_TENANT", "default"),
                tuple(
                    part.strip()
                    for part in os.getenv(
                        "LINGXIGRAPH_DEV_ROLES", "tenant-admin"
                    ).split(",")
                    if part.strip()
                ),
            )
        return cls(
            issuer=os.getenv("LINGXIGRAPH_OIDC_ISSUER"),
            audience=os.getenv("LINGXIGRAPH_OIDC_AUDIENCE"),
            jwks_url=os.getenv("LINGXIGRAPH_OIDC_JWKS_URL"),
            tenant_claim=os.getenv("LINGXIGRAPH_TENANT_CLAIM", "tenant_id"),
            roles_claim=os.getenv("LINGXIGRAPH_ROLES_CLAIM", "roles"),
            allow_insecure_dev=os.getenv("LINGXIGRAPH_INSECURE_DEV_AUTH", "false").lower()
            == "true",
            dev_api_keys=keys,
        )


class Authenticator:
    def __init__(self, settings: AuthSettings | None = None) -> None:
        self.settings = settings or AuthSettings.from_env()
        self._jwk_client: Any | None = None
        if self.settings.jwks_url:
            try:
                import jwt
            except ImportError as exc:  # pragma: no cover
                raise RuntimeError("install lingxigraph[server] for JWT support") from exc
            self._jwk_client = jwt.PyJWKClient(self.settings.jwks_url)

    @classmethod
    def insecure_dev(cls) -> Authenticator:
        return cls(AuthSettings(allow_insecure_dev=True))

    async def authenticate(
        self,
        authorization: str | None,
        *,
        api_key: str | None = None,
        dev_tenant: str | None = None,
        dev_roles: str | None = None,
    ) -> Principal:
        if api_key and api_key in self.settings.dev_api_keys:
            tenant, api_roles = self.settings.dev_api_keys[api_key]
            return Principal(f"api-key:{api_key[:6]}", tenant, frozenset(api_roles))
        if authorization and authorization.lower().startswith("bearer "):
            return await self._decode_jwt(authorization.split(" ", 1)[1])
        if self.settings.allow_insecure_dev:
            roles: frozenset[str] = frozenset(
                role.strip()
                for role in (dev_roles or "tenant-admin").split(",")
                if role.strip() in ROLES
            )
            return Principal("insecure-development", dev_tenant or "default", roles)
        raise PermissionError("missing bearer token or API key")

    async def _decode_jwt(self, token: str) -> Principal:
        if self._jwk_client is None or not self.settings.issuer or not self.settings.audience:
            raise PermissionError("OIDC issuer, audience, and JWKS URL must be configured")
        import jwt

        signing_key = await asyncio.to_thread(self._jwk_client.get_signing_key_from_jwt, token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256", "ES256"],
            audience=self.settings.audience,
            issuer=self.settings.issuer,
            options={"require": ["exp", "iat", "sub"]},
        )
        tenant = claims.get(self.settings.tenant_claim)
        if not tenant:
            raise PermissionError(f"token lacks {self.settings.tenant_claim!r} claim")
        raw_roles = claims.get(self.settings.roles_claim, ())
        if isinstance(raw_roles, str):
            raw_roles = raw_roles.split()
        roles = frozenset(str(role) for role in raw_roles if str(role) in ROLES)
        return Principal(str(claims["sub"]), str(tenant), roles, claims)


__all__ = ["AuthSettings", "Authenticator", "Principal", "ROLES"]
