"""Azure data-source naming must use the ORG/subscription identity, never
the signed-in user's name.

Bug: onboarding an Azure data source stored the admin's name ("Amit Mishra")
as the tenant display_name instead of the org ("QFION Software"). Root cause:
auth-service derived display_name from the OAuth user's JWT `name` claim, and
the subscription-name path was gated behind `if not external_tenant_id` (which
is skipped whenever the JWT carries `tid`). The fix routes naming through this
pure helper, which structurally cannot return the user's name.
"""
from __future__ import annotations

from shared.azure_naming import derive_azure_datasource_name


def test_prefers_existing_org_name():
    # When the same Azure AD tenant was already onboarded as M365, reuse its
    # org display_name so Azure shows "QFION Software", not the admin.
    assert derive_azure_datasource_name(
        arm_subscriptions=[{"displayName": "Pay-As-You-Go"}],
        external_tenant_id="0cac6ab1-bbd3-4e27-8325-333c51e4567d",
        existing_org_name="QFION Software",
    ) == "QFION Software"


def test_falls_back_to_subscription_name():
    assert derive_azure_datasource_name(
        arm_subscriptions=[{"displayName": "Prod Subscription"}],
        external_tenant_id="0cac6ab1-bbd3-4e27-8325-333c51e4567d",
        existing_org_name=None,
    ) == "Azure - Prod Subscription"


def test_falls_back_to_tenant_id_generic_not_user():
    # No org, no subscription → a tenant-id-keyed generic. Never a person name.
    name = derive_azure_datasource_name(
        arm_subscriptions=None,
        external_tenant_id="0cac6ab1-bbd3-4e27-8325-333c51e4567d",
        existing_org_name=None,
    )
    assert name == "Azure Tenant 0cac6ab1"


def test_last_resort_when_nothing_known():
    assert derive_azure_datasource_name(
        arm_subscriptions=[], external_tenant_id=None, existing_org_name=None
    ) == "Azure Tenant"
