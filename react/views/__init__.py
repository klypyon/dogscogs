"""react.views package shim.

Exports run_reactrole_wizard, ReactRoleWizardView, WizardState, and ReactRoleListView
to preserve the previous public API while splitting implementations.
"""
from .wizard import run_reactrole_wizard, ReactRoleWizardView, WizardState
from .list import ReactRoleListView

__all__ = ["run_reactrole_wizard", "ReactRoleWizardView", "WizardState", "ReactRoleListView"]
"""Views package for the react cog.

This package exposes the same public API as the original
``react.views`` module so external imports remain unchanged.
"""
from .wizard import run_reactrole_wizard, ReactRoleWizardView, WizardState
from .list import ReactRoleListView

__all__ = [
    "run_reactrole_wizard",
    "ReactRoleWizardView",
    "WizardState",
    "ReactRoleListView",
]
