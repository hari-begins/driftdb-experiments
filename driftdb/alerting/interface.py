"""
driftdb.alerting.interface
=============================
Alerting Interface: surfaces detected drift and adaptation actions to
administrators, with an option for manual override (proposal's
architecture component + the "fall back to logging alerts for manual admin
action" risk mitigation).

Ships with a logging sink (always on) and an optional webhook sink (real
HTTP POST via `requests`, used for e.g. Slack/PagerDuty integration). An
admin can call `suppress(signature, component)` to force manual-only mode
for a specific component -- the Adaptor checks this before acting.
"""
from __future__ import annotations

import dataclasses
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

logger = logging.getLogger("driftdb.alerting")


@dataclasses.dataclass
class Alert:
    kind: str                 # "drift_detected" | "adaptation_started" | "adaptation_completed" | "adaptation_skipped"
    signature: str
    component: str
    severity: Optional[str]
    detail: Dict[str, Any]
    timestamp: float = dataclasses.field(default_factory=time.time)


class AlertingInterface:
    def __init__(self, webhook_url: Optional[str] = None, log_file: Optional[str] = None):
        self.webhook_url = webhook_url
        self._suppressed: Set[Tuple[str, str]] = set()
        self._history: List[Alert] = []
        self._log_file = log_file

    def suppress(self, signature: str, component: str) -> None:
        """Manual override: stop DriftDB from auto-adapting this
        (signature, component); alerts still fire so an admin can act."""
        self._suppressed.add((signature, component))

    def unsuppress(self, signature: str, component: str) -> None:
        self._suppressed.discard((signature, component))

    def is_suppressed(self, signature: str, component: str) -> bool:
        return (signature, component) in self._suppressed

    def emit(self, alert: Alert) -> None:
        self._history.append(alert)
        logger.warning("[driftdb-alert] %s sig=%s component=%s severity=%s detail=%s",
                        alert.kind, alert.signature, alert.component, alert.severity, alert.detail)
        if self._log_file:
            with open(self._log_file, "a") as f:
                f.write(json.dumps(dataclasses.asdict(alert)) + "\n")
        if self.webhook_url:
            try:
                import requests
                requests.post(self.webhook_url, json=dataclasses.asdict(alert), timeout=3)
            except Exception:
                logger.exception("Failed to deliver webhook alert")

    def recent(self, n: int = 50) -> List[Alert]:
        return self._history[-n:]
