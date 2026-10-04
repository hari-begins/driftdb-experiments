"""
driftdb.store.model_store
============================
Model / Policy Store: holds the current cost model, cardinality estimator,
and tuned configuration per query signature, versioned on disk so a bad
adaptation can be rolled back (per the proposal's risk-mitigation table:
"Version models for rollback; apply adaptation incrementally rather than
atomically swapping components").

Storage format is deliberately simple (pickled sklearn-compatible
estimators + JSON metadata) so it has zero exotic dependencies; swap
`_serialize`/`_deserialize` for your org's model registry if you have one
(MLflow, etc.) without touching the rest of DriftDB.
"""
from __future__ import annotations

import dataclasses
import json
import os
import pickle
import shutil
import time
from typing import Any, Dict, List, Optional


@dataclasses.dataclass
class ModelVersion:
    signature: str
    component: str          # "cardinality_estimator" | "cost_model" | "tuned_config"
    version: int
    created_at: float
    metadata: Dict[str, Any]
    path: str


class ModelStore:
    def __init__(self, root: str):
        self.root = root
        os.makedirs(root, exist_ok=True)

    def _component_dir(self, signature: str, component: str) -> str:
        d = os.path.join(self.root, signature, component)
        os.makedirs(d, exist_ok=True)
        return d

    def _index_path(self, signature: str, component: str) -> str:
        return os.path.join(self._component_dir(signature, component), "index.json")

    def _load_index(self, signature: str, component: str) -> List[dict]:
        p = self._index_path(signature, component)
        if not os.path.exists(p):
            return []
        with open(p) as f:
            return json.load(f)

    def _save_index(self, signature: str, component: str, index: List[dict]) -> None:
        with open(self._index_path(signature, component), "w") as f:
            json.dump(index, f, indent=2)

    def put(self, signature: str, component: str, obj: Any,
            metadata: Optional[Dict[str, Any]] = None) -> ModelVersion:
        """Persist a new version of `obj` for (signature, component).
        `obj` can be any pickleable model/policy object, or for
        `tuned_config` a plain dict of Spark conf key/value overrides."""
        index = self._load_index(signature, component)
        next_version = (index[-1]["version"] + 1) if index else 1
        fname = f"v{next_version}.pkl"
        path = os.path.join(self._component_dir(signature, component), fname)
        with open(path, "wb") as f:
            pickle.dump(obj, f)
        entry = ModelVersion(
            signature=signature, component=component, version=next_version,
            created_at=time.time(), metadata=metadata or {}, path=path,
        )
        index.append(dataclasses.asdict(entry))
        self._save_index(signature, component, index)
        return entry

    def latest(self, signature: str, component: str) -> Optional[ModelVersion]:
        index = self._load_index(signature, component)
        if not index:
            return None
        return ModelVersion(**index[-1])

    def load_object(self, version: ModelVersion) -> Any:
        with open(version.path, "rb") as f:
            return pickle.load(f)

    def rollback(self, signature: str, component: str) -> Optional[ModelVersion]:
        """Discard the latest version, exposing the previous one -- used
        when an adaptation is judged (via the scheduler's post-hoc check,
        or a manual admin override) to have made things worse."""
        index = self._load_index(signature, component)
        if len(index) < 2:
            return None
        removed = index.pop()
        try:
            os.remove(removed["path"])
        except OSError:
            pass
        self._save_index(signature, component, index)
        return ModelVersion(**index[-1])

    def history(self, signature: str, component: str) -> List[ModelVersion]:
        return [ModelVersion(**e) for e in self._load_index(signature, component)]

    def wipe(self, signature: str) -> None:
        d = os.path.join(self.root, signature)
        if os.path.exists(d):
            shutil.rmtree(d)
