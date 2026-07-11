from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping


@dataclass(frozen=True)
class ModuleCSemanticContract:
    version: str
    config_hash: str
    chan_config: Mapping[str, bool]

    def build_chan_config(self, base_config: Mapping[str, Any]) -> dict[str, Any]:
        """Build the one mutable config object consumed by CChanConfig."""
        config = dict(base_config)
        config.update(self.chan_config)
        return config


MODULE_C_SEMANTICS = ModuleCSemanticContract(
    version="native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false",
    config_hash="module-c:native-5lvl-v4-bi-strict-false-bi-allow-sub-peak-false",
    chan_config=MappingProxyType({
        "bi_strict": False,
        "bi_allow_sub_peak": False,
    }),
)

MODULE_C_CONFIG_HASH = MODULE_C_SEMANTICS.config_hash
