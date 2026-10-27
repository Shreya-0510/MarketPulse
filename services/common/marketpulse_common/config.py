"""Typed loading and cross-validation of config/*.yaml. Fails fast with actionable errors."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ConfigError(RuntimeError):
    pass


class _Cfg(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Instrument(_Cfg):
    entity_id: str
    symbol: str
    display_name: str
    market: str
    exchange: str | None = None
    instrument_type: str
    currency: str
    timezone: str


class EntityRule(_Cfg):
    strong_terms: list[str] = Field(default_factory=list)
    context_terms: list[str] = Field(default_factory=list)
    context_required: list[str] = Field(default_factory=list)
    exclude_terms: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check(self):
        if not (self.strong_terms or self.context_terms):
            raise ValueError("entity needs strong_terms or context_terms")
        if self.context_terms and not self.context_required:
            raise ValueError("context_terms require context_required (ambiguous-term safeguard)")
        return self


class Source(_Cfg):
    name: str
    type: str
    url: str
    enabled: bool = True
    poll_interval_seconds: int = Field(ge=0)
    timeout_seconds: int = Field(gt=0)
    options: dict[str, Any] = Field(default_factory=dict)


class Topics(_Cfg):
    market_raw: str
    social_raw: str
    news_raw: str
    analytics_entity: str
    analytics_attention: str
    analytics_anomalies: str
    system_pipeline: str


class KafkaCfg(_Cfg):
    topics: Topics
    raw_partitions: int = Field(gt=0)
    derived_partitions: int = Field(gt=0)
    retention_ms: int = Field(gt=0)


class Settings(_Cfg):
    kafka: KafkaCfg
    market: dict[str, Any]
    sentiment: dict[str, Any]
    anomaly: dict[str, Any]
    retention: dict[str, Any]

    @model_validator(mode="after")
    def _thresholds(self):
        if self.sentiment["negative_threshold"] >= self.sentiment["positive_threshold"]:
            raise ValueError("sentiment negative_threshold must be < positive_threshold")
        return self


class AppConfig(_Cfg):
    instruments: list[Instrument]
    entities: dict[str, EntityRule]
    sources: list[Source]
    settings: Settings

    @model_validator(mode="after")
    def _xref(self):
        ids = [i.entity_id for i in self.instruments]
        syms = [i.symbol for i in self.instruments]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate entity_id in watchlist")
        if len(set(syms)) != len(syms):
            raise ValueError("duplicate symbol in watchlist")
        unknown = set(self.entities) - set(ids)
        if unknown:
            raise ValueError(f"entities.yaml references unknown entity_id(s): {sorted(unknown)}")
        names = [s.name for s in self.sources]
        if len(set(names)) != len(names):
            raise ValueError("duplicate source name")
        return self


def config_dir() -> Path:
    return Path(os.environ.get("MARKETPULSE_CONFIG_DIR", "config"))


def _read(path: Path) -> Any:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except FileNotFoundError as e:
        raise ConfigError(f"missing config file: {path}") from e
    except yaml.YAMLError as e:
        raise ConfigError(f"invalid YAML in {path}: {e}") from e


def load_config(directory: Path | str | None = None) -> AppConfig:
    d = Path(directory) if directory else config_dir()
    try:
        return AppConfig(
            instruments=_read(d / "watchlist.yaml")["instruments"],
            entities=_read(d / "entities.yaml")["entities"],
            sources=_read(d / "sources.yaml")["sources"],
            settings=_read(d / "settings.yaml"),
        )
    except (KeyError, TypeError) as e:
        raise ConfigError(f"malformed config in {d}: {e!r}") from e
    except ValueError as e:  # pydantic.ValidationError subclasses ValueError
        raise ConfigError(f"invalid config in {d}: {e}") from e
