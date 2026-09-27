"""The shape of a data contract.

A contract is the agreement between whoever produces a source and
whoever consumes it: these columns, these types, nulls allowed here and
not there, values in this range, refreshed this often, and this person
answers the phone when it breaks.

It lives in YAML rather than in the control plane on purpose. The
control plane is operational state the platform writes; a contract is a
human-authored, reviewed artefact that belongs in git next to the code,
where a change to it shows up in a pull request. `sync_contract`
compiles it *into* the control plane, so the two are not rival sources
of truth — one generates the other.

Pydantic validates the file itself. A contract with a typo is a broken
contract, and finding that out at load time beats finding out when a
rule silently never fires.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lakehouse.metadata.enums import Severity

Name = Annotated[str, Field(min_length=1, max_length=200)]


class Expectation(BaseModel):
    """One checkable claim about a column, beyond its type.

    Every expectation carries its own severity, because "this must never
    be null" and "this is usually one of five values" deserve different
    consequences and are both legitimate things to write down.
    """

    model_config = ConfigDict(extra="forbid")

    unique: bool = False
    allowed_values: list[str] | None = None
    minimum: float | None = None
    maximum: float | None = None
    pattern: str | None = None
    severity: Severity = Severity.WARN

    @model_validator(mode="after")
    def _at_least_one(self) -> Expectation:
        if not any(
            (
                self.unique,
                self.allowed_values is not None,
                self.minimum is not None,
                self.maximum is not None,
                self.pattern is not None,
            )
        ):
            raise ValueError("an expectation must state at least one check")
        return self

    @model_validator(mode="after")
    def _range_is_ordered(self) -> Expectation:
        if self.minimum is not None and self.maximum is not None and self.minimum > self.maximum:
            raise ValueError(f"minimum {self.minimum} is above maximum {self.maximum}")
        return self


class ColumnContract(BaseModel):
    """A column the source promises to provide."""

    model_config = ConfigDict(extra="forbid")

    name: Name
    type: Name
    """Arrow type name, as `str(field.type)` renders it — `string`, `int64`."""

    nullable: bool = True
    description: str | None = None
    expect: Expectation | None = None
    null_severity: Severity = Severity.FAIL
    """Consequence when a non-nullable column contains nulls."""

    @model_validator(mode="after")
    def _nullable_severity_is_meaningful(self) -> ColumnContract:
        if self.nullable and self.null_severity != Severity.FAIL:
            raise ValueError(f"column '{self.name}' is nullable, so null_severity has no effect")
        return self


class Contract(BaseModel):
    """Everything promised about one source object."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[1] = 1
    source_system: Name
    object: Name
    owner: Name
    """Who to contact when this breaks. A contract without an owner is a wish."""

    description: str | None = None
    freshness_sla_minutes: int | None = Field(default=None, gt=0)
    freshness_column: Name | None = None
    columns: list[ColumnContract] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_column_names(self) -> Contract:
        names = [c.name for c in self.columns]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate column(s) in contract: {', '.join(duplicates)}")
        return self

    @model_validator(mode="after")
    def _freshness_needs_a_column(self) -> Contract:
        if self.freshness_sla_minutes is not None and self.freshness_column is None:
            raise ValueError("freshness_sla_minutes requires freshness_column")
        if self.freshness_column is not None:
            known = {c.name for c in self.columns}
            if self.freshness_column not in known:
                raise ValueError(
                    f"freshness_column '{self.freshness_column}' is not a contracted column"
                )
        return self

    def column(self, name: str) -> ColumnContract | None:
        return next((c for c in self.columns if c.name == name), None)
