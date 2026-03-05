from __future__ import annotations

from typing import List, Optional
from pydantic import BaseModel, Field

class EconomicActivity(BaseModel):
    name: str
    order: Optional[int] = None
    percentage: Optional[float] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class TaxRegime(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    startDate: Optional[str] = None
    endDate: Optional[str] = None

class TaxStatus(BaseModel):
    rfc: str
    status: Optional[str] = None
    economicActivities: List[EconomicActivity] = Field(default_factory=list)
    taxRegimes: List[TaxRegime] = Field(default_factory=list)
