from pydantic import BaseModel, Field, validator
from typing import Optional, List, Dict, Any
from datetime import datetime
from enum import Enum

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# UnifiedDocument v1: canonical representation for all ingested sources.


class SourceType(str, Enum):
    PDF = "PDF"
    TRANSCRIPT = "Transcript"
    HTML = "HTML"
    CSV = "CSV"
    CODE = "Code"
    VIDEO = "Video"
    OTHER = "Other"


class UnifiedDocument(BaseModel):
    """Canonical document schema used across the pipeline (v1).

    Keep this schema stable for ingestion. A migration to v2 will be
    handled separately when the mid-lab incident occurs.
    """

    document_id: str = Field(..., description="Unique id for the document")
    title: Optional[str] = Field(None, description="Optional human-readable title")
    summary: Optional[str] = Field(None, description="Short summary / abstract")
    content: Optional[str] = Field("", description="Primary extracted textual content")
    source_type: SourceType = Field(..., description="Type of source (PDF/HTML/CSV/...)" )
    author: Optional[str] = Field("Unknown", description="Author or creator if known")
    timestamp: Optional[datetime] = Field(None, description="Optional UTC timestamp when content was produced or extracted")
    tags: List[str] = Field(default_factory=list, description="Optional list of tags or topics")
    language: Optional[str] = Field(None, description="ISO language code, if known")

    # Source-specific metadata (e.g., page count, original filename, table locations)
    source_metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        orm_mode = True
        use_enum_values = True

    @validator("timestamp", pre=True, always=False)
    def _parse_timestamp(cls, v):
        if v is None:
            return None
        if isinstance(v, datetime):
            return v
        # support ISO strings and unix timestamps
        try:
            return datetime.fromisoformat(str(v))
        except Exception:
            pass
        try:
            # numeric epoch
            return datetime.fromtimestamp(float(v))
        except Exception:
            return None

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serializable dict suitable for writing to disk or passing
        between pipeline stages.
        """
        return self.dict()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "UnifiedDocument":
        return cls(**data)