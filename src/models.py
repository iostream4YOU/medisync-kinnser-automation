from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class OrderRecord:
    source_row: int
    order_number: str
    patient_name: str
    mrn: str
    episode: str
    order_type: str
    order_date: str
    physician_name: str
    clinic: str
    npi: Optional[str] = None
    dob: Optional[str] = None
    phone: Optional[str] = None
    pdf_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    validation_errors: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.validation_errors) == 0


@dataclass
class SyncResult:
    order_number: str
    patient_name: str
    status: str
    reason: str = ""
    patient_id: Optional[str] = None
    physician_id: Optional[str] = None
    episode_id: Optional[str] = None
    order_id: Optional[str] = None
    document_id: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "order_number": self.order_number,
            "patient_name": self.patient_name,
            "status": self.status,
            "reason": self.reason,
            "patient_id": self.patient_id,
            "physician_id": self.physician_id,
            "episode_id": self.episode_id,
            "order_id": self.order_id,
            "document_id": self.document_id,
        }
