from dataclasses import dataclass, field
from string import Template
from typing import Any, Dict, List

@dataclass
class Query:
    id: str
    sql: str
    groups: List[str] = field(default_factory=list)
    params: Dict[str, str] = field(default_factory=dict)


    def render(self) -> str:
        sql = Template(self.sql)
        sql = sql.safe_substitute(**self.params)
        return str(sql)

    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "groups": self.groups,
            "params": self.params,
            "sql": self.sql,
        }


    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Query":
        return cls(
            id=data["id"],
            sql=data["sql"],
            groups=data.get("groups", []),
            params=data.get("params", {}),
        )


    def render(self) -> str:
        sql = Template(self.sql)
        sql = sql.safe_substitute(**self.params)
        return str(sql)