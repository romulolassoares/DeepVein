import pytest

from src.query_registry import QueryRegistry
from src.query_registry import Query

@pytest.fixture
def registry():
    with QueryRegistry(":memory:") as registry:
        yield registry
        
def test_add_and_get(registry):
    query = Query(id="q1", sql="SELECT 1")
    registry.add(query)
    assert registry.get("q1").sql == "SELECT 1"
    
def test_get_missing_raises_key_error(registry):
    with pytest.raises(KeyError):
        registry.get("ghost")
        
def test_update(registry):
    query = Query(id="q1", sql="SELECT 1")
    query_upd = Query(id="q1", sql="SELECT 2")
    registry.add(query)
    registry.update(query_upd)
    
    assert registry.get("q1").sql == query_upd.sql
    
def test_delete(registry):
    query = Query(id="q1", sql="SELECT 1")
    registry.add(query)
    
    assert registry.delete("q1") is True
    
    with pytest.raises(KeyError):
        registry.get("q1")
        
def test_delete_nonexistent_returns_false(registry):
    assert registry.delete("ghost") is False


def test_get_by_group(registry):
    query1 = Query(id="q1", sql="SELECT 1", groups=["g"])
    query2 = Query(id="q2", sql="SELECT 2", groups=["g"])

    registry.add(query1)
    registry.add(query2)

    results = registry.get_by_group("g")
    assert len(results) == 2
    assert {q.id for q in results} == {"q1", "q2"}


def test_get_by_group_empty_for_unknown_group(registry):
    registry.add(Query(id="q1", sql="SELECT 1", groups=["other"]))
    assert registry.get_by_group("missing") == []


def test_update_persists_new_sql(registry):
    registry.add(Query(id="q1", sql="SELECT 1", groups=["g"]))
    registry.update(Query(id="q1", sql="SELECT 99", groups=["g2"]))
    updated = registry.get("q1")
    assert updated.sql == "SELECT 99"
    assert updated.groups == ["g2"]
