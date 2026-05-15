import pytest

from src.query_registry.models import Query
from src.query_registry.store import Store


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def test_upsert_and_get(store):
    q = Query(id="q1", sql="SELECT 1", groups=["g1"], params={"k": "v"})
    store.upsert(q)
    result = store.get("q1")
    assert result.id == "q1"
    assert result.sql == "SELECT 1"
    assert "g1" in result.groups
    assert result.params == {"k": "v"}


def test_get_nonexistent_returns_none(store):
    assert store.get("missing") is None


def test_delete_existing(store):
    store.upsert(Query(id="q1", sql="SELECT 1"))
    assert store.delete("q1") is True
    assert store.get("q1") is None


def test_delete_nonexistent_returns_false(store):
    assert store.delete("ghost") is False


def test_upsert_replaces_groups_on_update(store):
    store.upsert(Query(id="q1", sql="SELECT 1", groups=["old"]))
    store.upsert(Query(id="q1", sql="SELECT 2", groups=["new"]))
    result = store.get("q1")
    assert result.groups == ["new"]
    assert result.sql == "SELECT 2"


def test_upsert_query_with_no_groups(store):
    store.upsert(Query(id="q1", sql="SELECT 1"))
    result = store.get("q1")
    assert result.groups == []


def test_upsert_query_with_multiple_groups(store):
    store.upsert(Query(id="q1", sql="SELECT 1", groups=["a", "b", "c"]))
    result = store.get("q1")
    assert set(result.groups) == {"a", "b", "c"}


def test_get_by_group_returns_matching_queries(store):
    store.upsert(Query(id="q1", sql="SELECT 1", groups=["analytics"]))
    store.upsert(Query(id="q2", sql="SELECT 2", groups=["analytics", "reports"]))
    store.upsert(Query(id="q3", sql="SELECT 3", groups=["reports"]))
    results = store.get_by_group("analytics")
    ids = {q.id for q in results}
    assert ids == {"q1", "q2"}


def test_get_by_group_empty_when_no_match(store):
    store.upsert(Query(id="q1", sql="SELECT 1", groups=["other"]))
    assert store.get_by_group("missing_group") == []


def test_get_groups_returns_distinct(store):
    store.upsert(Query(id="q1", sql="SELECT 1", groups=["a", "b"]))
    store.upsert(Query(id="q2", sql="SELECT 2", groups=["b", "c"]))
    groups = store.get_groups()
    assert set(groups) == {"a", "b", "c"}


def test_get_groups_empty_when_no_groups(store):
    store.upsert(Query(id="q1", sql="SELECT 1"))
    assert store.get_groups() == []


def test_delete_cascades_groups(store):
    store.upsert(Query(id="q1", sql="SELECT 1", groups=["g"]))
    store.delete("q1")
    assert store.get_by_group("g") == []


def test_context_manager_closes_connection():
    with Store(":memory:") as s:
        s.upsert(Query(id="q1", sql="SELECT 1"))
    # after __exit__ the connection is closed; further operations raise
    import sqlite3
    with pytest.raises(Exception):
        s.get("q1")
