from src.query_registry.models import Query


def test_render_substitutes_params():
    query = Query(
        id="q1",
        sql="SELECT * FROM $table WHERE id = $id",
        params={"table": "users", "id": "42"},
    )
    assert query.render() == "SELECT * FROM users WHERE id = 42"


def test_render_leaves_missing_params():
    query = Query(id="q1", sql="SELECT $missing", params={})
    # safe_substitute keeps unknown placeholders untouched
    assert query.render() == "SELECT $missing"


def test_render_partial_substitution():
    query = Query(
        id="q1",
        sql="SELECT $col FROM $table",
        params={"col": "name"},
    )
    assert query.render() == "SELECT name FROM $table"


def test_to_dict_roundtrip():
    query = Query(id="q1", sql="SELECT 1", groups=["a"], params={"x": "1"})
    assert Query.from_dict(query.to_dict()) == query


def test_to_dict_contains_all_fields():
    query = Query(id="q1", sql="SELECT 1", groups=["g1"], params={"k": "v"})
    d = query.to_dict()
    assert d["id"] == "q1"
    assert d["sql"] == "SELECT 1"
    assert d["groups"] == ["g1"]
    assert d["params"] == {"k": "v"}


def test_from_dict_optional_fields_default():
    query = Query.from_dict({"id": "q1", "sql": "SELECT 1"})
    assert query.groups == []
    assert query.params == {}


def test_default_groups_and_params():
    query = Query(id="q1", sql="SELECT 1")
    assert query.groups == []
    assert query.params == {}


def test_render_no_placeholders():
    query = Query(id="q1", sql="SELECT 1", params={"unused": "val"})
    assert query.render() == "SELECT 1"
