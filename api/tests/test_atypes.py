from fastapi.testclient import TestClient
from api.app.main import app

client = TestClient(app)


def test_round_trip():
    # list should be empty
    assert client.get("/v1/atypes").json() == []
    body = {
        "mode": "field_spec",
        "name": "Foo",
        "fields": [
            {"name": "x", "type_label": "int"},
            {"name": "y", "type_label": "str", "optional": False},
        ],
    }
    res = client.post("/v1/atypes", json=body)
    assert res.status_code == 200
    assert res.json()["name"] == "Foo"
    payload = {"x": 1, "y": "ok"}
    assert client.post("/v1/atypes/Foo/validate", json=payload).json()["valid"]
    assert client.delete("/v1/atypes/Foo").status_code == 204
