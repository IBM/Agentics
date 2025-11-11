from fastapi.testclient import TestClient
from api.app.main import app

client = TestClient(app)


def setup_answer_type():
    body = {
        "mode": "field_spec",
        "name": "Answer",
        "fields": [{"name": "answer", "type_label": "str"}],
    }
    client.post("/v1/atypes", json=body)


def new_session():
    return client.post("/v1/session").json()["session_id"]


def test_amap_areduce_flow():
    setup_answer_type()
    sid = new_session()
    # create agent
    client.post("/v1/agents", headers={"X-Session": sid}, json={"atype_name": "Answer"})
    # append two states
    client.post(
        f"/v1/agents/{sid}/states", json={"states": [{"answer": "A"}, {"answer": "B"}]}
    )
    # amap identity
    r = client.post(f"/v1/agents/{sid}/amap", json={"function_name": "identity"})
    assert len(r.json()["states"]) == 2
    # areduce
    r = client.post(
        f"/v1/agents/{sid}/areduce", json={"function_name": "concat_reduce"}
    )
    assert len(r.json()["states"]) == 1
