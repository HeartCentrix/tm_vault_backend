"""GET /teams/{id}/channels rejects $top: live 2026-11-06 'Query option 'Top' is not allowed' (400),
which failed every TEAMS_CHANNEL backup. The list call must send no $top and still follow nextLink."""
import asyncio
from shared.graph_client import GraphClient


def test_team_channels_listing_sends_no_top():
    calls = []
    async def fake_get(url, params=None, **_):
        calls.append((url, params))
        if len(calls) == 1:
            return {"value": [{"id": "a"}], "@odata.nextLink": "https://graph.microsoft.com/v1.0/teams/t/channels?$skiptoken=2"}
        return {"value": [{"id": "b"}]}
    client = GraphClient.__new__(GraphClient)
    client._get = fake_get
    result = asyncio.run(GraphClient.get_teams_channels(client, "t"))
    assert [c["id"] for c in result["value"]] == ["a", "b"]
    assert all("$top" not in (params or {}) and "$top" not in url for url, params in calls)
