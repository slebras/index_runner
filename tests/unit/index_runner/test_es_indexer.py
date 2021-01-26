"""
Test functions found in src/index_runner/es_indexer.py
"""
import responses
from uuid import uuid4

from src.index_runner.es_indexer import set_user_perms
from src.utils.config import config


@responses.activate
def test_set_user_perms():
    """
    Mostly we are just asserting that the correct ES api call gets made ("expected_req")
    """
    wsid = 44869
    # Response for getPermissionsMass function from workspace
    mock_resp = {
        "version": "1.1",
        "result": [{"perms": [{"user1": "a", "user2": "r"}]}]
    }
    # Mock the workspace call
    responses.add(responses.POST, config()['workspace_url'], json=mock_resp)
    # Mock the Elasticsearch update
    base_url = config()['elasticsearch_url']
    idx = config()['elasticsearch_index_prefix'] + ".*"
    es_url = f"{base_url}/{idx}/_update_by_query?conflicts=proceed&wait_for_completion=true&refresh=true"
    script = "ctx._source.shared_users=['user1', 'user2']"
    expected_req = {
        'query': {'term': {'access_group': wsid}},
        'script': {'inline': script, 'lang': 'painless'},
    }
    resp_body = str(uuid4())
    responses.add(
        responses.POST,
        url=es_url,
        match=[
            responses.json_params_matcher(expected_req),
        ],
        body=resp_body,
    )
    msg = {
        "wsid": wsid,
    }
    resp = set_user_perms(msg)
    assert resp.text == resp_body
