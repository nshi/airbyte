# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
import json
from datetime import datetime, timedelta, timezone
from typing import List
from unittest import TestCase

import freezegun
from airbyte_cdk.models import FailureType, SyncMode
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import read

# from airbyte_cdk.connector_builder.models import HttpResponse
from airbyte_cdk.test.mock_http import HttpMocker, HttpRequestMatcher, HttpResponse
from airbyte_cdk.test.mock_http.request import HttpRequest
from airbyte_cdk.test.mock_http.response_builder import (
    FieldPath,
    HttpResponseBuilder,
    NestedPath,
    RecordBuilder,
    create_record_builder,
    create_response_builder,
    find_template,
)
from airbyte_cdk.test.state_builder import StateBuilder
from airbyte_protocol.models import AirbyteStreamStatus, Level

# from integration.pagination import StripePaginationStrategy
# from integration.request_builder import GithubBuilder
# from integration.response_builder import a_response_with_status
from source_github import SourceGithub

from .config import ConfigBuilder
from .pagination import GitHubPaginationStrategy
from .request_builder import GithubRequestBuilder

_TOKEN = "GITHUB_TEST_TOKEN"

_CONFIG = ConfigBuilder().with_repositories(["airbytehq/integration-test"]).build()


def _create_catalog(sync_mode: SyncMode = SyncMode.full_refresh):
    return CatalogBuilder().with_stream(name="events", sync_mode=sync_mode).build()


def _create_event_request() -> GithubRequestBuilder:
    return GithubRequestBuilder.events_endpoint(repo="airbytehq/integration_test", token=_TOKEN)


def _create_response() -> HttpResponseBuilder:
    return create_response_builder(
        response_template=find_template("events", __file__), records_path=FieldPath(), pagination_strategy=GitHubPaginationStrategy()
    )


def _create_record(resource: str) -> RecordBuilder:
    return create_record_builder(
        find_template(resource, __file__), FieldPath(), record_id_path=FieldPath("id"), record_cursor_path=FieldPath("created_at")
    )


class EventsTest(TestCase):
    rate_limit_request = HttpRequest(
        url="https://api.github.com/rate_limit",
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Authorization": "token GITHUB_TEST_TOKEN",
        },
    )
    rate_limit_response = HttpResponse(
        json.dumps(
            {
                "resources": {
                    "core": {"limit": 5000, "used": 0, "remaining": 5000, "reset": 5070908800},
                    "graphql": {"limit": 5000, "used": 0, "remaining": 5000, "reset": 5070908800},
                }
            }
        )
    )

    @HttpMocker()
    def test_full_refresh_no_pagination(self, http_mocker):
        """Ensure http integration and record extraction"""
        http_mocker.get(self.rate_limit_request, self.rate_limit_response)
        http_mocker.get(
            HttpRequest(url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}", query_params={"per_page": 100}, headers={}),
            HttpResponse(json.dumps({"full_name": "airbytehq/integration-test", "default_branch": "master"})),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/branches", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(json.dumps([{"repository": "airbytehq/integration-test", "name": "master"}])),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/events", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(json.dumps(find_template("events", __file__))),
        )

        source = SourceGithub()
        actual_messages = read(source, config=_CONFIG, catalog=_create_catalog())

        assert len(actual_messages.records) == 2

    @HttpMocker()
    def test_full_refresh_with_pagination(self, http_mocker):
        """Ensure pagination"""
        http_mocker.get(self.rate_limit_request, self.rate_limit_response)
        http_mocker.get(
            HttpRequest(url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}", query_params={"per_page": 100}, headers={}),
            HttpResponse(json.dumps({"full_name": "airbytehq/integration-test", "default_branch": "master"})),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/branches", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(json.dumps([{"repository": "airbytehq/integration-test", "name": "master"}])),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/events", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(
                json.dumps(find_template("events", __file__)),
                headers={"Link": '<https://api.github.com/repos/{}/events?page=2>; rel="next"'.format(_CONFIG.get("repositories")[0])},
            ),
        )
        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/events",
                query_params={"per_page": 100, "page": 2},
                headers={},
            ),
            HttpResponse(json.dumps(find_template("events", __file__))),
        )

        source = SourceGithub()
        actual_messages = read(source, config=_CONFIG, catalog=_create_catalog())

        assert len(actual_messages.records) == 4

    @HttpMocker()
    def test_incremental_read(self, http_mocker):
        """Ensure incremental sync.
         Stream `Events` is semi-incremental, so all request  will be performed and only new records will be extracted"""

        http_mocker.get(self.rate_limit_request, self.rate_limit_response)
        http_mocker.get(
            HttpRequest(url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}", query_params={"per_page": 100}, headers={}),
            HttpResponse(json.dumps({"full_name": "airbytehq/integration-test", "default_branch": "master"})),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/branches", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(json.dumps([{"repository": "airbytehq/integration-test", "name": "master"}])),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/events", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(json.dumps(find_template("events", __file__))),
        )

        source = SourceGithub()
        actual_messages = read(source, config=_CONFIG, catalog=_create_catalog(sync_mode=SyncMode.incremental),
                               state=StateBuilder().with_stream_state("events", {"airbytehq/integration-test": {"created_at": "2022-06-09T10:00:00Z"}}).build(),
                               )
        assert len(actual_messages.records) == 1

    @HttpMocker()
    def test_read_with_error(self, http_mocker):
        """Ensure read() method does not raises an error"""

        http_mocker.get(self.rate_limit_request, self.rate_limit_response)
        http_mocker.get(
            HttpRequest(url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}", query_params={"per_page": 100}, headers={}),
            HttpResponse(json.dumps({"full_name": "airbytehq/integration-test", "default_branch": "master"})),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/branches", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(json.dumps([{"repository": "airbytehq/integration-test", "name": "master"}])),
        )

        http_mocker.get(
            HttpRequest(
                url=f"https://api.github.com/repos/{_CONFIG.get('repositories')[0]}/events", query_params={"per_page": 100}, headers={}
            ),
            HttpResponse(body='{"message":"some_error_message"}', status_code=403),
        )

        source = SourceGithub()
        actual_messages = read(source, config=_CONFIG, catalog=_create_catalog())

        assert len(actual_messages.records) == 0
