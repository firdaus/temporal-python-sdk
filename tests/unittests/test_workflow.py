from datetime import timedelta

from temporal.api.enums.v1 import WorkflowIdReusePolicy
from temporal.converter import DEFAULT_DATA_CONVERTER_INSTANCE
from temporal.workflow import create_memo, create_search_attributes, WorkflowMethod, WorkflowClient, WorkflowOptions, \
    create_start_workflow_request


def test_create_memo():
    memo = create_memo({
        "name": "bob",
        "age": 20
    }, DEFAULT_DATA_CONVERTER_INSTANCE)
    assert "name" in memo.fields
    assert "age" in memo.fields
    assert memo.fields["name"].data == b'\"bob\"'
    assert memo.fields["age"].data == b'20'


def test_create_search_attributes():
    search_attributes = create_search_attributes({
        "name": "bob",
        "age": 20
    }, DEFAULT_DATA_CONVERTER_INSTANCE)
    assert "name" in search_attributes.indexed_fields
    assert "age" in search_attributes.indexed_fields
    assert search_attributes.indexed_fields["name"].data == b'\"bob\"'
    assert search_attributes.indexed_fields["age"].data == b'20'


def test_create_start_workflow_request_override_workflow_options():
    client = WorkflowClient(None, "the-namespace", None, DEFAULT_DATA_CONVERTER_INSTANCE)
    wm = WorkflowMethod()
    options = WorkflowOptions(workflow_id="workflow-id",
                              workflow_id_reuse_policy=WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
                              workflow_run_timeout=timedelta(seconds=200),
                              workflow_execution_timeout=timedelta(seconds=100),
                              workflow_task_timeout=timedelta(seconds=60),
                              task_queue="task-queue",
                              cron_schedule="cron-schedule",
                              memo={"name": "bob"}, search_attributes={"name": "alex"})
    request = create_start_workflow_request(client, wm, [], options)
    assert request.workflow_id == "workflow-id"
    assert request.workflow_id_reuse_policy == WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE
    assert request.workflow_run_timeout == timedelta(seconds=200)
    assert request.workflow_execution_timeout == timedelta(seconds=100)
    assert request.workflow_task_timeout == timedelta(seconds=60)
    assert request.task_queue == "task-queue"
    assert request.cron_schedule == "cron-schedule"
    assert request.memo.fields["name"].data == b'"bob"'
    assert request.search_attributes.indexed_fields["name"].data == b'"alex"'
