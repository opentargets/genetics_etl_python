"""Check for airflow import errors. Inspiration from https://garystafford.medium.com/devops-for-dataops-building-a-ci-cd-pipeline-for-apache-airflow-dags-975e4a622f83."""
from __future__ import annotations

import pytest
from airflow.models import DagBag


@pytest.fixture(params=["./src/airflow/dags"])
def dag_bag(request, monkeypatch: pytest.MonkeyPatch):
    """Return a DAG bag for testing."""
    # It's important to change directory before importing DAGs, because this is way Airflow works in production: the
    # dags/ directory serves as a root (and indeed $PYTHONPATH) for all DAGS contained therein.  Using Monkeypatch
    # ensures that the other tests are not affected.
    monkeypatch.chdir(request.param)
    return DagBag(dag_folder=request.param, include_examples=False)


def test_no_import_errors(dag_bag):
    """Test for import errors."""
    assert (
        not dag_bag.import_errors
    ), f"DAG import failures. Errors: {dag_bag.import_errors}"


def test_requires_tags(dag_bag):
    """Tags should be defined for each DAG."""
    for _, dag in dag_bag.dags.items():
        assert dag.tags


def test_owner_len_greater_than_five(dag_bag):
    """Owner should be defined for each DAG and be longer than 5 characters."""
    for _, dag in dag_bag.dags.items():
        assert len(dag.owner) > 5


def test_desc_len_greater_than_fifteen(dag_bag):
    """Description should be defined for each DAG and be longer than 30 characters."""
    for _, dag in dag_bag.dags.items():
        assert len(dag.description) > 30


def test_owner_not_airflow(dag_bag):
    """Owner should not be 'airflow'."""
    for _, dag in dag_bag.dags.items():
        assert str.lower(dag.owner) != "airflow"


def test_three_or_less_retries(dag_bag):
    """Retries should be 3 or less."""
    for _, dag in dag_bag.dags.items():
        assert dag.default_args["retries"] <= 3
