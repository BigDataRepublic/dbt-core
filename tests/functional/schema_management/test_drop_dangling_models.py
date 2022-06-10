import pytest
import os

from dbt.tests.util import run_dbt, check_table_does_exist, check_table_does_not_exist, write_file

model = """
{{
  config(
    materialized = "table"
  )
}}

with source_data as (

        select 1 as id
union all
select null as id

)

select *
from source_data
"""


@pytest.fixture(scope="class")
def models():
    return {
        "users.sql": model,
    }


@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        "type": "postgres",
        "threads": 4,
        "host": "localhost",
        "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
        "user": os.getenv("POSTGRES_TEST_USER", "root"),
        "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
        "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
        "manage_schemas": True,
    }


class TestDanglingModels:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "managed_schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": "dbt",
                    "action": "drop",
                }
            ]
        }

    def test_drop(
        self,
        project,
    ):
        # create users model
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "users")
        check_table_does_not_exist(project.adapter, "baz")

        # remove users model
        project.update_models({})
        run_dbt(["run"])
        check_table_does_not_exist(project.adapter, "users")
