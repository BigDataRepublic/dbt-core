from test.integration.base import DBTIntegrationTest, use_profile


class BaseDependencyTest(DBTIntegrationTest):
    @property
    def schema(self):
        return 'schema_management_077'

    @property
    def models(self):
        return 'models'

    def base_schema(self):
        return self.unique_schema()

    def configured_schema(self):
        return self.unique_schema() + '_configured'

    def setUp(self):
        super().setUp()
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.base_schema())
        )
        self._created_schemas.add(
            self._get_schema_fqn(self.default_database, self.configured_schema())
        )


class TestSimpleDependency(BaseDependencyTest):
    @use_profile('postgres')
    def test_postgres_local_dependency(self):
        results = self.run_dbt()
        self.assertEqual(len(results), 1)
        self.assertTableDoesExist("my_frist_dbt_model")
