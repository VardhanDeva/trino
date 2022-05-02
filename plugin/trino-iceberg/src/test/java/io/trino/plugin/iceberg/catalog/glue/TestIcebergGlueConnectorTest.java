/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg.catalog.glue;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.iceberg.BaseIcebergConnectorTest;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.plugin.iceberg.SchemaInitializer;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.iceberg.IcebergFileFormat.PARQUET;
import static io.trino.plugin.iceberg.S3Util.deleteObjects;
import static io.trino.testing.sql.TestTable.randomTableSuffix;
import static io.trino.tpch.TpchTable.LINE_ITEM;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/*
 * TestIcebergGlueConnectorTest currently uses AWS Default Credential Provider Chain,
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 * on ways to set your AWS credentials which will be needed to run this test.
 */
public class TestIcebergGlueConnectorTest
        extends BaseIcebergConnectorTest
{
    private final String bucketName;
    private final String schemaName;

    @Parameters("s3.bucket")
    public TestIcebergGlueConnectorTest(String bucketName)
    {
        super(PARQUET);
        this.bucketName = requireNonNull(bucketName, "bucketName is null");
        this.schemaName = "test_iceberg_connector_" + randomTableSuffix();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setIcebergProperties(
                        ImmutableMap.of(
                                "iceberg.catalog.type", "glue",
                                "iceberg.file-format", PARQUET.name(),
                                "hive.metastore.glue.default-warehouse-dir", getBaseDirectory()))
                .setSchemaInitializer(
                        SchemaInitializer.builder()
                                .withClonedTpchTables(ImmutableList.<TpchTable<?>>builder()
                                        .addAll(REQUIRED_TPCH_TABLES)
                                        .add(LINE_ITEM)
                                        .build())
                                .withSchemaName(schemaName)
                                .build())
                .build();
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        computeActual("SHOW TABLES").getMaterializedRows()
                .forEach(table -> getQueryRunner().execute("DROP TABLE " + table.getField(0)));
        getQueryRunner().execute("DROP SCHEMA IF EXISTS " + schemaName);

        // DROP TABLES should clean up any files, but clear the directory manually to be safe
        deleteObjects(bucketName, getBaseDirectory());
    }

    @Test
    @Override
    public void testInformationSchemaFiltering()
    {
        // Add schema name to WHERE condition because finding a table from all schemas in Glue is too slow
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schemaName + "' AND table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'bigint' AND table_schema = '" + schemaName + "' AND table_name = 'customer' AND column_name = 'custkey' LIMIT 1",
                "SELECT 'customer' table_name");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        // Add schema name to WHERE condition and skip below query because finding a table from all schemas in Glue is too slow
        // SELECT DISTINCT table_name FROM information_schema.columns WHERE table_schema = 'information_schema' OR rand() = 42 ORDER BY 1

        String catalog = getSession().getCatalog().orElseThrow();
        String schema = getSession().getSchema().orElseThrow();
        String schemaPattern = schema.replaceAll(".$", "_");

        @Language("SQL") String ordersTableWithColumns = "VALUES " +
                "('orders', 'orderkey'), " +
                "('orders', 'custkey'), " +
                "('orders', 'orderstatus'), " +
                "('orders', 'totalprice'), " +
                "('orders', 'orderdate'), " +
                "('orders', 'orderpriority'), " +
                "('orders', 'clerk'), " +
                "('orders', 'shippriority'), " +
                "('orders', 'comment')";

        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
        assertQuery("SELECT table_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders' GROUP BY table_name", "VALUES 'orders'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name = 'orders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '%rders'", ordersTableWithColumns);
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_schema LIKE '" + schemaPattern + "' AND table_name LIKE '_rder_'", ordersTableWithColumns);
        assertThat(query(
                "SELECT table_name, column_name FROM information_schema.columns " +
                        "WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%orders%'"))
                .skippingTypesCheck()
                .containsAll(ordersTableWithColumns);

        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_schema = '" + schema + "'");
        assertQuery("SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "'");
        assertQuery("SELECT table_name, column_name FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '_rders'", ordersTableWithColumns);
        assertQuerySucceeds("SELECT * FROM information_schema.columns WHERE table_catalog = '" + catalog + "' AND table_schema = '" + schema + "' AND table_name LIKE '%'");
        assertQuery("SELECT column_name FROM information_schema.columns WHERE table_catalog = 'something_else' AND table_schema = '" + schema + "'", "SELECT '' WHERE false");
    }

    @Test
    @Override
    public void testShowCreateSchema()
    {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat(computeActual("SHOW CREATE SCHEMA " + schemaName).getOnlyValue().toString())
                .matches("CREATE SCHEMA iceberg." + schemaName);
    }

    @Test
    @Override
    public void testDropNonEmptySchemaWithMaterializedView()
    {
        assertThatThrownBy(super::testDropNonEmptySchemaWithMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Test(dataProvider = "testColumnNameDataProvider")
    @Override
    public void testMaterializedViewColumnName(String columnName)
    {
        assertThatThrownBy(() -> super.testMaterializedViewColumnName(columnName))
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Test
    @Override
    public void testReadMetadataWithRelationsConcurrentModifications()
    {
        assertThatThrownBy(super::testReadMetadataWithRelationsConcurrentModifications)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Test
    @Override
    public void testRenameMaterializedView()
    {
        assertThatThrownBy(super::testRenameMaterializedView)
                .hasStackTraceContaining("createMaterializedView is not supported for Iceberg Glue catalogs");
    }

    @Test
    @Override
    public void testRenameSchema()
    {
        assertThatThrownBy(super::testRenameSchema)
                .hasStackTraceContaining("renameNamespace is not supported for Iceberg Glue catalogs");
    }

    @Test(dataProvider = "repartitioningDataProvider")
    @Override
    public void testRepartitionDataOnCtas(Session session, String partitioning, int expectedFiles)
    {
        // TODO: Enable testRepartitionDataOnCtas
        throw new SkipException("Disable because the test causes OOM");
    }

    @Test(dataProvider = "repartitioningDataProvider")
    @Override
    public void testRepartitionDataOnInsert(Session session, String partitioning, int expectedFiles)
    {
        // TODO: Enable testRepartitionDataOnInsert
        throw new SkipException("Disable because the test causes OOM");
    }

    @Override
    protected boolean supportsIcebergFileStatistics(String typeName)
    {
        return !(typeName.equalsIgnoreCase("varbinary")) &&
                !(typeName.equalsIgnoreCase("uuid"));
    }

    @Override
    protected boolean supportsRowGroupStatistics(String typeName)
    {
        return !typeName.equalsIgnoreCase("varbinary");
    }

    @Override
    protected Session withSmallRowGroups(Session session)
    {
        return Session.builder(session)
                .setCatalogSessionProperty("iceberg", "orc_writer_max_stripe_rows", "10")
                .build();
    }

    @Override
    protected List<String> getAllDataFilesFromTableDirectory(String tableName)
    {
        ListObjectsV2Request request = new ListObjectsV2Request();
        request.withBucketName(bucketName);
        request.withPrefix(format("%s/%s.db/%s/data", schemaName, schemaName, tableName));

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
        return s3.listObjectsV2(request).getObjectSummaries().stream()
                .map(object -> format("s3://%s/%s", bucketName, object.getKey()))
                .filter(path -> !path.matches("\\..*\\.crc"))
                .collect(toImmutableList());
    }

    @Override
    protected String getSchemaLocation()
    {
        return format("%s/%s.db", getBaseDirectory(), schemaName);
    }

    @Override
    protected String getBaseDirectory()
    {
        return format("s3://%s/%s", bucketName, schemaName);
    }
}
