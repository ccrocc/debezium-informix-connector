/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import static laoflch.debezium.connector.informix.InformixConnectorConfig.SNAPSHOT_MODE;
import static laoflch.debezium.connector.informix.InformixConnectorConfig.TABLE_INCLUDE_LIST;
import static laoflch.debezium.connector.informix.InformixConnectorConfig.SnapshotMode.INITIAL_SCHEMA_ONLY;
import static laoflch.debezium.connector.informix.util.TestHelper.TEST_DATABASE;
import static org.fest.assertions.Assertions.assertThat;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.config.Configuration;
import io.debezium.data.VerifyRecord;
import io.debezium.embedded.AbstractConnectorTest;

import laoflch.debezium.connector.informix.util.TestHelper;

public class InformixCdcAttachIT extends AbstractConnectorTest {

    private InformixConnection connection;

    private static final String testTableName = "test_attach";
    private static final Map<String, String> testTableColumns = new LinkedHashMap<String, String>() {
        {
            put("id", "int");
            put("a", "varchar");
            put("b", "varchar");
        }
    };
    private static final List<String> testTableColumnsStringList = new LinkedList<String>() {
        {
            testTableColumns.forEach((column, type) -> add(column + " " + type));
        }
    };
    private static final String[][] testTableInitValues = { { "1", "A", "a" }, { "2", "B", "b" } };

    private static final String HIS_SUFFIX = "_his";
    private static final String TMP_SUFFIX = "_tmp";

    private static final Logger LOGGER = LoggerFactory.getLogger(InformixCdcAttachIT.class);

    @Before
    public void before() throws SQLException {
        connection = TestHelper.testConnection();

        // Step 0. create archived(historical) table if not exists
        connection.execute(String.format(
                "create table if not exists %s(%s) fragment by expression (id < 20) in rootdbs1,(id >=20) in rootdbs2",
                testTableName + HIS_SUFFIX, String.join(", ", testTableColumnsStringList)));
        // Step 0. unlock table & drop it
        connection.execute(String.format(
                "execute function syscdcv1:informix.cdc_set_fullrowlogging('%s:informix.%s',0);",
                TEST_DATABASE, testTableName));
        connection.execute(String.format("drop table if exists %s", testTableName));
        // Step 0. unlock tmp table & drop it
        connection.execute(String.format(
                "execute function syscdcv1:informix.cdc_set_fullrowlogging('%s:informix.%s',0);",
                TEST_DATABASE, testTableName + TMP_SUFFIX));
        connection.execute(String.format("drop table if exists %s", testTableName + TMP_SUFFIX));

        // Step 0. prepare test attach table
        connection.execute(String.format("create table if not exists %s(%s)",
                testTableName, String.join(", ", testTableColumnsStringList)));
        prepareTestTableData(testTableName, testTableInitValues);

        initializeConnectorTestFramework();
        Files.delete(TestHelper.DB_HISTORY_PATH);
        Print.enable();
    }

    @After
    public void after() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    // prepare some data for test table
    private void prepareTestTableData(String testTableName, String[][] tupleList) throws SQLException {
        connection.execute(String.format("truncate table %s", testTableName));

        for (String[] tuple : tupleList) {
            connection.execute(String.format(
                    "insert into %s values(\"%s\")", testTableName, String.join("\", \"", tuple)));
        }
    }

    @Test
    public void testAttachTableThenRenameTmp() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .with(TABLE_INCLUDE_LIST, String.format("informix.systables, informix.%s", testTableName))
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        // Step 1. Create Temp Table
        connection.execute(String.format("create table if not exists %s(%s)", testTableName + TMP_SUFFIX, String.join(", ", testTableColumnsStringList)));

        // unlock tables
        connection.execute(String.format("execute function syscdcv1:informix.cdc_set_fullrowlogging('%s:informix.%s',0)", TEST_DATABASE, testTableName));
        connection.execute(String.format("execute function syscdcv1:informix.cdc_set_fullrowlogging('%s:informix.%s',0)", TEST_DATABASE, testTableName + HIS_SUFFIX));

        // Step 2. Attach Table to Archived Historical Table
        String hisTablePartition = "p_" + System.currentTimeMillis();
        connection.execute(String.format(
                "alter fragment on table %s attach %s as partition %s id < 20",
                testTableName + HIS_SUFFIX, testTableName, hisTablePartition));

        // Step 3. Rename Temp Table to New Table
        connection.execute(String.format("rename table %s to %s", testTableName + TMP_SUFFIX, testTableName));

        // Insert a record to the new table. Then check whether the cdc engine can capture it.
        Map<String, String> recordToBeInsert = new LinkedHashMap<String, String>() {
            {
                put("id", "0");
                put("a", "x");
                put("b", "y");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName,
                String.join(", ", recordToBeInsert.keySet()),
                String.join("\", \"", recordToBeInsert.values())));

        Thread.sleep(30_000);

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);

        // consume enough records (cause there lots of event dispatched to kafka while we do attch on test table)
        // then check the last one, to determine whether the cdc engine works fine after reload.
        SourceRecords sourceRecords = consumeRecordsByTopic(100, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(topicName);

        final SourceRecord testInsertedRecord = records.get(records.size() - 1);
        final Struct testInsertedValue = (Struct) testInsertedRecord.value();

        VerifyRecord.isValidInsert(testInsertedRecord);
        assertRecord((Struct) testInsertedValue.get("after"), recordToBeInsert);

        stopConnector();
    }

    @Test
    public void testAttachTableThenCreateNew() throws Exception {

        final Configuration config = TestHelper.defaultConfig()
                .with(SNAPSHOT_MODE, INITIAL_SCHEMA_ONLY)
                .with(TABLE_INCLUDE_LIST, String.format("informix.systables, informix.%s", testTableName))
                .build();

        start(InformixConnector.class, config);

        /*
         * Wait InformixStreamingChangeEventSource.execute() is running.
         */
        Thread.sleep(60_000);

        // unlock tables
        connection.execute(String.format("execute function syscdcv1:informix.cdc_set_fullrowlogging('%s:informix.%s',0)", TEST_DATABASE, testTableName));
        connection.execute(String.format("execute function syscdcv1:informix.cdc_set_fullrowlogging('%s:informix.%s',0)", TEST_DATABASE, testTableName + HIS_SUFFIX));

        // Step 1. Attach table to archived historical table
        String hisTablePartition = "p_" + System.currentTimeMillis();
        connection.execute(String.format(
                "alter fragment on table %s attach %s as partition %s id < 20",
                testTableName + HIS_SUFFIX, testTableName, hisTablePartition));

        // Step 2. Create new table which has the same schema with test table
        connection.execute(String.format("create table if not exists %s(%s)", testTableName, String.join(", ", testTableColumnsStringList)));

        // Step 3: After Attach And Create New Table: test insert to the new table

        Map<String, String> recordToBeInsert = new LinkedHashMap<String, String>() {
            {
                put("id", "0");
                put("a", "x");
                put("b", "y");
            }
        };
        connection.execute(String.format("insert into %s(%s) values(\"%s\")", testTableName,
                String.join(", ", recordToBeInsert.keySet()),
                String.join("\", \"", recordToBeInsert.values())));

        Thread.sleep(30_000);

        String topicName = String.format("%s.informix.%s", TEST_DATABASE, testTableName);

        SourceRecords sourceRecords = consumeRecordsByTopic(100, false);
        List<SourceRecord> records = sourceRecords.recordsForTopic(topicName);

        final SourceRecord testInsertedRecord = records.get(records.size() - 1);
        final Struct testInsertedValue = (Struct) testInsertedRecord.value();

        VerifyRecord.isValidInsert(testInsertedRecord);
        assertRecord((Struct) testInsertedValue.get("after"), recordToBeInsert);

        stopConnector();
    }

    public static void assertRecord(Struct record, Map<String, String> recordToBeCheck) {
        recordToBeCheck.keySet().forEach(field -> assertThat(record.get(field).toString().trim()).isEqualTo(recordToBeCheck.get(field)));
    }

}
