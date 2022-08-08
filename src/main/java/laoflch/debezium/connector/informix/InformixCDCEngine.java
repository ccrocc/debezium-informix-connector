/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbcx.IfxDataSource;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.impl.IfxStreamException;
import com.informix.util.AdvancedUppercaseProperties;

import io.debezium.config.Configuration;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;

public class InformixCDCEngine {

    private static Logger LOGGER = LoggerFactory.getLogger(InformixCDCEngine.class);

    private static String URL_PATTERN = "jdbc:informix-sqli://%s:%s/syscdcv1";

    private String host;
    private String port;
    private String user;
    private String password;

    private static int bufferSize;
    private static int timeOut;
    private static int maxRecs;

    long lsn;
    boolean hasInit;

    private POIfxCDCEngine cdcEngine;

    private Map<Integer, TableId> tableIdByLabelId;

    public InformixCDCEngine(Configuration config) {
        cdcEngine = null;
        lsn = 0;
        hasInit = false;

        bufferSize = config.getInteger(InformixConnectorConfig.CDC_BUFFERSIZE);
        timeOut = config.getInteger(InformixConnectorConfig.CDC_TIMEOUT);
        maxRecs = config.getInteger(InformixConnectorConfig.CDC_MAXRECS);

        host = config.getString(JdbcConfiguration.HOSTNAME);
        port = config.getString(JdbcConfiguration.PORT);
        user = config.getString(JdbcConfiguration.USER);
        password = config.getString(JdbcConfiguration.PASSWORD);

        // TODO: try HPPC or FastUtils's Integer Map?
        tableIdByLabelId = new HashMap<>();
    }

    public void init(InformixDatabaseSchema schema) throws InterruptedException {
        try {
            String url = InformixCDCEngine.genURLStr(host, port);
            this.cdcEngine = this.buildCDCEngine(url, user, password, schema);

            this.cdcEngine.init();
            this.hasInit = true;
        }
        catch (SQLException ex) {
            LOGGER.error("Caught SQLException", ex);
            throw new InterruptedException("Failed while while initialize CDC Engine");
        }
        catch (IfxStreamException ex) {
            LOGGER.error("Caught IfxStreamException", ex);
            throw new InterruptedException("Failed while while initialize CDC Engine");
        }
    }

    public POIfxCDCEngine buildCDCEngine(String url, String user, String password, InformixDatabaseSchema schema) throws SQLException {
        IfxDataSource ds = new IfxDataSource(url);
        ds.setUser(user);
        ds.setPassword(password);
        Properties dsMasked = propsWithMaskedPassword(ds.getDsProperties());
        LOGGER.info("Connecting to Informix CDC: {}", dsMasked);

        POIfxCDCEngine.Builder builder = new POIfxCDCEngine.Builder(ds);

        // TODO: Make an parameter 'buffer size' for better performance. Default value is 10240
        builder.buffer(this.bufferSize);
        builder.maxRecs(this.maxRecs);
        builder.timeout(this.timeOut);
        LOGGER.info("Set Informix CDC Builder Parameters: maxRecs=[{}] bufferSize=[{}] timeOut=[{}]", this.maxRecs, this.bufferSize, this.timeOut);

        schema.tableIds().forEach((TableId tid) -> {
            String tname = tid.catalog() + ":" + tid.schema() + "." + tid.table();
            String[] colNames;
            if (tid.table().equals("systables")) {
                colNames = new String[]{ "tabname", "owner", "tabid" };
            }
            else if (tid.table().equals("syscolumns")) {
                colNames = new String[]{ "colname", "tabid", "colno", "coltype", "collength" };
            }
            else {
                colNames = schema.tableFor(tid).columns().stream()
                        .map(Column::name).toArray(String[]::new);
            }
            builder.watchTable(tname, colNames);
        });

        if (this.lsn > 0) {
            builder.sequenceId(this.lsn);
        }
        long seqId = builder.getSequenceId();
        LOGGER.info("Set CDCEngine's LSN to '{}' aka {}", seqId, Lsn.valueOf(seqId).toLongString());

        /*
         * Build Map of Label_id to TableId.
         */
        tableIdByLabelId.clear();
        for (POIfxCDCEngine.IfmxWatchedTable tbl : builder.getWatchedTables()) {
            TableId tid = new TableId(tbl.getDatabaseName(), tbl.getNamespace(), tbl.getTableName());
            tableIdByLabelId.put(tbl.getLabel(), tid);
            LOGGER.info("Added WatchedTable : label={} -> tableId={}", tbl.getLabel(), tid);
        }

        return builder.build();
    }

    public void reloadCDCEngine(InformixDatabaseSchema schema, Long fromLsn) throws InterruptedException {
        this.close();
        this.setStartLsn(fromLsn);
        this.init(schema);
    }

    public void close() {
        try {
            this.cdcEngine.close();
        }
        catch (IfxStreamException e) {
            LOGGER.error("Caught a exception while closing cdcEngine", e);
        }
    }

    public void setStartLsn(Long fromLsn) {
        this.lsn = fromLsn;
    }

    public Map<Integer, TableId> convertLabel2TableId() {
        return this.tableIdByLabelId;
    }

    public void stream(StreamHandler streamHandler) throws InterruptedException, SQLException, IfxStreamException {
        while (streamHandler.accept(cdcEngine.getRecords())) {

        }
    }

    public POIfxCDCEngine getCdcEngine() {
        return cdcEngine;
    }

    public static String genURLStr(String host, String port) {
        return String.format(URL_PATTERN, host, port);
    }

    public static InformixCDCEngine build(Configuration config) {
        return new InformixCDCEngine(config);
    }

    private static Properties propsWithMaskedPassword(Properties props) {
        final Properties filtered = new Properties();
        filtered.putAll(props);
        String passwdKeyName = props instanceof AdvancedUppercaseProperties ? JdbcConfiguration.PASSWORD.name().toUpperCase() : JdbcConfiguration.PASSWORD.name();
        if (props.containsKey(passwdKeyName)) {
            filtered.put(passwdKeyName, "***");
        }
        return filtered;
    }

    public interface StreamHandler {

        boolean accept(Vector<IfmxStreamRecord> records) throws SQLException, IfxStreamException, InterruptedException;

    }
}
