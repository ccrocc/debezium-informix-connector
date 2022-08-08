/*
 * Copyright Debezium-Informix-Connector Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package laoflch.debezium.connector.informix;

import java.nio.ByteBuffer;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.informix.jdbc.IfmxTableDescriptor;
import com.informix.jdbc.IfxSmartBlob;
import com.informix.stream.api.IfmxStreamEngine;
import com.informix.stream.api.IfmxStreamRecord;
import com.informix.stream.cdc.IfxCDCRecordBuilder;
import com.informix.stream.impl.IfxStreamException;

public class POIfxCDCEngine implements IfmxStreamEngine {
    private final Logger logger;
    private final Builder builder;
    private IfxSmartBlob smartBlob;
    private final Connection con;
    private int sessionID;
    private final int bufferSize;
    private final byte[] buffer;
    private final IfxCDCRecordBuilder recordBuilder;
    private final int timeout;
    private final int maxRecs;
    private final List<POIfxCDCEngine.IfmxWatchedTable> capturedTables;
    private final boolean stopLoggingOnClose;
    private boolean inlineLOB;
    private boolean isClosed;
    private final long startingSequencePosition;

    public static POIfxCDCEngine.Builder builder(DataSource ds) {
        return new POIfxCDCEngine.Builder(ds);
    }

    private POIfxCDCEngine(Builder builder) throws SQLException {
        this.logger = LoggerFactory.getLogger(POIfxCDCEngine.class);
        this.capturedTables = new ArrayList();
        this.inlineLOB = false;
        this.isClosed = false;
        this.builder = builder;
        this.con = builder.ds.getConnection();
        this.recordBuilder = new IfxCDCRecordBuilder(builder.ds.getConnection());
        this.timeout = builder.getTimeout();
        this.maxRecs = builder.getMaxRecs();
        this.bufferSize = builder.getBufferSize();
        this.buffer = new byte[this.bufferSize];
        this.startingSequencePosition = builder.getSequenceId();
        this.capturedTables.addAll(builder.getWatchedTables());
        this.stopLoggingOnClose = builder.stopLoggingOnClose;
    }

    public void init() throws SQLException, IfxStreamException {
        Statement s = this.con.createStatement();
        Throwable var3 = null;

        String serverName;
        ResultSet rs;
        Throwable var5;
        try {
            rs = s.executeQuery("SELECT env_value FROM sysmaster:sysenv where env_name = 'INFORMIXSERVER'");
            var5 = null;

            try {
                rs.next();
                serverName = rs.getString(1).trim();
            }
            catch (Throwable var77) {
                var5 = var77;
                throw var77;
            }
            finally {
                if (rs != null) {
                    if (var5 != null) {
                        try {
                            rs.close();
                        }
                        catch (Throwable var74) {
                            var5.addSuppressed(var74);
                        }
                    }
                    else {
                        rs.close();
                    }
                }

            }
        }
        catch (Throwable var79) {
            var3 = var79;
            throw var79;
        }
        finally {
            if (s != null) {
                if (var3 != null) {
                    try {
                        s.close();
                    }
                    catch (Throwable var73) {
                        var3.addSuppressed(var73);
                    }
                }
                else {
                    s.close();
                }
            }

        }

        this.logger.debug("Server name detected: {}", serverName);
        CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_opensess(?,?,?,?,?,?)");
        var3 = null;

        try {
            cstmt.setString(1, serverName);
            cstmt.setInt(2, 0);
            cstmt.setInt(3, this.timeout);
            cstmt.setInt(4, this.maxRecs);
            cstmt.setInt(5, 1);
            cstmt.setInt(6, 1);
            rs = cstmt.executeQuery();
            var5 = null;

            try {
                rs.next();
                this.sessionID = rs.getInt(1);
                if (this.sessionID < 0) {
                    throw new IfxStreamException("Unable to create CDC session. Error code: " + this.sessionID);
                }
            }
            catch (Throwable var81) {
                var5 = var81;
                throw var81;
            }
            finally {
                if (rs != null) {
                    if (var5 != null) {
                        try {
                            rs.close();
                        }
                        catch (Throwable var76) {
                            var5.addSuppressed(var76);
                        }
                    }
                    else {
                        rs.close();
                    }
                }

            }
        }
        catch (Throwable var83) {
            var3 = var83;
            throw var83;
        }
        finally {
            if (cstmt != null) {
                if (var3 != null) {
                    try {
                        cstmt.close();
                    }
                    catch (Throwable var75) {
                        var3.addSuppressed(var75);
                    }
                }
                else {
                    cstmt.close();
                }
            }

        }

        this.smartBlob = new IfxSmartBlob(this.con);
        Iterator var86 = this.capturedTables.iterator();

        while (var86.hasNext()) {
            POIfxCDCEngine.IfmxWatchedTable table = (POIfxCDCEngine.IfmxWatchedTable) var86.next();
            this.watchTable(table);
        }

        this.activateSession();
    }

    public Vector<IfmxStreamRecord> getRecords() throws SQLException, IfxStreamException {
        // TODO: catch error while buffer size not enough
        int length = this.smartBlob.IfxLoRead(this.sessionID, this.buffer, this.buffer.length);
        if (length > -1) {
            Vector<IfmxStreamRecord> records = new Vector<IfmxStreamRecord>();
            int index = 0;
            String recordSizeLogString = "";
            while (index < length) {
                byte[] b = Arrays.copyOfRange(this.buffer, index, length);
                IfmxStreamRecord record = this.recordBuilder.buildRecord(b);
                records.add(record);
                // Todo: handle with buffer not enough when get more records
                // header.getHeaderSize() + header.getPayloadSize() + CDCConstants.CDC_HEADER_SIZE;
                ByteBuffer bb = ByteBuffer.wrap(b);
                int headerSize = bb.getInt() - 16;
                int payloadSize = bb.getInt();
                int recordSize = headerSize + payloadSize + 16;

                recordSizeLogString = recordSizeLogString + recordSize + " | ";

                index += recordSize;
            }

            this.logger.info("Total Size [{}] with Records Size Pre Load => {}", length, recordSizeLogString);

            if (this.logger.isTraceEnabled()) {
                this.logger.trace("{}", records);
            }

            return records;
        }
        else {
            throw new IfxStreamException("IfxLoRead returned -1, no more data?");
        }
    }

    public IfmxStreamRecord getRecord() throws SQLException, IfxStreamException {
        int length = this.smartBlob.IfxLoRead(this.sessionID, this.buffer, this.buffer.length);
        if (length > -1) {
            IfmxStreamRecord r = this.recordBuilder.buildRecord(this.buffer);
            if (this.logger.isTraceEnabled()) {
                this.logger.trace("{}", r);
            }

            return r;
        }
        else {
            throw new IfxStreamException("IfxLoRead returned -1, no more data?");
        }
    }

    private void watchTable(POIfxCDCEngine.IfmxWatchedTable table) throws SQLException, IfxStreamException {
        this.logger.debug("Starting watch on table [{}]", table);
        this.setFullRowLogging(table.getDesciptorString(), true);
        this.startCapture(table);
    }

    private void setFullRowLogging(String tableName, boolean enable) throws IfxStreamException {
        this.logger.debug("Setting full row logging on [{}] to '{}'", tableName, enable);

        try {
            CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_set_fullrowlogging(?,?)");
            Throwable var4 = null;

            try {
                cstmt.setString(1, tableName);
                if (enable) {
                    cstmt.setInt(2, 1);
                }
                else {
                    cstmt.setInt(2, 0);
                }

                ResultSet rs = cstmt.executeQuery();
                Throwable var6 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to set full row logging. Error code: " + resultCode);
                    }
                }
                catch (Throwable var31) {
                    var6 = var31;
                    throw var31;
                }
                finally {
                    if (rs != null) {
                        if (var6 != null) {
                            try {
                                rs.close();
                            }
                            catch (Throwable var30) {
                                var6.addSuppressed(var30);
                            }
                        }
                        else {
                            rs.close();
                        }
                    }

                }
            }
            catch (Throwable var33) {
                var4 = var33;
                throw var33;
            }
            finally {
                if (cstmt != null) {
                    if (var4 != null) {
                        try {
                            cstmt.close();
                        }
                        catch (Throwable var29) {
                            var4.addSuppressed(var29);
                        }
                    }
                    else {
                        cstmt.close();
                    }
                }

            }

        }
        catch (SQLException var35) {
            throw new IfxStreamException("Unable to set full row logging ", var35);
        }
    }

    private void startCapture(POIfxCDCEngine.IfmxWatchedTable table) throws SQLException {
        Throwable var3;
        ResultSet rs;
        Throwable var5;
        if (table.getColumnDescriptorString().equals("*")) {
            this.logger.debug("Starting column lookup for [{}]", table.getDesciptorString());
            Statement s = this.con.createStatement(1003, 1007);
            var3 = null;

            try {
                rs = s.executeQuery("SELECT FIRST 1 * FROM " + table.getDesciptorString());
                var5 = null;

                try {
                    ResultSetMetaData md = rs.getMetaData();
                    String[] columns = new String[md.getColumnCount()];

                    for (int i = 1; i <= md.getColumnCount(); ++i) {
                        columns[i - 1] = md.getColumnName(i).trim();
                    }

                    this.logger.debug("Dynamically adding to table [{}] columns: {}", table.getDesciptorString(), columns);
                    table.columns(columns);
                }
                catch (Throwable var88) {
                    var5 = var88;
                    throw var88;
                }
                finally {
                    if (rs != null) {
                        if (var5 != null) {
                            try {
                                rs.close();
                            }
                            catch (Throwable var82) {
                                var5.addSuppressed(var82);
                            }
                        }
                        else {
                            rs.close();
                        }
                    }

                }
            }
            catch (Throwable var90) {
                var3 = var90;
                throw var90;
            }
            finally {
                if (s != null) {
                    if (var3 != null) {
                        try {
                            s.close();
                        }
                        catch (Throwable var81) {
                            var3.addSuppressed(var81);
                        }
                    }
                    else {
                        s.close();
                    }
                }

            }
        }

        this.logger.debug("Starting capture on [{}]", table);

        try {
            CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_startcapture(?,?,?,?,?)");
            var3 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                cstmt.setLong(2, 0L);
                cstmt.setString(3, table.getDesciptorString());
                cstmt.setString(4, table.getColumnDescriptorString());
                cstmt.setInt(5, table.getLabel());
                rs = cstmt.executeQuery();
                var5 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new SQLException("CDCConnection: Unable to start cdc capture. Error code: " + resultCode);
                    }
                }
                catch (Throwable var83) {
                    var5 = var83;
                    throw var83;
                }
                finally {
                    if (rs != null) {
                        if (var5 != null) {
                            try {
                                rs.close();
                            }
                            catch (Throwable var80) {
                                var5.addSuppressed(var80);
                            }
                        }
                        else {
                            rs.close();
                        }
                    }

                }
            }
            catch (Throwable var85) {
                var3 = var85;
                throw var85;
            }
            finally {
                if (cstmt != null) {
                    if (var3 != null) {
                        try {
                            cstmt.close();
                        }
                        catch (Throwable var79) {
                            var3.addSuppressed(var79);
                        }
                    }
                    else {
                        cstmt.close();
                    }
                }

            }

        }
        catch (SQLException var87) {
            throw new SQLException("CDCConnection: Unable to start cdc capture ", var87);
        }
    }

    private void unwatchTable(POIfxCDCEngine.IfmxWatchedTable table) throws IfxStreamException {
        this.endCapture(table);
        if (this.stopLoggingOnClose) {
            this.setFullRowLogging(table.getDesciptorString(), false);
        }

    }

    private void endCapture(POIfxCDCEngine.IfmxWatchedTable table) throws IfxStreamException {
        try {
            CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_endcapture(?,0,?)");
            Throwable var3 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                cstmt.setString(2, table.getDesciptorString());
                ResultSet rs = cstmt.executeQuery();
                Throwable var5 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to end cdc capture. Error code: " + resultCode);
                    }
                }
                catch (Throwable var30) {
                    var5 = var30;
                    throw var30;
                }
                finally {
                    if (rs != null) {
                        if (var5 != null) {
                            try {
                                rs.close();
                            }
                            catch (Throwable var29) {
                                var5.addSuppressed(var29);
                            }
                        }
                        else {
                            rs.close();
                        }
                    }

                }
            }
            catch (Throwable var32) {
                var3 = var32;
                throw var32;
            }
            finally {
                if (cstmt != null) {
                    if (var3 != null) {
                        try {
                            cstmt.close();
                        }
                        catch (Throwable var28) {
                            var3.addSuppressed(var28);
                        }
                    }
                    else {
                        cstmt.close();
                    }
                }

            }

        }
        catch (SQLException var34) {
            throw new IfxStreamException("Unable to end cdc capture ", var34);
        }
    }

    private void activateSession() throws IfxStreamException {
        this.logger.debug("Activating CDC session");

        try {
            CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_activatesess(?,?)");
            Throwable var2 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                cstmt.setLong(2, this.startingSequencePosition);
                ResultSet rs = cstmt.executeQuery();
                Throwable var4 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to activate session. Error code: " + resultCode);
                    }
                }
                catch (Throwable var29) {
                    var4 = var29;
                    throw var29;
                }
                finally {
                    if (rs != null) {
                        if (var4 != null) {
                            try {
                                rs.close();
                            }
                            catch (Throwable var28) {
                                var4.addSuppressed(var28);
                            }
                        }
                        else {
                            rs.close();
                        }
                    }

                }
            }
            catch (Throwable var31) {
                var2 = var31;
                throw var31;
            }
            finally {
                if (cstmt != null) {
                    if (var2 != null) {
                        try {
                            cstmt.close();
                        }
                        catch (Throwable var27) {
                            var2.addSuppressed(var27);
                        }
                    }
                    else {
                        cstmt.close();
                    }
                }

            }

        }
        catch (SQLException var33) {
            throw new IfxStreamException("Unable to activate session", var33);
        }
    }

    private void closeSession() throws IfxStreamException {
        this.logger.debug("Closing CDC session");

        try {
            CallableStatement cstmt = this.con.prepareCall("execute function informix.cdc_closesess(?)");
            Throwable var2 = null;

            try {
                cstmt.setInt(1, this.sessionID);
                ResultSet rs = cstmt.executeQuery();
                Throwable var4 = null;

                try {
                    rs.next();
                    int resultCode = rs.getInt(1);
                    if (resultCode != 0) {
                        throw new IfxStreamException("Unable to close session. Error code: " + resultCode);
                    }
                }
                catch (Throwable var29) {
                    var4 = var29;
                    throw var29;
                }
                finally {
                    if (rs != null) {
                        if (var4 != null) {
                            try {
                                rs.close();
                            }
                            catch (Throwable var28) {
                                var4.addSuppressed(var28);
                            }
                        }
                        else {
                            rs.close();
                        }
                    }

                }
            }
            catch (Throwable var31) {
                var2 = var31;
                throw var31;
            }
            finally {
                if (cstmt != null) {
                    if (var2 != null) {
                        try {
                            cstmt.close();
                        }
                        catch (Throwable var27) {
                            var2.addSuppressed(var27);
                        }
                    }
                    else {
                        cstmt.close();
                    }
                }

            }

        }
        catch (SQLException var33) {
            throw new IfxStreamException("Unable to close session", var33);
        }
    }

    public void close() throws IfxStreamException {
        if (!this.isClosed) {
            this.logger.debug("Closing down CDC engine");
            IfxStreamException e = null;
            boolean var15 = false;

            label211: {
                IfxStreamException se;
                label212: {
                    try {
                        var15 = true;
                        Iterator var2 = this.capturedTables.iterator();

                        while (var2.hasNext()) {
                            POIfxCDCEngine.IfmxWatchedTable capturedTable = (POIfxCDCEngine.IfmxWatchedTable) var2.next();
                            this.unwatchTable(capturedTable);
                        }

                        this.closeSession();
                        var15 = false;
                        break label212;
                    }
                    catch (IfxStreamException var22) {
                        e = var22;
                        var15 = false;
                    }
                    finally {
                        if (var15) {
                            this.isClosed = true;

                            // IfxStreamException se;
                            try {
                                this.con.close();
                            }
                            catch (SQLException var17) {
                                se = new IfxStreamException("Could not close main connection", var17);
                                if (e == null) {
                                    e = se;
                                }
                                else {
                                    e.addSuppressed(se);
                                }
                            }

                            try {
                                this.recordBuilder.close();
                            }
                            catch (SQLException var16) {
                                se = new IfxStreamException("Could not close record builder", var16);
                                if (e != null) {
                                    e.addSuppressed(se);
                                }
                            }

                        }
                    }

                    this.isClosed = true;

                    try {
                        this.con.close();
                    }
                    catch (SQLException var19) {
                        se = new IfxStreamException("Could not close main connection", var19);
                        if (e == null) {
                            e = se;
                        }
                        else {
                            e.addSuppressed(se);
                        }
                    }

                    try {
                        this.recordBuilder.close();
                    }
                    catch (SQLException var18) {
                        se = new IfxStreamException("Could not close record builder", var18);
                        if (e == null) {
                            e = se;
                        }
                        else {
                            e.addSuppressed(se);
                        }
                    }
                    break label211;
                }

                this.isClosed = true;

                try {
                    this.con.close();
                }
                catch (SQLException var21) {
                    se = new IfxStreamException("Could not close main connection", var21);
                    if (e == null) {
                        e = se;
                    }
                    else {
                        e.addSuppressed(se);
                    }
                }

                try {
                    this.recordBuilder.close();
                }
                catch (SQLException var20) {
                    se = new IfxStreamException("Could not close record builder", var20);
                    if (e == null) {
                        e = se;
                    }
                    else {
                        e.addSuppressed(se);
                    }
                }
            }

            if (e != null) {
                throw e;
            }
        }
    }

    public boolean isInlineLOB() {
        return this.inlineLOB;
    }

    public Builder getBuilder() {
        return this.builder;
    }

    public static class Builder {
        private final DataSource ds;
        private final List<POIfxCDCEngine.IfmxWatchedTable> tables = new ArrayList();
        private int timeout = 5;

        private int maxRecs = 10;
        private int buffer = 10240;
        private long sequencePosition = 0L;
        private boolean stopLoggingOnClose = true;

        public Builder(DataSource ds) {
            this.ds = ds;
        }

        public Builder timeout(int timeout) {
            this.timeout = timeout;
            return this;
        }

        public Builder maxRecs(int maxRecs) {
            this.maxRecs = maxRecs;
            return this;
        }

        public Builder buffer(int bufferSize) {
            this.buffer = bufferSize;
            return this;
        }

        public int getBufferSize() {
            return this.buffer;
        }

        public Builder watchTable(String canonicalTableName, String... columns) {
            return this.watchTable(IfmxTableDescriptor.parse(canonicalTableName), columns);
        }

        public Builder watchTable(IfmxTableDescriptor desc, String... columns) {
            this.tables.add((new POIfxCDCEngine.IfmxWatchedTable(desc)).columns(columns));
            return this;
        }

        public Builder watchTable(POIfxCDCEngine.IfmxWatchedTable table) {
            this.tables.add(table);
            return this;
        }

        public int getTimeout() {
            return this.timeout;
        }

        public int getMaxRecs() {
            return this.maxRecs;
        }

        public List<POIfxCDCEngine.IfmxWatchedTable> getWatchedTables() {
            return this.tables;
        }

        public DataSource getDataSource() {
            return this.ds;
        }

        public Builder sequenceId(long position) {
            this.sequencePosition = position;
            return this;
        }

        public long getSequenceId() {
            return this.sequencePosition;
        }

        public Builder stopLoggingOnClose(boolean stopOnClose) {
            this.stopLoggingOnClose = stopOnClose;
            return this;
        }

        public POIfxCDCEngine build() throws SQLException {
            return new POIfxCDCEngine(this);
        }
    }

    public static class IfmxWatchedTable extends IfmxTableDescriptor {
        private static final AtomicInteger counter = new AtomicInteger(1);
        private int label = -1;
        private String[] columns;

        public IfmxWatchedTable(String databaseName, String namespace, String tableName) {
            super(databaseName, namespace, tableName);
            this.label = counter.getAndIncrement();
        }

        public IfmxWatchedTable(IfmxTableDescriptor desc) {
            super(desc.getDatabaseName(), desc.getNamespace(), desc.getTableName());
            this.label = counter.getAndIncrement();
        }

        public String getColumnDescriptorString() {
            return String.join(",", this.columns);
        }

        public String[] getColumns() {
            return this.columns;
        }

        public IfmxWatchedTable columns(String[] columns) {
            this.columns = columns;
            return this;
        }

        public IfmxWatchedTable label(int label) {
            if (label < 0) {
                throw new IllegalArgumentException("Label must be a positive number");
            }
            else {
                this.label = label;
                return this;
            }
        }

        public int getLabel() {
            return this.label;
        }

        public String toString() {
            return super.toString() + "::" + this.getColumnDescriptorString();
        }
    }
}
