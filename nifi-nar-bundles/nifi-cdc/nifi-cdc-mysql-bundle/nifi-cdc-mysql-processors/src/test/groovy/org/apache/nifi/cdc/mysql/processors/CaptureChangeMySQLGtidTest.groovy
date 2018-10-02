/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.cdc.mysql.processors

import com.github.shyiko.mysql.binlog.BinaryLogClient
import com.github.shyiko.mysql.binlog.event.DeleteRowsEventData
import com.github.shyiko.mysql.binlog.event.Event
import com.github.shyiko.mysql.binlog.event.EventData
import com.github.shyiko.mysql.binlog.event.EventHeaderV4
import com.github.shyiko.mysql.binlog.event.EventType
import com.github.shyiko.mysql.binlog.event.GtidEventData
import com.github.shyiko.mysql.binlog.event.QueryEventData
import com.github.shyiko.mysql.binlog.event.TableMapEventData
import com.github.shyiko.mysql.binlog.event.UpdateRowsEventData
import com.github.shyiko.mysql.binlog.event.WriteRowsEventData
import groovy.json.JsonSlurper
import org.apache.nifi.cdc.event.ColumnDefinition
import org.apache.nifi.cdc.event.TableInfo
import org.apache.nifi.cdc.event.TableInfoCacheKey
import org.apache.nifi.cdc.event.io.EventWriter
import org.apache.nifi.cdc.mysql.MockBinlogClient
import org.apache.nifi.cdc.mysql.event.BinlogEventInfo
import org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQLTest.DistributedMapCacheClientImpl
import org.apache.nifi.components.state.Scope
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClientService
import org.apache.nifi.processor.exception.ProcessException
import org.apache.nifi.reporting.InitializationException
import org.apache.nifi.util.TestRunner
import org.apache.nifi.util.TestRunners
import org.junit.After
import org.junit.Before
import org.junit.Test

import java.sql.Connection
import java.sql.ResultSet
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.TimeoutException

import static org.apache.nifi.cdc.mysql.processors.CaptureChangeMySQLTest.createCacheClient
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import static org.mockito.Matchers.anyString
import static org.mockito.Mockito.mock
import static org.mockito.Mockito.when

/**
 * Unit test(s) for MySQL CDC referring to GTID
 */
class CaptureChangeMySQLGtidTest {
    CaptureChangeMySQLGtid processor
    TestRunner testRunner
    MockBinlogClient client

    @Before
    void setUp() throws Exception {
        processor = new MockCaptureChangeMySQLGtid()
        testRunner = TestRunners.newTestRunner(processor)
        client = new MockBinlogClient('localhost', 3306, 'root', 'password')
    }

    @After
    void tearDown() throws Exception {

    }

    @Test
    void testConnectionFailures() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')

        client.connectionError = true
        try {
            testRunner.run()
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('Could not connect binlog client to any of the specified hosts due to: Error during connect', ioe.getMessage())
            assertTrue(ioe.getCause() instanceof IOException)
        }
        client.connectionError = false

        client.connectionTimeout = true
        try {
            testRunner.run()
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('Could not connect binlog client to any of the specified hosts due to: Connection timed out', ioe.getMessage())
            assertTrue(ioe.getCause() instanceof TimeoutException)
        }
        client.connectionTimeout = false
    }

    @Test
    void testBasicTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '0 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INIT_SEQUENCE_ID, '10')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.setProperty(CaptureChangeMySQLGtid.SERVER_ID, '1')
        final DistributedMapCacheClientImpl cacheClient = createCacheClient()
        def clientProperties = [:]
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), 'localhost')
        testRunner.addControllerService('client', cacheClient, clientProperties)
        testRunner.setProperty(CaptureChangeMySQLGtid.DIST_CACHE_CLIENT, 'client')
        testRunner.enableControllerService(cacheClient)

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE_MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 100, database: 'myDB', table: 'myTable'] as TableMapEventData
        ))

        // INSERT
        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 100, includedColumns: cols,
                 rows: [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 10] as EventHeaderV4,
                {} as EventData
        ))

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 12] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 14] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE_MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 16] as EventHeaderV4,
                [tableId: 100, database: 'myDB', table: 'myTable'] as TableMapEventData
        ))

        // UPDATE
        def colsBefore = new BitSet()
        colsBefore.set(0)
        colsBefore.set(1)
        def colsAfter = new BitSet()
        colsAfter.set(1)
        Map.Entry<Serializable[], Serializable[]> updateMapEntry = new Map.Entry<Serializable[], Serializable[]>() {
            Serializable[] getKey() {
                return [2, 'Smith'] as Serializable[]
            }

            @Override
            Serializable[] getValue() {
                return [3, 'Jones'] as Serializable[]
            }

            @Override
            Serializable[] setValue(Serializable[] value) {
                return [3, 'Jones'] as Serializable[]
            }
        }

        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.UPDATE_ROWS, nextPosition: 18] as EventHeaderV4,
                [tableId: 1, includedColumnsBeforeUpdate: colsBefore, includedColumns: colsAfter, rows: [updateMapEntry]
                        as List<Map.Entry<Serializable[], Serializable[]>>] as UpdateRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 20] as EventHeaderV4,
                {} as EventData
        ))

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 22] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:3'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 24] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE_MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 26] as EventHeaderV4,
                [tableId: 100, database: 'myDB', table: 'myTable'] as TableMapEventData
        ))

        // DELETE
        cols = new BitSet()
        cols.set(0)
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_DELETE_ROWS, nextPosition: 28] as EventHeaderV4,
                [tableId: 1, includedColumns: cols, rows: [[2, 'Smith'] as Serializable[], [3, 'Jones'] as Serializable[]] as List<Serializable[]>] as DeleteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 30] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(10, resultFiles.size())
        assertEquals(
                'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1-3',
                resultFiles.last().getAttribute(BinlogEventInfo.BINLOG_GTIDSET_KEY)
        )
    }

    @Test
    void testInitialGtidIgnoredWhenStatePresent() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INIT_GTID, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1')
        testRunner.setProperty(CaptureChangeMySQLGtid.INIT_SEQUENCE_ID, '10')
        testRunner.setProperty(CaptureChangeMySQLGtid.RETRIEVE_ALL_RECORDS, 'false')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.getStateManager().setState([
                ("${BinlogEventInfo.BINLOG_GTIDSET_KEY}".toString()): 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2',
                ("${EventWriter.SEQUENCE_ID_KEY}".toString()): '1'
        ], Scope.CLUSTER)

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:3'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQLGtid.REL_SUCCESS)

        assertEquals(2, resultFiles.size())
        assertEquals(
                'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2-3',
                resultFiles.last().getAttribute(BinlogEventInfo.BINLOG_GTIDSET_KEY)
        )
    }

    @Test
    void testInitialGtidNoStatePresent() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INIT_GTID, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1')
        testRunner.setProperty(CaptureChangeMySQLGtid.RETRIEVE_ALL_RECORDS, 'false')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:3'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQLGtid.REL_SUCCESS)

        assertEquals(2, resultFiles.size())
        assertEquals(
                'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1-1:3-3',
                resultFiles.last().getAttribute(BinlogEventInfo.BINLOG_GTIDSET_KEY)
        )
    }

    @Test
    void testCommitWithoutBegin() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')

        testRunner.run(1, false, true)

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 2] as EventHeaderV4,
                {} as EventData
        ))

        try {
            testRunner.run(1, true, false)
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('COMMIT event received while not processing a transaction (i.e. no corresponding BEGIN event). ' +
                    'This could indicate that your binlog position is invalid.', ioe.getMessage())
        }

        testRunner.run(1, false, true)

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'COMMIT'] as QueryEventData
        ))

        try {
            testRunner.run(1, true, false)
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('COMMIT event received while not processing a transaction (i.e. no corresponding BEGIN event). ' +
                    'This could indicate that your binlog position is invalid.', ioe.getMessage())
        }
    }

    @Test
    void testBeginInsideTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')

        testRunner.run(1, false, true)

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 2] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        try {
            testRunner.run(1, true, false)
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('BEGIN event received while already processing a transaction. This could indicate that your binlog position is invalid.', ioe.getMessage())
        }

    }

    @Test
    void testInsertOutsideTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')

        testRunner.run(1, false, true)

        // TABLE_MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 2] as EventHeaderV4,
                [tableId: 100, database: 'myDB', table: 'myTable'] as TableMapEventData
        ))

        // INSERT
        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 4] as EventHeaderV4,
                [tableId: 100, includedColumns: cols,
                 rows: [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(0, resultFiles.size())
    }

    @Test
    void testNoTableInformationAvailable() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INIT_SEQUENCE_ID, '10')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')
        final DistributedMapCacheClientImpl cacheClient = createCacheClient()
        def clientProperties = [:]
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), 'localhost')
        testRunner.addControllerService('client', cacheClient, clientProperties)
        testRunner.setProperty(CaptureChangeMySQLGtid.DIST_CACHE_CLIENT, 'client')
        testRunner.enableControllerService(cacheClient)

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // INSERT
        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 6] as EventHeaderV4,
                [tableId: 1, includedColumns: cols,
                 rows   : [[2, 'Smith'] as Serializable[], [3, 'Jones'] as Serializable[], [10, 'Cruz'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 8] as EventHeaderV4,
                {} as EventData
        ))

        try {
            testRunner.run(1, true, false)
        } catch (AssertionError ae) {
            def pe = ae.getCause()
            assertTrue(pe instanceof ProcessException)
            def ioe = pe.getCause()
            assertTrue(ioe instanceof IOException)
            assertEquals('No table information is available for this event, cannot process further.', ioe.getMessage())
        }
    }

    @Test
    void testSkipTable() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.setProperty(CaptureChangeMySQLGtid.DATABASE_NAME_PATTERN, "myDB")
        testRunner.setProperty(CaptureChangeMySQLGtid.TABLE_NAME_PATTERN, "myTable")

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // TABLE_MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 6] as EventHeaderV4,
                [tableId: 100, database: 'myDB', table: 'notMyTable'] as TableMapEventData
        ))

        // INSERT
        def cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 8] as EventHeaderV4,
                [tableId: 100, includedColumns: cols,
                 rows: [[2, 'Smith'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // TABLE_MAP
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.TABLE_MAP, nextPosition: 10] as EventHeaderV4,
                [tableId: 101, database: 'myDB', table: 'myTable'] as TableMapEventData
        ))

        // INSERT
        cols = new BitSet()
        cols.set(1)
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.EXT_WRITE_ROWS, nextPosition: 12] as EventHeaderV4,
                [tableId: 100, includedColumns: cols,
                 rows: [[10, 'Cruz'] as Serializable[]] as List<Serializable[]>] as WriteRowsEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 14] as EventHeaderV4,
                [database: 'myDB', sql: 'COMMIT'] as QueryEventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(3, resultFiles.size())
    }

    @Test
    void testSkipDatabase() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_DDL_EVENTS, 'true')
        testRunner.setProperty(CaptureChangeMySQLGtid.DATABASE_NAME_PATTERN, "myDB")
        testRunner.setProperty(CaptureChangeMySQLGtid.TABLE_NAME_PATTERN, "myTable")

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1'] as GtidEventData
        ))

        // QUERY
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'NotMyDB', sql: 'ALTER TABLE myTable add column col1 int'] as QueryEventData
        ))

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 6] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2'] as GtidEventData
        ))

        // QUERY
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 8] as EventHeaderV4,
                [database: 'myDB', sql: 'ALTER TABLE myTable add column col1 int'] as QueryEventData
        ))

        testRunner.run(1, true, false)

        def resultFiles = testRunner.getFlowFilesForRelationship(CaptureChangeMySQL.REL_SUCCESS)
        assertEquals(1, resultFiles.size())

        resultFiles.each {f ->
            def json = new JsonSlurper().parseText(new String(f.toByteArray()))
            assertEquals('myDB', json.database)
        }
    }

    @Test
    void testUpdateState() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_BEGIN_COMMIT, 'true')

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 6] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, false, false)

        // Ensure state not set, as the processor hasn't been stopped and no State Update Interval has been set
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, null, Scope.CLUSTER)

        // Stop the processor and verify the state is set
        testRunner.run(1, true, false)
        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1-1', Scope.CLUSTER)

        testRunner.stateManager.clear(Scope.CLUSTER)

        // Send some events, wait for the State Update Interval, and verify the state was set
        testRunner.setProperty(CaptureChangeMySQLGtid.STATE_UPDATE_INTERVAL, '1 second')
        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 8] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 10] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 12] as EventHeaderV4,
                {} as EventData
        ))

        sleep(1000)

        testRunner.run(1, false, false)

        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2-2', Scope.CLUSTER)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 14] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:3'] as GtidEventData
        ))

        // BEGIN
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 16] as EventHeaderV4,
                [database: 'myDB', sql: 'BEGIN'] as QueryEventData
        ))

        // COMMIT
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.XID, nextPosition: 18] as EventHeaderV4,
                {} as EventData
        ))

        testRunner.run(1, true, false)

        testRunner.stateManager.assertStateEquals(BinlogEventInfo.BINLOG_GTIDSET_KEY, 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:2-3', Scope.CLUSTER)
    }

    @Test
    void testDDLOutsideTransaction() throws Exception {
        testRunner.setProperty(CaptureChangeMySQLGtid.DRIVER_LOCATION, 'file:///path/to/mysql-connector-java-5.1.38-bin.jar')
        testRunner.setProperty(CaptureChangeMySQLGtid.HOSTS, 'localhost:3306')
        testRunner.setProperty(CaptureChangeMySQLGtid.USERNAME, 'root')
        testRunner.setProperty(CaptureChangeMySQLGtid.PASSWORD, 'password')
        testRunner.setProperty(CaptureChangeMySQLGtid.CONNECT_TIMEOUT, '2 seconds')
        testRunner.setProperty(CaptureChangeMySQLGtid.INCLUDE_DDL_EVENTS, 'true')
        final DistributedMapCacheClientImpl cacheClient = createCacheClient()
        def clientProperties = [:]
        clientProperties.put(DistributedMapCacheClientService.HOSTNAME.getName(), 'localhost')
        testRunner.addControllerService('client', cacheClient, clientProperties)
        testRunner.setProperty(CaptureChangeMySQLGtid.DIST_CACHE_CLIENT, 'client')
        testRunner.enableControllerService(cacheClient)

        testRunner.run(1, false, true)

        // GTID
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.GTID, nextPosition: 2] as EventHeaderV4,
                [gtid: 'xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx:1'] as GtidEventData
        ))

        // DROP DATABASE
        client.sendEvent(new Event(
                [timestamp: new Date().time, eventType: EventType.QUERY, nextPosition: 4] as EventHeaderV4,
                [database: 'myDB', sql: 'DROP DATABASE myDB'] as QueryEventData
        ))

        testRunner.run(1, false, false)
        testRunner.assertTransferCount(CaptureChangeMySQL.REL_SUCCESS, 1)
    }

    /********************************
     * Mock and helper classes below
     ********************************/

    class MockCaptureChangeMySQLGtid extends CaptureChangeMySQLGtid {

        Map<TableInfoCacheKey, TableInfo> cache = new HashMap<>()

        @Override
        BinaryLogClient createBinlogClient(String hostname, int port, String username, String password) {
            client
        }

        @Override
        protected TableInfo loadTableInfo(TableInfoCacheKey key) {
            TableInfo tableInfo = cache.get(key)
            if (tableInfo == null) {
                tableInfo = new TableInfo(key.databaseName, key.tableName, key.tableId,
                        [new ColumnDefinition((byte) 4, 'id'),
                         new ColumnDefinition((byte) -4, 'string1')
                        ] as List<ColumnDefinition>)
                cache.put(key, tableInfo)
            }
            return tableInfo
        }

        @Override
        protected void registerDriver(String locationString, String drvName) throws InitializationException {
        }

        @Override
        protected Connection getJdbcConnection(String locationString, String drvName, InetSocketAddress host, String username, String password, Map<String, String> customProperties)
                throws InitializationException, SQLException {
            Connection mockConnection = mock(Connection)
            Statement mockStatement = mock(Statement)
            when(mockConnection.createStatement()).thenReturn(mockStatement)
            ResultSet mockResultSet = mock(ResultSet)
            when(mockStatement.executeQuery(anyString())).thenReturn(mockResultSet)
            return mockConnection
        }
    }
}
