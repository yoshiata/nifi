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
package org.apache.nifi.cdc.mysql.processors;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.GtidEventData;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.cdc.event.RowEventException;
import org.apache.nifi.cdc.event.TableInfo;
import org.apache.nifi.cdc.event.TableInfoCacheKey;
import org.apache.nifi.cdc.event.io.EventWriter;
import org.apache.nifi.cdc.mysql.event.BeginTransactionEventInfo;
import org.apache.nifi.cdc.mysql.event.BinlogEventInfo;
import org.apache.nifi.cdc.mysql.event.BinlogEventListener;
import org.apache.nifi.cdc.mysql.event.BinlogLifecycleListener;
import org.apache.nifi.cdc.mysql.event.CommitTransactionEventInfo;
import org.apache.nifi.cdc.mysql.event.DDLEventInfo;
import org.apache.nifi.cdc.mysql.event.DeleteRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.InsertRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.RawBinlogEvent;
import org.apache.nifi.cdc.mysql.event.UpdateRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.io.BeginTransactionEventWriter;
import org.apache.nifi.cdc.mysql.event.io.CommitTransactionEventWriter;
import org.apache.nifi.cdc.mysql.event.io.DDLEventWriter;
import org.apache.nifi.cdc.mysql.event.io.DeleteRowsWriter;
import org.apache.nifi.cdc.mysql.event.io.InsertRowsWriter;
import org.apache.nifi.cdc.mysql.event.io.UpdateRowsWriter;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static com.github.shyiko.mysql.binlog.event.EventType.DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.EXT_DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.PRE_GA_DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.PRE_GA_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.WRITE_ROWS;

/**
 * A processor to retrieve Change Data Capture (CDC) events and send them as flow files.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "jdbc", "cdc", "mysql"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from a MySQL database. CDC Events include INSERT, UPDATE, DELETE operations. Events "
    + "are output as individual flow files ordered by the time at which the operation occurred.")
@Stateful(scopes = Scope.CLUSTER, description = "Information such as a 'pointer' to the current CDC event in the database is stored by this processor, such "
    + "that it can continue from the same location if restarted.")
@WritesAttributes({
    @WritesAttribute(attribute = EventWriter.SEQUENCE_ID_KEY, description = "A sequence identifier (i.e. strictly increasing integer value) specifying the order "
        + "of the CDC event flow file relative to the other event flow file(s)."),
    @WritesAttribute(attribute = EventWriter.CDC_EVENT_TYPE_ATTRIBUTE, description = "A string indicating the type of CDC event that occurred, including (but not limited to) "
        + "'begin', 'insert', 'update', 'delete', 'ddl' and 'commit'."),
    @WritesAttribute(attribute = "mime.type", description = "The processor outputs flow file content in JSON format, and sets the mime.type attribute to "
        + "application/json")
})
public class CaptureChangeMySQLGtid extends CaptureChangeMySQL {

  public static final PropertyDescriptor INIT_GTID = new PropertyDescriptor.Builder()
      .name("capture-change-mysql-init-gtid")
      .displayName("Initial Gtid")
      .description("Specifies an initial gtid to use if this processor's State does not have a current gtid. If a gtid is present "
          + "in the processor's State, this property is ignored. This can be used to \"skip ahead\" if previous events are not desired. "
          + "Note that NiFi Expression Language is supported, but this property is evaluated when the processor is configured, so FlowFile attributes may not be used. Expression "
          + "Language is supported to enable the use of the Variable Registry and/or environment properties.")
      .required(false)
      .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
      .expressionLanguageSupported(true)
      .build();

  private static List<PropertyDescriptor> propDescriptors;

  private volatile ProcessSession currentSession;
  private BinaryLogClient binlogClient;
  private BinlogEventListener eventListener;
  private BinlogLifecycleListener lifecycleListener;
  private GtidSet gtidSet;

  private volatile LinkedBlockingQueue<RawBinlogEvent> queue = new LinkedBlockingQueue<>();
  private volatile String currentGtidSet = null;

  // The following variables save the value of the binlog gtid. Used for rollback
  private volatile String xactGtidSet = null;
  private volatile long xactSequenceId = 0;

  private volatile TableInfo currentTable = null;
  private volatile String currentDatabase = null;
  private volatile Pattern databaseNamePattern;
  private volatile Pattern tableNamePattern;
  private volatile boolean includeBeginCommit = false;
  private volatile boolean includeDDLEvents = false;

  private volatile boolean inTransaction = false;
  private volatile boolean skipTable = false;
  private AtomicBoolean doStop = new AtomicBoolean(false);
  private AtomicBoolean hasRun = new AtomicBoolean(false);

  private int currentHost = 0;
  private String transitUri = "<unknown>";

  private volatile long lastStateUpdate = 0L;
  private volatile long stateUpdateInterval = -1L;
  private AtomicLong currentSequenceId = new AtomicLong(0);

  private volatile DistributedMapCacheClient cacheClient = null;
  private final Serializer<TableInfoCacheKey> cacheKeySerializer = new TableInfoCacheKey.Serializer();
  private final Serializer<TableInfo> cacheValueSerializer = new TableInfo.Serializer();
  private final Deserializer<TableInfo> cacheValueDeserializer = new TableInfo.Deserializer();

  private Connection jdbcConnection = null;

  private final BeginTransactionEventWriter beginEventWriter = new BeginTransactionEventWriter();
  private final CommitTransactionEventWriter commitEventWriter = new CommitTransactionEventWriter();
  private final DDLEventWriter ddlEventWriter = new DDLEventWriter();
  private final InsertRowsWriter insertRowsWriter = new InsertRowsWriter();
  private final DeleteRowsWriter deleteRowsWriter = new DeleteRowsWriter();
  private final UpdateRowsWriter updateRowsWriter = new UpdateRowsWriter();

  static {

    final Set<Relationship> r = new HashSet<>();
    r.add(REL_SUCCESS);
    relationships = Collections.unmodifiableSet(r);

    final List<PropertyDescriptor> pds = new ArrayList<>();
    pds.add(HOSTS);
    pds.add(DRIVER_NAME);
    pds.add(DRIVER_LOCATION);
    pds.add(USERNAME);
    pds.add(PASSWORD);
    pds.add(SERVER_ID);
    pds.add(DATABASE_NAME_PATTERN);
    pds.add(TABLE_NAME_PATTERN);
    pds.add(CONNECT_TIMEOUT);
    pds.add(DIST_CACHE_CLIENT);
    pds.add(RETRIEVE_ALL_RECORDS);
    pds.add(INCLUDE_BEGIN_COMMIT);
    pds.add(INCLUDE_DDL_EVENTS);
    pds.add(STATE_UPDATE_INTERVAL);
    pds.add(INIT_SEQUENCE_ID);
    pds.add(INIT_GTID);
    propDescriptors = Collections.unmodifiableList(pds);
  }

  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return propDescriptors;
  }

  @Override
  public void setup(ProcessContext context) {
    final ComponentLog logger = getLogger();

    final StateManager stateManager = context.getStateManager();
    final StateMap stateMap;

    try {
      stateMap = stateManager.getState(Scope.CLUSTER);
    } catch (final IOException ioe) {
      logger.error("Failed to retrieve observed maximum values from the State Manager. Will not attempt "
          + "connection until this is accomplished.", ioe);
      context.yield();
      return;
    }

    PropertyValue dbNameValue = context.getProperty(DATABASE_NAME_PATTERN);
    databaseNamePattern = dbNameValue.isSet() ? Pattern.compile(dbNameValue.getValue()) : null;

    PropertyValue tableNameValue = context.getProperty(TABLE_NAME_PATTERN);
    tableNamePattern = tableNameValue.isSet() ? Pattern.compile(tableNameValue.getValue()) : null;

    stateUpdateInterval = context.getProperty(STATE_UPDATE_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

    boolean getAllRecords = context.getProperty(RETRIEVE_ALL_RECORDS).asBoolean();

    includeBeginCommit = context.getProperty(INCLUDE_BEGIN_COMMIT).asBoolean();
    includeDDLEvents = context.getProperty(INCLUDE_DDL_EVENTS).asBoolean();

    // Set current gtid to whatever is in State, falling back to the Retrieve All Records then Initial Gtid if no State variable is present
    currentGtidSet = stateMap.get(BinlogEventInfo.BINLOG_GTIDSET_KEY);
    if (currentGtidSet == null) {
      if (!getAllRecords) {
        if (context.getProperty(INIT_GTID).isSet()) {
          currentGtidSet = context.getProperty(INIT_GTID).evaluateAttributeExpressions().getValue();
        }
      } else {
        // If we're starting from the beginning of all binlogs, the binlog gtid must be the empty string (not null)
        currentGtidSet = "";
      }
    }

    // Get current sequence ID from state
    String seqIdString = stateMap.get(EventWriter.SEQUENCE_ID_KEY);
    if (StringUtils.isEmpty(seqIdString)) {
      // Use Initial Sequence ID property if none is found in state
      PropertyValue seqIdProp = context.getProperty(INIT_SEQUENCE_ID);
      if (seqIdProp.isSet()) {
        currentSequenceId.set(seqIdProp.evaluateAttributeExpressions().asInteger());
      }
    } else {
      currentSequenceId.set(Integer.parseInt(seqIdString));
    }

    // Get reference to Distributed Cache if one exists. If it does not, no enrichment (resolution of column names, e.g.) will be performed
    boolean createEnrichmentConnection = false;
    if (context.getProperty(DIST_CACHE_CLIENT).isSet()) {
      cacheClient = context.getProperty(DIST_CACHE_CLIENT).asControllerService(DistributedMapCacheClient.class);
      createEnrichmentConnection = true;
    } else {
      logger.warn("No Distributed Map Cache Client is specified, so no event enrichment (resolution of column names, e.g.) will be performed.");
      cacheClient = null;
    }

    // Save off MySQL cluster and JDBC driver information, will be used to connect for event enrichment as well as for the binlog connector
    try {
      List<InetSocketAddress> hosts = getHosts(context.getProperty(HOSTS).evaluateAttributeExpressions().getValue());

      String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
      String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

      // BinaryLogClient expects a non-null password, so set it to the empty string if it is not provided
      if (password == null) {
        password = "";
      }

      long connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

      String driverLocation = context.getProperty(DRIVER_LOCATION).evaluateAttributeExpressions().getValue();
      String driverName = context.getProperty(DRIVER_NAME).evaluateAttributeExpressions().getValue();

      Long serverId = context.getProperty(SERVER_ID).evaluateAttributeExpressions().asLong();

      connect(hosts, username, password, serverId, createEnrichmentConnection, driverLocation, driverName, connectTimeout);
    } catch (IOException | IllegalStateException e) {
      context.yield();
      binlogClient = null;
      throw new ProcessException(e.getMessage(), e);
    }
  }

  @Override
  public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

    // Indicate that this processor has executed at least once, so we know whether or not the state values are valid and should be updated
    hasRun.set(true);
    ComponentLog log = getLogger();
    StateManager stateManager = context.getStateManager();

    // Create a client if we don't have one
    if (binlogClient == null) {
      setup(context);
    }

    // If the client has been disconnected, try to reconnect
    if (!binlogClient.isConnected()) {
      Exception e = lifecycleListener.getException();
      // If there's no exception, the listener callback might not have been executed yet, so try again later. Otherwise clean up and start over next time
      if (e != null) {
        // Communications failure, disconnect and try next time
        log.error("Binlog connector communications failure: " + e.getMessage(), e);
        try {
          stop(stateManager);
        } catch (CDCException ioe) {
          throw new ProcessException(ioe);
        }
      }

      // Try again later
      context.yield();
      return;
    }

    if (currentSession == null) {
      currentSession = sessionFactory.createSession();
    }

    try {
      outputEvents(currentSession, stateManager, log);
      long now = System.currentTimeMillis();
      long timeSinceLastUpdate = now - lastStateUpdate;

      if (stateUpdateInterval != 0 && timeSinceLastUpdate >= stateUpdateInterval) {
        updateState(stateManager, currentGtidSet, currentSequenceId.get());
        lastStateUpdate = now;
      }
    } catch (IOException ioe) {
      try {
        // Perform some processor-level "rollback", then rollback the session
        currentGtidSet = xactGtidSet == null ? "" : xactGtidSet;
        currentSequenceId.set(xactSequenceId);
        inTransaction = false;
        stop(stateManager);
        queue.clear();
        currentSession.rollback();
      } catch (Exception e) {
        // Not much we can recover from here
        log.warn("Error occurred during rollback", e);
      }
      throw new ProcessException(ioe);
    }
  }

  /**
   * Get a list of hosts from a NiFi property, e.g.
   *
   * @param hostsString A comma-separated list of hosts (host:port,host2:port2, etc.)
   * @return List of InetSocketAddresses for the hosts
   */
  private List<InetSocketAddress> getHosts(String hostsString) {

    if (hostsString == null) {
      return null;
    }
    final List<String> hostsSplit = Arrays.asList(hostsString.split(","));
    List<InetSocketAddress> hostsList = new ArrayList<>();

    for (String item : hostsSplit) {
      String[] addresses = item.split(":");
      if (addresses.length != 2) {
        throw new ArrayIndexOutOfBoundsException("Not in host:port format");
      }

      hostsList.add(new InetSocketAddress(addresses[0].trim(), Integer.parseInt(addresses[1].trim())));
    }
    return hostsList;
  }

  @Override
  protected void connect(List<InetSocketAddress> hosts, String username, String password, Long serverId, boolean createEnrichmentConnection,
      String driverLocation, String driverName, long connectTimeout) throws IOException {

    int connectionAttempts = 0;
    final int numHosts = hosts.size();
    InetSocketAddress connectedHost = null;
    Exception lastConnectException = new Exception("Unknown connection error");

    if (createEnrichmentConnection) {
      try {
        // Ensure driverLocation and driverName are correct before establishing binlog connection
        // to avoid failing after binlog messages are received.
        // Actual JDBC connection is created after binlog client gets started, because we need
        // the connect-able host same as the binlog client.
        registerDriver(driverLocation, driverName);
      } catch (InitializationException e) {
        throw new RuntimeException("Failed to register JDBC driver. Ensure MySQL Driver Location(s)" +
            " and MySQL Driver Class Name are configured correctly. " + e, e);
      }
    }

    while (connectedHost == null && connectionAttempts < numHosts) {
      if (binlogClient == null) {

        connectedHost = hosts.get(currentHost);
        binlogClient = createBinlogClient(connectedHost.getHostString(), connectedHost.getPort(), username, password);
      }

      // Add an event listener and lifecycle listener for binlog and client events, respectively
      if (eventListener == null) {
        eventListener = createBinlogEventListener(binlogClient, queue);
      }
      eventListener.start();
      binlogClient.registerEventListener(eventListener);

      if (lifecycleListener == null) {
        lifecycleListener = createBinlogLifecycleListener();
      }
      binlogClient.registerLifecycleListener(lifecycleListener);

      binlogClient.setBinlogFilename("");
      binlogClient.setGtidSet(currentGtidSet);
      binlogClient.setGtidSetFallbackToPurged(true);

      if (serverId != null) {
        binlogClient.setServerId(serverId);
      }

      try {
        if (connectTimeout == 0) {
          connectTimeout = Long.MAX_VALUE;
        }
        binlogClient.connect(connectTimeout);
        transitUri = "mysql://" + connectedHost.getHostString() + ":" + connectedHost.getPort();

      } catch (IOException | TimeoutException te) {
        // Try the next host
        connectedHost = null;
        transitUri = "<unknown>";
        currentHost = (currentHost + 1) % numHosts;
        connectionAttempts++;
        lastConnectException = te;
      }
    }
    if (!binlogClient.isConnected()) {
      binlogClient.disconnect();
      binlogClient = null;
      throw new IOException("Could not connect binlog client to any of the specified hosts due to: " + lastConnectException.getMessage(), lastConnectException);
    }

    if (createEnrichmentConnection) {
      try {
        jdbcConnection = getJdbcConnection(driverLocation, driverName, connectedHost, username, password, null);
      } catch (InitializationException | SQLException e) {
        binlogClient.disconnect();
        binlogClient = null;
        throw new IOException("Error creating binlog enrichment JDBC connection to any of the specified hosts", e);
      }
    }

    gtidSet = new GtidSet(binlogClient.getGtidSet());
    doStop.set(false);
  }

  @Override
  public void outputEvents(ProcessSession session, StateManager stateManager, ComponentLog log) throws IOException {
    RawBinlogEvent rawBinlogEvent;

    // Drain the queue
    while ((rawBinlogEvent = queue.poll()) != null && !doStop.get()) {
      Event event = rawBinlogEvent.getEvent();
      EventHeaderV4 header = event.getHeader();
      long timestamp = header.getTimestamp();
      EventType eventType = header.getEventType();
      log.debug("Got message event type: {} ", new Object[]{header.getEventType().toString()});
      switch (eventType) {
        case TABLE_MAP:
          // This is sent to inform which table is about to be changed by subsequent events
          TableMapEventData data = event.getData();

          // Should we skip this table? Yes if we've specified a DB or table name pattern and they don't match
          skipTable = (databaseNamePattern != null && !databaseNamePattern.matcher(data.getDatabase()).matches())
              || (tableNamePattern != null && !tableNamePattern.matcher(data.getTable()).matches());

          if (!skipTable) {
            TableInfoCacheKey key = new TableInfoCacheKey(this.getIdentifier(), data.getDatabase(), data.getTable(), data.getTableId());
            if (cacheClient != null) {
              try {
                currentTable = cacheClient.get(key, cacheKeySerializer, cacheValueDeserializer);
              } catch (ConnectException ce) {
                throw new IOException("Could not connect to Distributed Map Cache server to get table information", ce);
              }

              if (currentTable == null) {
                // We don't have an entry for this table yet, so fetch the info from the database and populate the cache
                try {
                  currentTable = loadTableInfo(key);
                  try {
                    cacheClient.put(key, currentTable, cacheKeySerializer, cacheValueSerializer);
                  } catch (ConnectException ce) {
                    throw new IOException("Could not connect to Distributed Map Cache server to put table information", ce);
                  }
                } catch (SQLException se) {
                  // Propagate the error up, so things like rollback and logging/bulletins can be handled
                  throw new IOException(se.getMessage(), se);
                }
              }
            }
          } else {
            // Clear the current table, to force a reload next time we get a TABLE_MAP event we care about
            currentTable = null;
          }
          break;
        case QUERY:
          QueryEventData queryEventData = event.getData();
          currentDatabase = queryEventData.getDatabase();

          String sql = queryEventData.getSql();

          // Is this the start of a transaction?
          if ("BEGIN".equals(sql)) {
            // If we're already in a transaction, something bad happened, alert the user
            if (inTransaction) {
              throw new IOException("BEGIN event received while already processing a transaction. This could indicate that your binlog position is invalid.");
            }
            // Mark the current gtid in case we have to rollback the transaction (if the processor is stopped, e.g.)
            xactGtidSet = currentGtidSet;
            xactSequenceId = currentSequenceId.get();

            if (includeBeginCommit && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
              BeginTransactionEventInfo beginEvent = new BeginTransactionEventInfo(currentDatabase, timestamp, currentGtidSet);
              currentSequenceId.set(beginEventWriter.writeEvent(currentSession, transitUri, beginEvent, currentSequenceId.get(), REL_SUCCESS));
            }
            inTransaction = true;
          } else if ("COMMIT".equals(sql)) {
            if (!inTransaction) {
              throw new IOException("COMMIT event received while not processing a transaction (i.e. no corresponding BEGIN event). "
                  + "This could indicate that your binlog position is invalid.");
            }
            // InnoDB generates XID events for "commit", but MyISAM generates Query events with "COMMIT", so handle that here
            if (includeBeginCommit && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
              CommitTransactionEventInfo commitTransactionEvent = new CommitTransactionEventInfo(currentDatabase, timestamp, currentGtidSet);
              currentSequenceId.set(commitEventWriter.writeEvent(currentSession, transitUri, commitTransactionEvent, currentSequenceId.get(), REL_SUCCESS));
            }
            // Commit the NiFi session
            session.commit();
            inTransaction = false;
            currentTable = null;

          } else {
            // Check for DDL events (alter table, e.g.). Normalize the query to do string matching on the type of change
            String normalizedQuery = sql.toLowerCase().trim().replaceAll(" {2,}", " ");
            if (normalizedQuery.startsWith("alter table")
                || normalizedQuery.startsWith("alter ignore table")
                || normalizedQuery.startsWith("create table")
                || normalizedQuery.startsWith("truncate table")
                || normalizedQuery.startsWith("rename table")
                || normalizedQuery.startsWith("drop table")
                || normalizedQuery.startsWith("drop database")) {

              if (includeDDLEvents && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
                // If we don't have table information, we can still use the database name
                TableInfo ddlTableInfo = (currentTable != null) ? currentTable : new TableInfo(currentDatabase, null, null, null);
                DDLEventInfo ddlEvent = new DDLEventInfo(ddlTableInfo, timestamp, currentGtidSet, sql);
                currentSequenceId.set(ddlEventWriter.writeEvent(currentSession, transitUri, ddlEvent, currentSequenceId.get(), REL_SUCCESS));
              }
              // Remove all the keys from the cache that this processor added
              if (cacheClient != null) {
                cacheClient.removeByPattern(this.getIdentifier() + ".*");
              }
              // If not in a transaction, commit the session so the DDL event(s) will be transferred
              if (includeDDLEvents && !inTransaction) {
                session.commit();
              }
            }
          }
          break;

        case XID:
          if (!inTransaction) {
            throw new IOException("COMMIT event received while not processing a transaction (i.e. no corresponding BEGIN event). "
                + "This could indicate that your binlog position is invalid.");
          }
          if (includeBeginCommit && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
            CommitTransactionEventInfo commitTransactionEvent = new CommitTransactionEventInfo(currentDatabase, timestamp, currentGtidSet);
            currentSequenceId.set(commitEventWriter.writeEvent(currentSession, transitUri, commitTransactionEvent, currentSequenceId.get(), REL_SUCCESS));
          }
          // Commit the NiFi session
          session.commit();
          inTransaction = false;
          currentTable = null;
          currentDatabase = null;
          break;

        case WRITE_ROWS:
        case EXT_WRITE_ROWS:
        case PRE_GA_WRITE_ROWS:
        case UPDATE_ROWS:
        case EXT_UPDATE_ROWS:
        case PRE_GA_UPDATE_ROWS:
        case DELETE_ROWS:
        case EXT_DELETE_ROWS:
        case PRE_GA_DELETE_ROWS:
          // If we are skipping this table, then don't emit any events related to its modification
          if (skipTable) {
            break;
          }
          if (!inTransaction) {
            // These events should only happen inside a transaction, warn the user otherwise
            log.warn("Table modification event occurred outside of a transaction.");
            break;
          }
          if (currentTable == null && cacheClient != null) {
            // No Table Map event was processed prior to this event, which should not happen, so throw an error
            throw new RowEventException("No table information is available for this event, cannot process further.");
          }

          if (eventType == WRITE_ROWS
              || eventType == EXT_WRITE_ROWS
              || eventType == PRE_GA_WRITE_ROWS) {

            InsertRowsEventInfo eventInfo = new InsertRowsEventInfo(currentTable, timestamp, currentGtidSet, event.getData());
            currentSequenceId.set(insertRowsWriter.writeEvent(currentSession, transitUri, eventInfo, currentSequenceId.get(), REL_SUCCESS));

          } else if (eventType == DELETE_ROWS
              || eventType == EXT_DELETE_ROWS
              || eventType == PRE_GA_DELETE_ROWS) {

            DeleteRowsEventInfo eventInfo = new DeleteRowsEventInfo(currentTable, timestamp, currentGtidSet, event.getData());
            currentSequenceId.set(deleteRowsWriter.writeEvent(currentSession, transitUri, eventInfo, currentSequenceId.get(), REL_SUCCESS));

          } else {
            // Update event
            UpdateRowsEventInfo eventInfo = new UpdateRowsEventInfo(currentTable, timestamp, currentGtidSet, event.getData());
            currentSequenceId.set(updateRowsWriter.writeEvent(currentSession, transitUri, eventInfo, currentSequenceId.get(), REL_SUCCESS));
          }
          break;

        case GTID:
          // Update current binlog gtid
          GtidEventData gtidEventData = event.getData();
          gtidSet.add(gtidEventData.getGtid());
          currentGtidSet = gtidSet.toString();
          break;
        default:
          break;
      }
    }
  }

  @Override
  protected void stop(StateManager stateManager) throws CDCException {
    try {
      if (binlogClient != null) {
        binlogClient.disconnect();
      }
      if (eventListener != null) {
        eventListener.stop();
        if (binlogClient != null) {
          binlogClient.unregisterEventListener(eventListener);
        }
      }
      doStop.set(true);

      if (hasRun.getAndSet(false)) {
        updateState(stateManager, currentGtidSet, currentSequenceId.get());
      }

    } catch (IOException e) {
      throw new CDCException("Error closing CDC connection", e);
    } finally {
      binlogClient = null;
    }
  }

  private void updateState(StateManager stateManager, String gtidSet, long sequenceId) throws IOException {
    // Update state with latest values
    if (stateManager != null) {
      Map<String, String> newStateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

      // Save current binlog gtid to the state map
      if (gtidSet != null) {
        newStateMap.put(BinlogEventInfo.BINLOG_GTIDSET_KEY, gtidSet);
      }
      newStateMap.put(EventWriter.SEQUENCE_ID_KEY, String.valueOf(sequenceId));
      stateManager.setState(newStateMap, Scope.CLUSTER);
    }
  }
}
