/**
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * ied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db.atalanta;


import com.light.atalanta.*;
import com.light.atalanta.client.*;
import com.light.atalanta.client.factories.AtalantaDbNioClientObjectFactory;
import com.light.atalanta.factory.ObjectFactory;
import com.light.atalanta.model.RecordCompressionType;
//import com.light.atalanta.model.RegionDistributionModel;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.UniformSplit;
import com.light.atalanta.util.XxHash32KeySalter;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.measurements.Measurements;


import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
//import java.util.stream.Collectors;

import static site.ycsb.workloads.CoreWorkload.*;
import static site.ycsb.Client.DO_TRANSACTIONS_PROPERTY;

/**
 * Atalanta client for YCSB framework.
 * <p>
 * Reads fetch complete records; requested fields are selected locally from the
 * configured column family. Batch reads check every record and return fields
 * from the first record. Buffered writes return {@link Status#BATCHED_OK};
 * synchronous writes return {@link Status#OK} only after the SDK call succeeds.
 */
public class AtalantaClient extends site.ycsb.DB {
  private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

  private Configuration config;

  private boolean debug = false;

  private String tableName = "";

  public static final String NAMESPACE_PROPERTY = "namespace";

  /**
   * The default name of the database table to run queries against.
   */
  public static final String NAMESPACE_PROPERTY_DEFAULT = "userns";

  /**
   * A Cluster Connection instance that is shared by all running ycsb threads.
   * Needs to be initialized late so we pick up command-line configs if any.
   * To ensure one instance only in a multi-threaded context, guard access
   * with a 'lock' object.
   *
   * @See #CONNECTION_LOCK.
   */
  private static Connection connection = null;

  private Table currentTable = null;
  private BufferedMutator bufferedMutator = null;

  /**
   * When enabled, updates are accumulated by a BufferedMutator and flushed from cleanup().
   * When disabled, update() obtains and closes a Table for every YCSB operation so the
   * lightweight-table-handle usage pattern can be measured explicitly.
   */
  private boolean clientSideBuffering = false;
  private boolean tableRecreate = false;
  public static final String TABLE_COMPRESSION_PROPERTY = "adb.tbl.compress";
  public static final String TABLE_DICTIONARY_PROPERTY = "adb.tbl.dict";
  public static final String REGION_RECORD_COUNT_PROPERTY = "adb.reg.rec.count";
  private RecordCompressionType tableCompression;
  private Boolean tableDictionary;
  private Long regionRecordCount;
  private String columnFamily = "";
  private byte[] columnFamilyBytes;

  private int tableRegionCount;
  private int batchSize;
  private boolean connectionNIO;


  /**
   * Whether or not a page filter should be used to limit scan length.
   */
  private boolean usePageFilter = true;

  /**
   * Initialize any state for this DB. Called once per DB instance; there is one
   * DB instance per client thread.
   */
  @Override
  public void init() throws DBException {
    // TODO можно namespace брать из имени таблицы.
    String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
    String table = getProperties().getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

    readTableOptions();
    try {
      batchSize = Integer.parseInt(getProperties().getProperty(BATCH_SIZE, BATCH_SIZE_DEFAULT));
    } catch (NumberFormatException e) {
      throw new DBException("batchsize must be a positive integer", e);
    }
    if (batchSize < 1) {
      throw new DBException("batchsize must be a positive integer");
    }
    connectionNIO = Boolean.parseBoolean(getProperties().getProperty(CONNECTION_NIO, CONNECTION_NIO_DEFAULT));
    clientSideBuffering = Boolean.parseBoolean(getProperties().getProperty("clientbuffering", "false"));

    columnFamily = getProperties().getProperty("columnfamily");
    if (columnFamily == null || columnFamily.trim().isEmpty()) {
      throw new DBException("No columnfamily specified");
    }
    columnFamilyBytes = Bytes.toBytes(columnFamily);

    try {
      THREAD_COUNT.getAndIncrement();
      synchronized (THREAD_COUNT) {
        if (connection == null) {
          // Initialize if not set up already.
          Connection newConnection;

          if (connectionNIO) {
            System.out.println("Enabled NIO mode");
            ObjectFactory objectFactory = new AtalantaDbNioClientObjectFactory();
            config = objectFactory.configuration();
            config.addResource(new File("atalanta-site.xml"));
            newConnection = objectFactory.connection(config);
          } else {
            System.out.println("Enabled Local Thread mode");
            config = Configuration.create();
            config.addResource(new File("atalanta-site.xml"));
            newConnection = ConnectionFactoryThreadLocal.createConnection(config);
          }

          try {
            Admin adm = (Admin)newConnection.getAdmin();
            if (!adm.isNamespaceExists(namespace)) {
              adm.createNamespace(namespace);
            }
            // Prepare once, before publishing the shared connection to worker threads.
            prepareInitialTable(adm, TableName.valueOf(namespace, table));
          } catch (Exception e) {
            try {
              newConnection.close();
            } catch (Exception closeError) {
              e.addSuppressed(closeError);
            }
            throw e;
          }
          connection = newConnection;
        }
      }
    } catch (Exception e) {
      THREAD_COUNT.decrementAndGet();
      throw new DBException(e);
    }

    if ((getProperties().getProperty("debug") != null)
        && (getProperties().getProperty("debug").compareTo("true") == 0)) {
      debug = true;
    }

    usePageFilter = isBooleanParamSet("hbase.usepagefilter", usePageFilter);


  }

  // Package-private for offline tests; validate before any destructive table operation.
  void readTableOptions() throws DBException {
    Properties props = getProperties();
    tableCompression = null;
    tableDictionary = null;
    regionRecordCount = null;
    tableRecreate = Boolean.parseBoolean(props.getProperty("tableRecreate",
        props.getProperty(TABLE_RECREATE, TABLE_RECREATE_DEFAULT)));
    try {
      tableRegionCount = Integer.parseInt(props.getProperty(TABLE_REGION_COUNT, TABLE_REGION_COUNT_DEFAULT));
      if (tableRegionCount < 1) {
        throw new IllegalArgumentException("regioncount must be positive");
      }
      String compression = props.getProperty(TABLE_COMPRESSION_PROPERTY);
      if (compression != null) {
        if (!"none".equals(compression) && !"lz".equals(compression)) {
          throw new IllegalArgumentException(TABLE_COMPRESSION_PROPERTY + " must be none or lz");
        }
        tableCompression = RecordCompressionType.valueOf(compression);
      }
      String dictionary = props.getProperty(TABLE_DICTIONARY_PROPERTY);
      if (dictionary != null) {
        if (!"true".equalsIgnoreCase(dictionary) && !"false".equalsIgnoreCase(dictionary)) {
          throw new IllegalArgumentException(TABLE_DICTIONARY_PROPERTY + " must be true or false");
        }
        tableDictionary = Boolean.valueOf(dictionary);
      }
      String recordCount = props.getProperty(REGION_RECORD_COUNT_PROPERTY);
      if (recordCount != null) {
        regionRecordCount = Long.valueOf(recordCount);
        if (regionRecordCount < 0) {
          throw new IllegalArgumentException(REGION_RECORD_COUNT_PROPERTY + " must be non-negative");
        }
      }
    } catch (IllegalArgumentException e) {
      throw new DBException("Invalid table options: " + e.getMessage(), e);
    }
  }

  void prepareInitialTable(Admin admin, TableName targetTable) throws IOException {
    boolean initialLoad = !Boolean.parseBoolean(getProperties().getProperty(DO_TRANSACTIONS_PROPERTY, "true"));
    boolean recreate = initialLoad && tableRecreate
        && (tableCompression != null || tableDictionary != null || regionRecordCount != null);
    boolean exists = admin.tableExists(targetTable);
    if (exists && recreate) {
      System.out.println("Recreating table " + targetTable + "; all existing data will be deleted.");
      if (!admin.isTableDisabled(targetTable)) {
        admin.disableTable(targetTable);
      }
      admin.deleteTable(targetTable);
      exists = false;
    }
    if (!exists) {
      byte[][] splits = tableRegionCount == 1 ? new byte[0][] : new UniformSplit().split(tableRegionCount);
      if (recreate) {
        ColumnDescriptor defaults = ColumnDescriptor.DEFAULT_COLUMN_DESCRIPTOR;
        ColumnDescriptor column = new ColumnDescriptor(Bytes.toBytes(getProperties().getProperty("columnfamily")));
        column.setBlocksize(ATALANTAConstants.DEFAULT_DATA_BLOCK_SIZE);
        column.setCompressionType(defaults.getCompression());
        column.setRecordCompressionType(tableCompression != null
            ? tableCompression : defaults.getRecordCompressionType());
        // Match Admin.createTable(TableName, splitKeys), which disables the dictionary by default.
        column.setDictionary(tableDictionary != null ? tableDictionary : false);
        if (regionRecordCount != null) {
          column.setRecordTargetCount(regionRecordCount);
        }
        TableDescriptor descriptor = new TableDescriptor(targetTable);
        descriptor.addFamily(column);
        admin.createTable(descriptor, splits);
        System.out.println("Table " + targetTable + " created: regions=" + tableRegionCount
            + ", record compression=" + column.getRecordCompressionType()
            + ", dictionary=" + column.getDictionary()
            + ", records per region=" + (regionRecordCount != null ? regionRecordCount : 0));
      } else {
        admin.createTable(targetTable, splits);
      }
    }
  }

  /**
   * Cleanup any state for this DB. Called once per DB instance; there is one DB
   * instance per client thread.
   */
  @Override
  public void cleanup() throws DBException {
    // Get the measurements instance as this is the only client that should
    // count clean up time like an update if client-side buffering is
    // enabled.
    Measurements measurements = Measurements.getMeasurements();
    try {
      long st = System.nanoTime();
      if (bufferedMutator != null) {
        bufferedMutator.close();
        bufferedMutator = null;
      }

      long en = System.nanoTime();
      final String type = clientSideBuffering ? "UPDATE" : "CLEANUP";
      measurements.measure(type, (int) ((en - st) / 1000));
      int threadCount = THREAD_COUNT.decrementAndGet();
      if (threadCount <= 0) {
        // Means we are done so ok to shut down the Connection.
        synchronized (THREAD_COUNT) {
          if (connection != null) {
            connection.close();
            connection = null;
          }
        }
      }
    } catch (Exception e) {
      throw new DBException(e);
    }
  }

  public void getTable(String namespace, String table) throws IOException {
    final TableName tName = TableName.valueOf(namespace, table);
    this.currentTable = (Table) connection.getTable(tName);
  }

  /**
   * Read a record from the database. Each field/value pair from the result will
   * be stored in a HashMap.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to read.
   * @param fields The fields to return, or null for all fields. Atalanta always
   *               fetches the complete record; filtering happens locally.
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    if (!tableName.equals(table) || currentTable == null) {
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException | RuntimeException e) {
        System.err.println("Error accessing ATALANTA table: " + e);
        return Status.ERROR;
      }
    }

    try {
      if (debug) {
        System.out.println("Doing read for key: " + key);
      }

      Result record;
      if (batchSize > 1) {
        List<Get> gets = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
          gets.add(new Get(XxHash32KeySalter.saltKey((key + "_" + i).getBytes())));
        }

        Result[] records = currentTable.get(gets);
        if (records == null || records.length != batchSize) {
          System.err.println("Incomplete batch read for key " + key);
          return Status.ERROR;
        }
        for (int i = 0; i < records.length; i++) {
          if (records[i] == null || records[i].isEmpty()) {
            System.err.println("Record key " + key + "_" + i + " not found (batch read)");
            return Status.ERROR;
          }
        }
        record = records[0];
      } else {
        record = currentTable.get(new Get(XxHash32KeySalter.saltKey((key.getBytes()))));
        if (record == null || record.isEmpty()) {
          System.err.println("Record key " + key + " not found (single read)");
          return Status.ERROR;
        }
      }

      populateResult(record, fields, result);
      return Status.OK;
    } catch (IOException | RuntimeException e) {
      if (debug) {
        System.err.println("Error doing get: " + e);
      }
      return Status.ERROR;
    }
  }

  private void populateResult(Result record, Set<String> fields, Map<String, ByteIterator> result) {
    // Fetching a partial record is not supported; filter the full response locally.
    List<Cell> cells = record.getColumnCells(columnFamilyBytes);
    if (cells != null) {
      for (Cell cell : cells) {
        String field = Bytes.toString(CellUtil.cloneQualifier(cell));
        if (fields == null || fields.contains(field)) {
          result.put(field, new ByteArrayByteIterator(CellUtil.cloneValue(cell)));
        }
      }
    }
  }

  public Status update(String table, String key, Map<String, ByteIterator> values) {
    String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
    final TableName targetTable = TableName.valueOf(namespace, table);
    if (!tableName.equals(table) || currentTable == null) {
      try {
        Admin adm = (Admin) connection.getAdmin();
        if (!adm.isNamespaceExists(namespace)) {
          adm.createNamespace(namespace);

        }
        if (!adm.tableExists(targetTable)) {
          adm.createTable(targetTable, new UniformSplit().split(tableRegionCount));
        }
        if (bufferedMutator != null) {
          bufferedMutator.close();
          bufferedMutator = null;
        }
        getTable(namespace, table);
        if (clientSideBuffering) {
          bufferedMutator = connection.getBufferedMutator(targetTable);
        }
        tableName = table;

      } catch (IOException e) {
        System.err.println("Error accessing ATALANTA table: " + e);
        return Status.ERROR;
      } catch (Exception e) {
        e.printStackTrace();
        return Status.ERROR;
      }
    }
    if (debug) {
      System.out.println("Setting up put for key: " + key);
    }

    HashMap<byte[], byte[]> hmQV = new HashMap<>();
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      hmQV.put(Bytes.toBytes(entry.getKey()), entry.getValue().toArray());
    }

    List<Put> puts = null;
    if (batchSize > 1) {
      puts = new ArrayList<>(batchSize);
      for (int i = 0; i < batchSize; i++) {
        Put p = new Put(XxHash32KeySalter.saltKey((key + "_" + i).getBytes()));
        for (byte[] qfl : hmQV.keySet()) {
          p.addColumn(columnFamilyBytes, qfl, hmQV.get(qfl));
        }
        puts.add(p);
      }
    }
    try {
      if (clientSideBuffering) {
        if (batchSize == 1) {
          Put p = new Put(XxHash32KeySalter.saltKey((key).getBytes()));
          for (byte[] qfl : hmQV.keySet()) {
            p.addColumn(columnFamilyBytes, qfl, hmQV.get(qfl));
          }
          bufferedMutator.mutate(p);
        } else {
          bufferedMutator.mutate(new ArrayList<DbMutation>(puts));
        }
      } else {
        if (batchSize == 1) {
          Put p = new Put(XxHash32KeySalter.saltKey((key).getBytes()));
          for (byte[] qfl : hmQV.keySet()) {
            p.addColumn(columnFamilyBytes, qfl, hmQV.get(qfl));
          }
          currentTable.put(p);
        } else {
          currentTable.put(puts);
        }
      }
      return clientSideBuffering ? Status.BATCHED_OK : Status.OK;
    } catch (IOException | RuntimeException e) {
      if (debug) {
        System.err.println("Error doing put: " + e);
      }
      return Status.ERROR;
    }
  }

  /**
   * Perform a range scan for a set of records in the database. Each field/value
   * pair from the result will be stored in a HashMap.
   *
   * @param table       The name of the table
   * @param startkey    The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields      The list of fields to read, or null for all of them
   * @param result      A Vector of HashMaps, where each HashMap is a set field/value
   *                    pairs for one record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  // TODO scan does not exist
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // if this is a "new" table, init Table object. Else, use existing one
    if (!tableName.equals(table)) {
      currentTable = null;
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException e) {
        System.err.println("Error accessing HBase table: " + e);
        return Status.ERROR;
      }
    }

    Get g = new Get(XxHash32KeySalter.saltKey((startkey).getBytes()));

    // get results
    try {
      Result r = currentTable.get(g);
      String key = new String(r.getRow());
      CellScanner scanner = r.cellScanner();
      StringBuilder sb = new StringBuilder();
      while (scanner.advance()) {
        Cell cell = r.current();
        sb.append("column family:")
            .append(new String(CellUtil.cloneFamily(cell)))
            .append(",")
            .append(new String(CellUtil.cloneQualifier(cell)))
            .append("=")
            .append(new String(CellUtil.cloneValue(cell)));
      }
      System.out.println("Got scan result for key: " + key);
    } catch (IOException e) {
      System.out.println("Error in getting/parsing scan result: " + e);
      return Status.ERROR;
    }

    return Status.OK;
  }


  /**
   * Insert a record in the database. Any field/value pairs in the specified
   * values HashMap will be written into the record with the specified record
   * key.
   *
   * @param table  The name of the table
   * @param key    The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key   The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error
   */
  @Override
  public Status delete(String table, String key) {
    if (!tableName.equals(table) || currentTable == null) {
      try {
        String namespace = getProperties().getProperty(NAMESPACE_PROPERTY, NAMESPACE_PROPERTY_DEFAULT);
        getTable(namespace, table);
        tableName = table;
      } catch (IOException | RuntimeException e) {
        System.err.println("Error accessing ATALANTA table: " + e);
        return Status.ERROR;
      }
    }

    try {
      if (debug) {
        System.out.println("Doing delete for key: " + key);
      }

      if (batchSize > 1) {
        List<Delete> deletes = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
          deletes.add(new Delete(XxHash32KeySalter.saltKey((key + "_" + i).getBytes())));
        }
        currentTable.delete(deletes);
      } else {
        currentTable.delete(new Delete(XxHash32KeySalter.saltKey((key.getBytes()))));
      }

      return Status.OK;
    } catch (IOException | RuntimeException e) {
      if (debug) {
        System.err.println("Error doing delete: " + e);
      }
      return Status.ERROR;
    }
  }

  // Only non-private for testing.
  void setConfiguration(final AtalantaConfiguration newConfig) {
    this.config = newConfig;
  }

  private boolean isBooleanParamSet(String param, boolean defaultValue) {
    return Boolean.parseBoolean(getProperties().getProperty(param, Boolean.toString(defaultValue)));
  }

}

/*
 * For customized vim control set autoindent set si set shiftwidth=4
 */
