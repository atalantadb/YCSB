/**
 * Copyright (c) 2015-2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.db;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.BatchWritePolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://www.aerospike.com/">Areospike</a>.
 */
public class AerospikeClient extends site.ycsb.DB {
  private static final String DEFAULT_HOST = "localhost";
  private static final String DEFAULT_PORT = "3000";
  private static final String DEFAULT_TIMEOUT = "10000";
  private static final String DEFAULT_NAMESPACE = "ycsb";

  private String namespace = null;

  private static final Object CLIENT_LOCK = new Object();
  private static IAerospikeClient sharedClient = null;
  private static int clientUsers = 0;

  private IAerospikeClient client = null;

  private Policy readPolicy;

  private WritePolicy insertPolicy;
  private WritePolicy updatePolicy;
  private WritePolicy deletePolicy;

  private BatchPolicy batchReadPolicy;
  private BatchPolicy batchWritePolicy;

  private BatchWritePolicy insertBatchPolicy;
  private BatchWritePolicy updateBatchPolicy;
  private BatchWritePolicy deleteBatchPolicy;

  private int batchSize = 1;


  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    namespace = props.getProperty("as.namespace", props.getProperty("namespace", DEFAULT_NAMESPACE));

    if (namespace.trim().isEmpty()) {
      throw new DBException("Aerospike namespace must not be empty.");
    }

    try {
      batchSize = Integer.parseInt(props.getProperty("batchsize", "1"));
    } catch (NumberFormatException e) {
      throw new DBException("batchsize must be a positive integer.", e);
    }
    if (batchSize <= 0) {
      throw new DBException("batchsize must be greater than zero.");
    }

    String host = props.getProperty("as.host", DEFAULT_HOST);
    String user = props.getProperty("as.user");
    String password = props.getProperty("as.password");

    int port = Integer.parseInt(props.getProperty("as.port", DEFAULT_PORT));

    String timeout = props.getProperty("as.timeout", DEFAULT_TIMEOUT);
    int singleSocketTimeout = Integer.parseInt(props.getProperty("as.single.socketTimeout", timeout));
    int singleTotalTimeout = Integer.parseInt(props.getProperty("as.single.totalTimeout", timeout));
    int batchSocketTimeout = Integer.parseInt(props.getProperty("as.batch.socketTimeout", timeout));
    int batchTotalTimeout = Integer.parseInt(props.getProperty("as.batch.totalTimeout", timeout));
    int timeoutDelay = Integer.parseInt(props.getProperty("as.timeoutDelay", "3000"));

    readPolicy = new Policy();

    readPolicy.setTimeouts(singleSocketTimeout, singleTotalTimeout);

    readPolicy.maxRetries = Integer.parseInt(props.getProperty("as.read.maxRetries", "1"));

    readPolicy.timeoutDelay = timeoutDelay;

    insertPolicy = new WritePolicy();
    updatePolicy = new WritePolicy();
    deletePolicy = new WritePolicy();

    configureSingleWritePolicy(insertPolicy, singleSocketTimeout, singleTotalTimeout, timeoutDelay);

    configureSingleWritePolicy(updatePolicy, singleSocketTimeout, singleTotalTimeout, timeoutDelay);

    configureSingleWritePolicy(deletePolicy, singleSocketTimeout, singleTotalTimeout, timeoutDelay);


    insertPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updatePolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

    batchReadPolicy = new BatchPolicy();

    batchReadPolicy.setTimeouts(batchSocketTimeout, batchTotalTimeout);

    batchReadPolicy.maxRetries = Integer.parseInt(props.getProperty("as.batch.read.maxRetries", "0"));

    batchReadPolicy.timeoutDelay = timeoutDelay;
    batchWritePolicy = new BatchPolicy();

    batchWritePolicy.setTimeouts(batchSocketTimeout, batchTotalTimeout);

    batchWritePolicy.maxRetries = Integer.parseInt(props.getProperty("as.batch.write.maxRetries", "0"));

    batchWritePolicy.timeoutDelay = timeoutDelay;

    insertBatchPolicy = new BatchWritePolicy();
    updateBatchPolicy = new BatchWritePolicy();
    deleteBatchPolicy = new BatchWritePolicy();

    insertBatchPolicy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
    updateBatchPolicy.recordExistsAction = RecordExistsAction.UPDATE_ONLY;

    ClientPolicy clientPolicy = new ClientPolicy();

    clientPolicy.maxConnsPerNode = Integer.parseInt(props.getProperty("as.maxConnsPerNode", "500"));

    clientPolicy.connPoolsPerNode = Integer.parseInt(props.getProperty("as.connPoolsPerNode", "4"));


    if (user != null && password != null) {
      clientPolicy.user = user;
      clientPolicy.password = password;
    }

    try {
      synchronized (CLIENT_LOCK) {
        if (sharedClient == null) {
          sharedClient = new com.aerospike.client.AerospikeClient(clientPolicy, host, port);
        }

        client = sharedClient;
        clientUsers++;
      }
    } catch (AerospikeException e) {
      throw new DBException(String.format("Error while creating Aerospike client for %s:%d.", host, port), e);
    }
  }

  private void configureSingleWritePolicy(
      WritePolicy policy,
      int socketTimeout,
      int totalTimeout,
      int timeoutDelay) {

    policy.setTimeouts(
        socketTimeout,
        totalTimeout
    );

    policy.maxRetries = 0;

    policy.timeoutDelay = timeoutDelay;
  }

  @Override
  public void cleanup() throws DBException {
    synchronized (CLIENT_LOCK) {
      if (client != null) {
        clientUsers--;
        client = null;

        if (clientUsers == 0 && sharedClient != null) {
          sharedClient.close();
          sharedClient = null;
        }
      }
    }
  }

  private Key createKey(String table, String key) {
    return new Key(namespace, table, key);
  }

  private void populateResult(Record record, Map<String, ByteIterator> result) {
    if (record.bins != null) {
      for (Map.Entry<String, Object> entry : record.bins.entrySet()) {
        result.put(entry.getKey(), new ByteArrayByteIterator((byte[]) entry.getValue()));
      }
    }
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
    try {
      if (batchSize == 1) {
        Key keyObj = createKey(table, key);
        Record record = fields != null
            ? client.get(readPolicy, keyObj, fields.toArray(new String[0]))
            : client.get(readPolicy, keyObj);

        if (record == null) {
          System.err.println("Record key " + key + " not found");
          return Status.ERROR;
        }

        populateResult(record, result);
        return Status.OK;
      }

      Key[] keys = new Key[batchSize];
      for (int i = 0; i < batchSize; i++) {
        keys[i] = createKey(table, key + "_" + i);
      }

      Record[] records = fields != null
          ? client.get(batchReadPolicy, keys, fields.toArray(new String[0]))
          : client.get(batchReadPolicy, keys);

      for (Record record : records) {
        if (record == null) {
          System.err.println("Record for batch key " + key + " not found");
          return Status.ERROR;
        }
      }

      // YCSB exposes one result map; batch writes use the same values for every key.
      populateResult(records[0], result);
      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while reading key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String table, String start, int count, Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> result) {
    System.err.println("Scan not implemented");
    return Status.ERROR;
  }

  private Status write(String table, String key, WritePolicy writePolicy, Map<String, ByteIterator> values) {

    Bin[] bins = new Bin[values.size()];
    int index = 0;

    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      bins[index++] = new Bin(entry.getKey(), entry.getValue().toArray());
    }

    if (batchSize > 1) {
      BatchWritePolicy recordBatchPolicy;

      if (writePolicy == insertPolicy) {
        recordBatchPolicy = insertBatchPolicy;
      } else if (writePolicy == updatePolicy) {
        recordBatchPolicy = updateBatchPolicy;
      } else {
        System.err.println("Unsupported WritePolicy for batch write, key " + key);
        return Status.ERROR;
      }

      Operation[] operations = new Operation[bins.length];

      for (int i = 0; i < bins.length; i++) {
        operations[i] = Operation.put(bins[i]);
      }

      List<BatchRecord> records = new ArrayList<BatchRecord>(batchSize);

      for (int i = 0; i < batchSize; i++) {
        records.add(new BatchWrite(recordBatchPolicy, createKey(table, key + "_" + i), operations));
      }

      try {
        boolean success = client.operate(batchWritePolicy, records);

        if (!success) {
          System.err.println("Error while batch writing key " + key);
          return Status.ERROR;
        }

        return Status.OK;

      } catch (AerospikeException e) {
        System.err.println("Error while batch writing key " + key + ": " + e);
        return Status.ERROR;
      }
    }

    Key keyObj = createKey(table, key);

    try {
      client.put(writePolicy, keyObj, bins);
      return Status.OK;

    } catch (AerospikeException e) {
      System.err.println("Error while writing key " + key + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, updatePolicy, values);
  }

  @Override
  public Status insert(String table, String key,
                       Map<String, ByteIterator> values) {
    return write(table, key, insertPolicy, values);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      if (batchSize > 1) {
        List<BatchRecord> records = new ArrayList<BatchRecord>(batchSize);
        Operation[] operations = new Operation[]{Operation.delete()};

        for (int i = 0; i < batchSize; i++) {
          records.add(new BatchWrite(deleteBatchPolicy, createKey(table, key + "_" + i), operations));
        }

        if (!client.operate(batchWritePolicy, records)) {
          System.err.println("Error while batch deleting key " + key);
          return Status.ERROR;
        }

        return Status.OK;
      }

      if (!client.delete(deletePolicy, createKey(table, key))) {
        System.err.println("Record key " + key + " not found (delete)");
        return Status.ERROR;
      }

      return Status.OK;
    } catch (AerospikeException e) {
      System.err.println("Error while deleting key " + key + ": " + e);
      return Status.ERROR;
    }
  }
}
