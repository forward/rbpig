package forward.pig.storage;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.pig.*;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigTextInputFormat;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.piggybank.storage.partition.PathPartitionHelper;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

import java.io.IOException;
import java.util.*;

public class HiveTableLoader extends FileInputLoadFunc implements LoadMetadata {
    private static final String SCHEMA = "schema";
    private static final Log LOG = LogFactory.getLog(HiveTableLoader.class);

    private final String hiveServer;
    private final int hivePort;
    private final String column_delimiter;
    private final String databaseName;
    private final PathPartitionHelper partitionHelper;

    private String signature;
    private LineRecordReader reader;
    private PigSplit split;

    public HiveTableLoader(String hiveServer, String hivePort) {
        this(hiveServer, hivePort, "\t", "default");
    }

    public HiveTableLoader(String hiveServer, String hivePort, String column_delimiter) {
        this(hiveServer, hivePort, column_delimiter, "default");
    }

    public HiveTableLoader(String hiveServer, String hivePort, String column_delimiter, String databaseName) {
        this.hiveServer = hiveServer;
        this.hivePort = Integer.parseInt(hivePort);
        this.column_delimiter = column_delimiter;
        this.databaseName = databaseName;
        this.partitionHelper = new PathPartitionHelper();
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
        Path[] hiveTables = FileInputFormat.getInputPaths(job);
        if(hiveTables.length != 1) {
            throw new RuntimeException("comma separated hive tables '" + location +"' are not supported.");
        }
        LOG.info("Loading Hive table from '" + hiveTables[0] + "'");
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new PigTextInputFormat(){
            @Override
            protected List<FileStatus> listStatus(JobContext jobContext) throws IOException {
                List<FileStatus> hiveTableFiles = partitionHelper.listStatus(jobContext, HiveTableLoader.class, signature);
                List<Path> tableFilePaths = new ArrayList<Path>();
                for (FileStatus hiveTableFile : hiveTableFiles) {
                    tableFilePaths.add(hiveTableFile.getPath());
                }
                LOG.debug("Loading Hive table rows from " + tableFilePaths);
                return hiveTableFiles;
            }
        };
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = (LineRecordReader)reader;
        this.split = split;
    }

    @Override
    public Tuple getNext() throws IOException {
        if (reader.nextKeyValue()) {
            Map<String, String> currentPathPartitionKeyMap = Collections.emptyMap();
            if (!getPartitionKeys().isEmpty()) {
                currentPathPartitionKeyMap = partitionHelper.getPathPartitionKeyValues(
                        ((FileSplit) split.getWrappedSplit()).getPath().toString());
            }

            ResourceSchema schema = getSchema();
            List<Object> tuple = new ArrayList<Object>();
            for (int columnIndex = 0; columnIndex < schema.fieldNames().length; columnIndex++) {
                String fieldName = schema.fieldNames()[columnIndex];
                if(currentPathPartitionKeyMap.containsKey(fieldName)) {
                    tuple.add(currentPathPartitionKeyMap.get(fieldName));
                } else {
                    tuple.add(getField(reader.getCurrentValue(), columnIndex));
                }
            }
            return TupleFactory.getInstance().newTupleNoCopy(tuple);
        } else {
            return null;
        }
    }

    private Object getField(Text text, int columnIndex) {
        String field = text.toString().split(column_delimiter, -1)[columnIndex];
        if (field.length() > 0) {
            DataByteArray value = new DataByteArray(field.getBytes());
            try {
                switch (getSchema().getFields()[columnIndex].getType()) {
                    case DataType.INTEGER:
                        return DataType.toInteger(value);
                    case DataType.LONG:
                        return DataType.toLong(value);
                    case DataType.FLOAT:
                        return DataType.toFloat(value);
                    case DataType.DOUBLE:
                        return DataType.toDouble(value);
                    default:
                        return value;
                }
            } catch (ExecException e) {
                return value;
            }
        } else {
          return null;
        }

    }

    @Override
    public void setUDFContextSignature(String signature) {
        super.setUDFContextSignature(signature);
        this.signature = signature;
    }

    private Properties getUDFContext() {
	    return UDFContext.getUDFContext().getUDFProperties(this.getClass(),new String[] { signature });
    }

    private boolean hasUDFContextProperty(String name) {
        return getUDFContext().containsKey(name);
    }

    private String getUDFContextProperty(String name) {
        return getUDFContext().getProperty(name);
    }

    private void setUDFContextProperty(String name, String value) {
        getUDFContext().setProperty(name, value);
    }

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        if(hasUDFContextProperty(SCHEMA)) {
            return getSchema();
        } else {
            String[] locations = location.split("/");
            String tableName = locations[locations.length - 1];
            List<org.apache.hadoop.hive.metastore.api.FieldSchema> hiveTable;

            TSocket socket = new TSocket(hiveServer, hivePort);
            try {
                ThriftHiveMetastore.Client client = new ThriftHiveMetastore.Client(new TBinaryProtocol(socket, true, false));
                socket.open();
                hiveTable = client.get_schema(databaseName, tableName);
            } catch (Exception e) {
                throw new RuntimeException("Failed to get schema for db: '" + databaseName + "' table: '" + tableName + "'", e);
            }  finally {
                socket.close();
            }

            Schema schema = new Schema();
            for (org.apache.hadoop.hive.metastore.api.FieldSchema hiveColumn : hiveTable) {
                byte type = findPigDataType(hiveColumn.getType());
                type = type == DataType.ERROR ? DataType.CHARARRAY : type;
                schema.add(new Schema.FieldSchema(hiveColumn.getName(), type));
            }

            LOG.info("Schema for " + databaseName + "." + tableName + " is " + schema);
            setUDFContextProperty(SCHEMA, ObjectSerializer.serialize(schema));

            return getSchema();
        }
    }

    private byte findPigDataType(String hiveType) {
        hiveType = hiveType.toLowerCase();
        if (hiveType.equals("string"))
            return DataType.CHARARRAY;
        else if (hiveType.equals("int"))
            return DataType.INTEGER;
        else if (hiveType.equals("bigint") || hiveType.equals("long"))
            return DataType.LONG;
        else if (hiveType.equals("float"))
            return DataType.FLOAT;
        else if (hiveType.equals("double"))
            return DataType.DOUBLE;
        else if (hiveType.equals("boolean"))
            return DataType.INTEGER;
        else if (hiveType.equals("byte"))
            return DataType.INTEGER;
        else
            return DataType.CHARARRAY;
    }

    private ResourceSchema getSchema() {
        try {
            return new ResourceSchema((Schema)ObjectSerializer.deserialize(getUDFContextProperty(SCHEMA)));
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize schema for signature: '" + signature + "'", e);
        }
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        if(!hasUDFContextProperty(PathPartitionHelper.PARTITION_COLUMNS)) {
            partitionHelper.setPartitionKeys(location, job.getConfiguration(), this.getClass(), signature);
            LOG.info("Hive table file '" + location + "' is partitioned by " + getUDFContextProperty(PathPartitionHelper.PARTITION_COLUMNS));
        }
        return getPartitionKeys().toArray(new String[]{});
    }

    private List<String> getPartitionKeys() {
        String partitionKeys = getUDFContextProperty(PathPartitionHelper.PARTITION_COLUMNS);
        if(partitionKeys.trim().length() == 0) {
            return Collections.emptyList();
        } else {
            return Arrays.asList(partitionKeys.split(","));
        }
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
        partitionHelper.setPartitionFilterExpression(partitionFilter.toString(), this.getClass(), signature);
    }
}