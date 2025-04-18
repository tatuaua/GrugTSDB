package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.BucketMetadata;
import org.tatuaua.grugtsdb.model.Field;

import org.apache.commons.io.FileUtils;
import org.tatuaua.grugtsdb.model.ReadResponse;

import java.io.*;
import java.nio.file.Files;
import java.util.*;

public class DB {
    public static final File DIR = new File("grug_tsdb");
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final Map<String, BucketMetadata> BUCKET_METADATA_MAP = new HashMap<>();

    static {
        try {
            if (DIR.exists()) {
                FileUtils.deleteDirectory(DIR);
            }
            Files.createDirectories(DIR.toPath());
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize database directory", e);
        }
    }

    public static void createBucket(String bucketName, List<Field> fields) throws IOException {
        File bucketFile = new File(DIR, bucketName + ".grug");
        if (!bucketFile.createNewFile()) {
            throw new IOException("Bucket '" + bucketName + "' already exists");
        }

        DataOutputStream dos = new DataOutputStream(
                new FileOutputStream(bucketFile, true)
        );

        RandomAccessFile raf = new RandomAccessFile(
                bucketFile, "r"
        );

        BucketMetadata metadata = new BucketMetadata(dos, raf, calculateRecordSize(fields), 0, fields);

        writeBucketMetadata(bucketName, metadata);
        BUCKET_METADATA_MAP.put(bucketName, metadata);
    }

    private static void writeBucketMetadata(String bucketName, BucketMetadata metadata) throws IOException {
        File metadataFile = new File(DIR, bucketName + "_meta.json");
        try (FileOutputStream fos = new FileOutputStream(metadataFile)) {
            String metadataJson = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);
            fos.write(metadataJson.getBytes());
        }
    }

    public static void writeToBucket(String bucketName, Map<String, Object> fieldValues) throws IOException {
        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        DataOutputStream dos = metadata.getDos();

        Object[] values = new Object[metadata.getFields().size()];

        // Need to first check if any field is null so we don't write partial records before throwing
        for(int i = 0; i < metadata.getFields().size(); i++) {
            Field field = metadata.getFields().get(i);
            Object value = fieldValues.get(field.getName());
            if(Objects.isNull(value)) {
                throw new IOException("Missing required field: " + field.getName());
            }
            values[i] = value;
        }

        for (int i = 0; i < metadata.getFields().size(); i++) {
            switch (metadata.getFields().get(i).getType()) {
                case INT:
                    dos.writeInt((int) values[i]);
                    break;
                case BOOLEAN:
                    dos.writeBoolean((boolean) values[i]);
                    break;
                case DOUBLE:
                    dos.writeDouble((Double) values[i]);
                    break;
                case STRING:
                    dos.write(Utils.stringToByteArray((String) values[i], metadata.getFields().get(i).getSize()));
                    break;
                case LONG:
                    dos.writeLong((long) values[i]);
                    break;
                default:
                    throw new IOException("Unsupported field type: " + metadata.getFields().get(i).getType());
            }
        }

        metadata.setRecordAmount(metadata.getRecordAmount() + 1);
    }

    public static ReadResponse readMostRecent(String bucketName) throws IOException {
        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<Field> fields = metadata.getFields();

        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        long lastRecordPosition = (metadata.getRecordAmount() - 1) * metadata.getRecordSize();

        metadata.getRaf().seek(lastRecordPosition);

        Map<String, Object> record = new HashMap<>();
        for (Field field : fields) {
            switch (field.getType()) {
                case INT:
                    record.put(field.getName(), metadata.getRaf().readInt());
                    break;
                case BOOLEAN:
                    record.put(field.getName(), metadata.getRaf().readBoolean());
                    break;
                case DOUBLE:
                    record.put(field.getName(), metadata.getRaf().readDouble());
                    break;
                case STRING:
                    byte[] buffer = new byte[field.getSize()];
                    metadata.getRaf().readFully(buffer);
                    record.put(field.getName(), Utils.byteArrayToString(buffer));
                    break;
                case LONG:
                    record.put(field.getName(), metadata.getRaf().readLong());
                    break;
                default:
                    throw new IOException("Unsupported field type: " + field.getType());
            }
        }
        metadata.getRaf().seek(0); // resets the raf pointer to start of file
        return new ReadResponse(record);
    }

    // TODO: pagination
    public static List<ReadResponse> readAll(String bucketName) throws IOException {
        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<Field> fields = metadata.getFields();

        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        long position = 0;
        int index = 0;
        metadata.getRaf().seek(position); // make sure we start at beginning of file
        List<ReadResponse> responses = new ArrayList<>();

        while(position < metadata.getRecordSize() * metadata.getRecordAmount()) {
            ReadResponse response = new ReadResponse(new HashMap<>());
            responses.add(index, response);

            for (Field field : fields) {
                readField(metadata, response, field);
            }
            position += metadata.getRecordSize();
            index++;
        }

        metadata.getRaf().seek(0); // resets the raf pointer to start of file
        return responses;
    }

    public static List<ReadResponse> readInTimeRange(String bucketName, long start, long end) throws IOException {
        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<Field> fields = metadata.getFields();

        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        long position = 0;
        int index = 0;
        metadata.getRaf().seek(position); // make sure we start at beginning of file
        List<ReadResponse> responses = new ArrayList<>();

        while(position < metadata.getRecordSize() * metadata.getRecordAmount()) {

            boolean inRange = true;
            ReadResponse response = new ReadResponse(new HashMap<>());

            for (Field field : fields) {

                if(field.getName().equals("timestamp")) {
                    long timestamp = metadata.getRaf().readLong();
                    metadata.getRaf().seek(metadata.getRaf().getFilePointer() - Long.BYTES);
                    if(timestamp < start || timestamp > end) {
                        inRange = false;
                        break;
                    }
                }

                readField(metadata, response, field);
            }

            if(inRange) {
                responses.add(index, response);
                index++;
            }
            position += metadata.getRecordSize();
        }

        metadata.getRaf().seek(0); // resets the raf pointer to start of file
        return responses;
    }

    private static void readField(BucketMetadata metadata, ReadResponse response, Field field) throws IOException {
        switch (field.getType()) {
            case INT:
                response.getData().put(field.getName(), metadata.getRaf().readInt());
                break;
            case BOOLEAN:
                response.getData().put(field.getName(), metadata.getRaf().readBoolean());
                break;
            case DOUBLE:
                response.getData().put(field.getName(), metadata.getRaf().readDouble());
                break;
            case STRING:
                byte[] buffer = new byte[field.getSize()];
                metadata.getRaf().readFully(buffer);
                response.getData().put(field.getName(), Utils.byteArrayToString(buffer));
                break;
            case LONG:
                response.getData().put(field.getName(), metadata.getRaf().readLong());
                break;
            default:
                throw new IOException("Unsupported field type: " + field.getType());
        }
    }

    private static long calculateRecordSize(List<Field> fields) throws IOException {
        long recordSize = 0;
        for (Field field : fields) {
            switch (field.getType()) {
                case INT:
                    recordSize += Integer.BYTES;
                    break;
                case BOOLEAN:
                    recordSize += Byte.BYTES;
                    break;
                case DOUBLE:
                    recordSize += Double.BYTES;
                    break;
                case LONG:
                    recordSize += Long.BYTES;
                    break;
                case STRING:
                    recordSize += field.getSize();
                    break;
                default:
                    throw new IOException("Unsupported field type: " + field.getType());
            }
        }

        return recordSize;
    }
}
