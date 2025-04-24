package org.tatuaua.grugtsdb.engine;

import lombok.extern.slf4j.Slf4j;

import org.tatuaua.grugtsdb.Utils;
import org.tatuaua.grugtsdb.engine.model.BucketMetadata;
import org.tatuaua.grugtsdb.engine.model.Field;
import org.tatuaua.grugtsdb.engine.model.FieldType;
import org.tatuaua.grugtsdb.engine.model.ReadResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.*;
import java.util.*;

@Slf4j
public class Engine {
    public static final ObjectMapper MAPPER = new ObjectMapper();
    public static final File DIR = new File("grug_tsdb");
    public static Map<String, BucketMetadata> BUCKET_METADATA_MAP = new HashMap<>();

    public static void generateMetadata() {
        for(BucketMetadata metadata : Utils.readBucketMetadata(DIR)) {
            try {
                createBucket(metadata.getName(), metadata.getFields());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void clearMetadata() {
        BUCKET_METADATA_MAP = new HashMap<>();
    }

    public static void clearDatabase() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(DIR.toPath())) {
            for (Path file : stream) {
                try {
                    Files.delete(file);
                    System.out.println("Deleted file: " + file.getFileName());
                } catch (IOException e) {
                    System.err.println("Failed to delete file: " + file.getFileName() + " - " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Error accessing directory: " + e.getMessage());
        }
        BUCKET_METADATA_MAP = new HashMap<>();
    }

    public static void createBucket(String bucketName, List<Field> fields) throws IOException {
        File bucketFile = new File(DIR, bucketName + ".grug");

        if (!DIR.exists()) {
            DIR.mkdir();
        }
        
        if (!bucketFile.createNewFile()) {
            log.info("File for bucket {} already exists", bucketName);
        }

        DataOutputStream dos = new DataOutputStream(
                new FileOutputStream(bucketFile, true)
        );

        RandomAccessFile raf = new RandomAccessFile(
                bucketFile, "r"
        );

        long recordSize = calculateRecordSize(fields);
        long recordAmount = bucketFile.length() / recordSize;

        BucketMetadata metadata = new BucketMetadata(dos, raf, recordSize, recordAmount, bucketName, fields);

        writeBucketMetadata(bucketName, metadata);
        BUCKET_METADATA_MAP.put(bucketName, metadata);
    }

    private static void writeBucketMetadata(String bucketName, BucketMetadata metadata) throws IOException {
        File metadataFile = new File(DIR, bucketName + ".grug_meta");
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
                case INT -> dos.writeInt((int) values[i]);
                case BOOLEAN -> dos.writeBoolean((boolean) values[i]);
                case DOUBLE -> dos.writeDouble((Double) values[i]);
                case STRING -> dos.write(Utils.stringToByteArray((String) values[i], metadata.getFields().get(i).getSize()));
                case LONG -> dos.writeLong((long) values[i]);
                default -> throw new IOException("Unsupported field type: " + metadata.getFields().get(i).getType());
            }
        }

        metadata.setRecordAmount(metadata.getRecordAmount() + 1);
    }

    public static ReadResponse readMostRecent(String bucketName) throws IOException {
        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        List<Field> fields = metadata.getFields();

        long lastRecordPosition = (metadata.getRecordAmount() - 1) * metadata.getRecordSize();

        metadata.getRaf().seek(lastRecordPosition);

        Map<String, Object> record = new HashMap<>();
        for (Field field : fields) {
            switch (field.getType()) {
                case INT -> record.put(field.getName(), metadata.getRaf().readInt());
                case BOOLEAN -> record.put(field.getName(), metadata.getRaf().readBoolean());
                case DOUBLE -> record.put(field.getName(), metadata.getRaf().readDouble());
                case STRING -> {
                    byte[] buffer = new byte[field.getSize()];
                    metadata.getRaf().readFully(buffer);
                    record.put(field.getName(), Utils.byteArrayToString(buffer));
                }
                case LONG -> record.put(field.getName(), metadata.getRaf().readLong());
                default -> throw new IOException("Unsupported field type: " + field.getType());
            }
        }
        metadata.getRaf().seek(0); // resets the raf pointer to start of file
        return new ReadResponse(record);
    }

    // TODO: pagination
    public static List<ReadResponse> readAll(String bucketName) throws IOException {
        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        List<Field> fields = metadata.getFields();

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
        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        List<Field> fields = metadata.getFields();

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

    public static ReadResponse aggregateRead(String bucketName, long start, long end, String fieldName, String operation) throws IOException {

        if (!(
            operation.equals("sum") 
            || operation.equals("avg") 
            || operation.equals("min") 
            || operation.equals("max"))) {

            throw new IllegalArgumentException("Unsupported operation: " + operation);
        }

        if (start > end) {
            throw new IllegalArgumentException("Start time must be less than or equal to end time");
        }

        BucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        List<Field> fields = metadata.getFields();
        Field targetField = null;
        for (Field field : fields) {
            if (field.getName().equals(fieldName)) {
                targetField = field;
                break;
            }
        }

        if (targetField == null) {
            throw new IllegalArgumentException("Field '" + fieldName + "' does not exist in bucket '" + bucketName + "'");
        }

        if (!(targetField.getType() == FieldType.INT || targetField.getType() == FieldType.DOUBLE || targetField.getType() == FieldType.LONG)) {
            throw new IllegalArgumentException("Cannot calculate " + operation + " of field type: " + targetField.getType());
        }

        List<ReadResponse> records = readInTimeRange(bucketName, start, end);

        if (records.isEmpty()) {
            throw new IOException("No records to calculate " + operation + " on");
        }

        double result = 0;
        boolean firstValue = true;

        for (ReadResponse record : records) {
            Object value = record.getData().get(fieldName);
            if (value != null) {
                double numericValue;
                switch (targetField.getType()) {
                    case INT -> numericValue = (Integer) value;
                    case DOUBLE -> numericValue = (Double) value;
                    case LONG -> numericValue = (Long) value;
                    default -> throw new IllegalStateException("Unexpected field type during " + operation + " calculation: " + targetField.getType());
                }

                switch (operation) {
                    case "sum" -> result += numericValue;
                    case "avg" -> result += numericValue;
                    case "min" -> {
                        if (firstValue || numericValue < result) {
                            result = numericValue;
                            firstValue = false;
                        }
                    }
                    case "max" -> {
                        if (firstValue || numericValue > result) {
                            result = numericValue;
                            firstValue = false;
                        }
                    }
                }
            }
        }

        if (operation.equalsIgnoreCase("avg")) {
            result /= records.size();
        }

        return new ReadResponse(Map.of(fieldName + "_" + operation, result));
    }
    
    private static void readField(BucketMetadata metadata, ReadResponse response, Field field) throws IOException {
        switch (field.getType()) {
            case INT -> response.getData().put(field.getName(), metadata.getRaf().readInt());
            case BOOLEAN -> response.getData().put(field.getName(), metadata.getRaf().readBoolean());
            case DOUBLE -> response.getData().put(field.getName(), metadata.getRaf().readDouble());
            case STRING -> {
                byte[] buffer = new byte[field.getSize()];
                metadata.getRaf().readFully(buffer);
                response.getData().put(field.getName(), Utils.byteArrayToString(buffer));
            }
            case LONG -> response.getData().put(field.getName(), metadata.getRaf().readLong());
            default -> throw new IOException("Unsupported field type: " + field.getType());
        }
    }

    private static long calculateRecordSize(List<Field> fields) throws IOException {
        long recordSize = 0;
        for (Field field : fields) {
            switch (field.getType()) {
                case INT -> recordSize += Integer.BYTES;
                case BOOLEAN -> recordSize += Byte.BYTES;
                case DOUBLE, LONG -> recordSize += Double.BYTES;
                case STRING -> recordSize += field.getSize();
                default -> throw new IOException("Unsupported field type: " + field.getType());
            }
        }

        return recordSize;
    }
}