package org.tatuaua.grugtsdb;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.tatuaua.grugtsdb.model.GrugBucketMetadata;
import org.tatuaua.grugtsdb.model.GrugField;

import org.apache.commons.io.FileUtils;
import org.tatuaua.grugtsdb.model.GrugReadResponse;

import java.io.*;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DB {
    private static final File DIR = new File("grug_tsdb");
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Map<String, GrugBucketMetadata> BUCKET_METADATA_MAP = new HashMap<>();

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

    public static void createBucket(String bucketName, List<GrugField> fields) throws IOException {
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

        GrugBucketMetadata metadata = null;//new GrugBucketMetadata(dos, raf, calculateRecordSize(fields), 0, fields);

        System.out.println("created bucket with record size: " + metadata.getRecordSize());
        writeBucketMetadata(bucketName, metadata);
        BUCKET_METADATA_MAP.put(bucketName, metadata);
    }

    private static void writeBucketMetadata(String bucketName, GrugBucketMetadata metadata) throws IOException {
        File metadataFile = new File(DIR, bucketName + "_meta.json");
        try (FileOutputStream fos = new FileOutputStream(metadataFile)) {
            String metadataJson = MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(metadata);
            fos.write(metadataJson.getBytes());
        }
    }

    public static void writeToBucket(String bucketName, Map<String, Object> fieldValues) throws IOException {
        GrugBucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        DataOutputStream dos = metadata.getDos();

        for (GrugField field : metadata.getFields()) {
            Object value = fieldValues.get(field.getName());
            if (value == null) {
                throw new IOException("Missing required field: " + field.getName());
            }

            switch (field.getType()) {
                case INT:
                    dos.writeInt((int) value);
                    break;
                case BOOLEAN:
                    dos.writeBoolean((boolean) value);
                    break;
                case FLOAT:
                    dos.writeFloat((float) value);
                    break;
                case STRING:
                    dos.write(Utils.stringToByteArray((String) value, field.getSize()));
                    break;
                default:
                    throw new IOException("Unsupported field type: " + field.getType());
            }
        }

        metadata.setRecordAmount(metadata.getRecordAmount() + 1);
    }

    public static GrugReadResponse readMostRecent(String bucketName) throws IOException {
        GrugBucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<GrugField> fields = metadata.getFields();

        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        long lastRecordPosition = (metadata.getRecordAmount() - 1) * metadata.getRecordSize();

        metadata.getRaf().seek(lastRecordPosition);

        Map<String, Object> record = new HashMap<>();
        for (GrugField field : fields) {
            switch (field.getType()) {
                case INT:
                    record.put(field.getName(), metadata.getRaf().readInt());
                    break;
                case BOOLEAN:
                    record.put(field.getName(), metadata.getRaf().readBoolean());
                    break;
                case FLOAT:
                    record.put(field.getName(), metadata.getRaf().readFloat());
                    break;
                case STRING:
                    byte[] buffer = new byte[field.getSize()];
                    metadata.getRaf().readFully(buffer);
                    record.put(field.getName(), Utils.byteArrayToString(buffer));
                    break;
                default:
                    throw new IOException("Unsupported field type: " + field.getType());
            }
        }
        metadata.getRaf().seek(0); // resets the raf pointer to start of file
        return new GrugReadResponse(record);
    }

    // TODO: pagination
    public static List<GrugReadResponse> readAll(String bucketName) throws IOException {
        GrugBucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<GrugField> fields = metadata.getFields();

        if (metadata.getRecordAmount() < 1) {
            throw new IOException("Tried to read empty bucket");
        }

        long position = 0;
        int index = 0;
        metadata.getRaf().seek(position); // make sure we start at beginning of file
        List<GrugReadResponse> responses = new ArrayList<>();

        while(position < metadata.getRecordSize() * metadata.getRecordAmount()) {
            GrugReadResponse response = new GrugReadResponse(new HashMap<>());
            responses.add(index, response);

            for (GrugField field : fields) {
                switch (field.getType()) {
                    case INT:
                        response.getData().put(field.getName(), metadata.getRaf().readInt());
                        break;
                    case BOOLEAN:
                        response.getData().put(field.getName(), metadata.getRaf().readBoolean());
                        break;
                    case FLOAT:
                        response.getData().put(field.getName(), metadata.getRaf().readFloat());
                        break;
                    case STRING:
                        byte[] buffer = new byte[field.getSize()];
                        metadata.getRaf().readFully(buffer);
                        response.getData().put(field.getName(), Utils.byteArrayToString(buffer));
                        break;
                    default:
                        throw new IOException("Unsupported field type: " + field.getType());
                }
            }
            position += metadata.getRecordSize();
            index++;
        }

        metadata.getRaf().seek(0); // resets the raf pointer to start of file
        return responses;
    }

    /*public static void printBucketContents(String bucketName) throws IOException {
        System.out.println("Printing bucket contents");
        GrugBucketMetadata metadata = BUCKET_METADATA_MAP.get(bucketName);
        List<GrugField> fields = metadata.getFields();
        File file = new File(DIR, bucketName + ".grug");

        try (DataInputStream dis = new DataInputStream(new FileInputStream(file))) {
            long fileSize = file.length();
            long recordSize = metadata.getRecordSize();
            long numRecords = fileSize / recordSize;

            for (long i = 0; i < numRecords; i++) {
                Map<String, Object> record = new HashMap<>();
                for (GrugField field : fields) {
                    switch (field.getType()) {
                        case INT:
                            record.put(field.getName(), dis.readInt());
                            break;
                        case BOOLEAN:
                            record.put(field.getName(), dis.readBoolean());
                            break;
                        case FLOAT:
                            record.put(field.getName(), dis.readFloat());
                            break;
                        case STRING:
                            record.put(field.getName(), Utils.byteArrayToString(dis.readNBytes(field.getSize())));
                            break;
                        default:
                            throw new IOException("Unsupported field type: " + field.getType());
                    }
                }
                System.out.println("Record " + i + ": " + record);
            }
        }
    }*/

    private static long calculateRecordSize(List<GrugField> fields) throws IOException {
        long recordSize = 0;
        for (GrugField field : fields) {
            switch (field.getType()) {
                case INT:
                    recordSize += 4;
                    break;
                case BOOLEAN:
                    recordSize += 1;
                    break;
                case FLOAT:
                    recordSize += 4;
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
