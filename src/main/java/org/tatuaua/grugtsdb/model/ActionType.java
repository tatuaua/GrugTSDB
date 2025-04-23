package org.tatuaua.grugtsdb.model;

public enum ActionType {
    CREATE_BUCKET,
    WRITE,
    READ,
    CREATE_STREAM;

    public static ActionType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Action type cannot be null");
        }
        return switch (value) {
            case "createBucket" -> CREATE_BUCKET;
            case "write" -> WRITE;
            case "read" -> READ;
            case "createStream" -> CREATE_STREAM;
            default -> throw new IllegalArgumentException("Invalid action type");
        };
    }

    public static String getResponseMessage(ActionType actionType, String bucketName) {
        return switch (actionType) {
            case CREATE_BUCKET -> "Bucket created successfully: " + bucketName;
            case WRITE -> "Data written to bucket: " + bucketName;
            case CREATE_STREAM -> "Stream started for buckets: " + bucketName;
            default -> throw new IllegalArgumentException("Invalid action type for response message");
        };
    }
}