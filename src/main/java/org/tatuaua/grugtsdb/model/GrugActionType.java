package org.tatuaua.grugtsdb.model;

public enum GrugActionType {
    CREATE_BUCKET,
    WRITE,
    READ;

    public static GrugActionType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("Action type cannot be null");
        }
        return switch (value) {
            case "createBucket" -> CREATE_BUCKET;
            case "write" -> WRITE;
            case "read" -> READ;
            default -> throw new IllegalArgumentException("Invalid action type");
        };
    }
}