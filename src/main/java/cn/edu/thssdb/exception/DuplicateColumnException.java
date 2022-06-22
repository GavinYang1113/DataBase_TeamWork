package cn.edu.thssdb.exception;

public class DuplicateColumnException extends RuntimeException {
    String column_name = "";

    public DuplicateColumnException(String name) {
        column_name = name;
    }

    @Override
    public String getMessage() {
        return "Exception: column " + column_name + " duplicates!";
    }
}
