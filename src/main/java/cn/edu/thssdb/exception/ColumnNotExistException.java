package cn.edu.thssdb.exception;

public class ColumnNotExistException extends RuntimeException {
    String column_name="";
    public ColumnNotExistException(String name){
        column_name=name;
    }
    @Override
    public String getMessage() {
        return "Exception: column "+column_name+" doesn't exist!";
    }
}
