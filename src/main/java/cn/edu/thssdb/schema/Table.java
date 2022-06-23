package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.common.Pair;
import sun.misc.Lock;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.type.ColumnType.STRING;


// TODO lock control, variables init.

public class Table implements Iterable<Row> {
    private final static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Cell, Row> index;
    private int primaryIndex;

    Lock m_lock = new Lock();
    ArrayList<Long> sLockSessions = new ArrayList<Long>();
    ArrayList<Long> xLockSessions = new ArrayList<Long>();

    public void takeSLock(Long sessionId, Manager manager) {
        try {
            m_lock.lock();
            if (sLockSessions.contains(sessionId)) {
                return;
            }
            if (xLockSessions.size() == 1 && xLockSessions.get(0) == sessionId) {
                return;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            m_lock.unlock();
        }

        try {
            while (true) {
                m_lock.lock();
                if (xLockSessions.size() == 0) {
                    sLockSessions.add(sessionId);
                    manager.x_lockDict.get(sessionId).add(tableName);
                    m_lock.unlock();
                    return;
                }
                m_lock.unlock();
                Thread.sleep(500);
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return;
    }
    //需要获取共享锁
    //但存在互斥锁
//        while (true) {
//            try {
//                m_lock.lock();
//                if (xLockSessions.size() == 0) {
//                    sLockSessions.add(sessionId);
//                    manager.x_lockDict.get(sessionId).add(tableName);
//                    m_lock.unlock();
//                    return;
//                }
//                m_lock.unlock();
//                Thread.sleep(500);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//      }

    public void releaseSLock(Long sessionId) {
        try {
            m_lock.lock();
            if (sLockSessions.contains(sessionId)) {
                sLockSessions.remove(sessionId);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            m_lock.unlock();
        }
    }

    public void takeXLock(Long sessionId, Manager manager) {
        try {
            m_lock.lock();
            if (xLockSessions.contains(sessionId)) {
                return;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            m_lock.unlock();
        }

        while (true) {
            try {
                m_lock.lock();
                if (xLockSessions.size() == 0 && sLockSessions.size() == 0) {
                    xLockSessions.add(sessionId);
                    manager.x_lockDict.get(sessionId).add(tableName);
                    m_lock.unlock();
                    return;
                }
                m_lock.unlock();
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    public void releaseXLock(Long sessionId) {
        try {
            m_lock.lock();
            if (xLockSessions.contains(sessionId)) {
                xLockSessions.remove(sessionId);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            m_lock.unlock();
        }
    }


    // Initiate: Table, recover
    public Table(String databaseName, String tableName, Column[] columns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        this.primaryIndex = -1;
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).isPrimary()) {
                if (this.primaryIndex >= 0)
                    throw new MultiPrimaryKeyException(this.tableName);
                this.primaryIndex = i;
            }
        }
        if (this.primaryIndex < 0)
            throw new MultiPrimaryKeyException(this.tableName);

        // TODO initiate lock status.

        recover();
    }

    private void recover() {
        // read from disk for recovering
        try {
            // TODO lock control
            ArrayList<Row> rowsOnDisk = deserialize();
            for (Row row : rowsOnDisk)
                this.index.put(row.getEntries().get(this.primaryIndex), row);
        } finally {
            // TODO lock control
        }
    }


    // Operations: get, insert, delete, update, dropTable, you can add other operations.
    // remember to use locks to fill the TODOs

    public Row get(Cell primaryCell) {
        try {
            // TODO lock control
            return this.index.get(primaryCell);
        } finally {
            // TODO lock control

        }
    }

    public void insert(Row row) {
        try {
            // TODO lock control
            this.checkRowValidInTable(row);
            if (this.containsRow(row))
                throw new DuplicateKeyException();
            this.index.put(row.getEntries().get(this.primaryIndex), row);
        } finally {
            // TODO lock control
        }
    }

    public void delete(Row row) {
        try {
            // TODO lock control.
            this.checkRowValidInTable(row);
            if (!this.containsRow(row))
                throw new KeyNotExistException();
            this.index.remove(row.getEntries().get(this.primaryIndex));
        } finally {
            // TODO lock control.
        }
    }

    public void update(Cell primaryCell, Row newRow) {
        try {
            // TODO lock control.
            this.checkRowValidInTable(newRow);
            Row oldRow = this.get(primaryCell);
            if (this.containsRow(newRow))
                throw new DuplicateKeyException();   // 要么删并插入，要么抛出异常
            this.index.remove(primaryCell);
            this.index.put(newRow.getEntries().get(this.primaryIndex), newRow);
        } finally {
            // TODO lock control.
        }
    }

    private void serialize() {
        try {
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " on serializing table in folder");
            File tableFile = new File(this.getTablePath());
            if (!tableFile.exists() ? !tableFile.createNewFile() : !tableFile.isFile())
                throw new FileIOException(this.getTablePath() + " on serializing table to file");
            FileOutputStream fileOutputStream = new FileOutputStream(this.getTablePath());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            for (Row row : this)
                objectOutputStream.writeObject(row);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            throw new FileIOException(this.getTablePath() + " on serializing");
        }
    }

    private ArrayList<Row> deserialize() {
        try {
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " when deserialize");
            File tableFile = new File(this.getTablePath());
            if (!tableFile.exists())
                return new ArrayList<>();
            FileInputStream fileInputStream = new FileInputStream(this.getTablePath());
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            ArrayList<Row> rowsOnDisk = new ArrayList<>();
            Object tmpObj;
            while (fileInputStream.available() > 0) {
                tmpObj = objectInputStream.readObject();
                rowsOnDisk.add((Row) tmpObj);
            }
            objectInputStream.close();
            fileInputStream.close();
            return rowsOnDisk;
        } catch (IOException e) {
            throw new FileIOException(this.getTablePath() + " when deserialize");
        } catch (ClassNotFoundException e) {
            throw new FileIOException(this.getTablePath() + " when deserialize(serialized object cannot be found)");
        }
    }

    public void persist() {
        try {
            // TODO add lock control.
            lock.writeLock().lock();
            serialize();
        } finally {
            // TODO add lock control.
            lock.writeLock().unlock();
        }
    }

    public void dropTable() { // remove table data file
        try {
            // TODO lock control.
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " when dropTable");
            File tableFile = new File(this.getTablePath());
            if (tableFile.exists() && !tableFile.delete())
                throw new FileIOException(this.getTablePath() + " when dropTable");
        } finally {
            // TODO lock control.
        }
    }

    // Operations involving logic expressions.

    // Operations

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Cell, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().right;
        }
    }

    public int getPrimaryIndex() {
        return this.primaryIndex;
    }

    @Override
    public Iterator<Row> iterator() {
        try {
            lock.readLock().lock();
            return new TableIterator(this);
        } finally {
            lock.readLock().unlock();
        }

    }

    private void checkRowValidInTable(Row row) {
        if (row.getEntries().size() != this.columns.size())
            throw new SchemaLengthMismatchException(this.columns.size(), row.getEntries().size(), "when check Row Valid In table");
        for (int i = 0; i < row.getEntries().size(); i++) {
            String entryValueType = row.getEntries().get(i).getValueType();
            Column column = this.columns.get(i);
            if (entryValueType.equals(Global.ENTRY_NULL)) {
                if (column.cantBeNull()) throw new NullValueException(column.getColumnName());
            } else {
                if (!entryValueType.equals(column.getColumnType().name()))
                    throw new ValueFormatInvalidException("(when check row valid in table)");
                Comparable entryValue = row.getEntries().get(i).value;
                if (entryValueType.equals(STRING.name()) && ((String) entryValue).length() > column.getMaxLength())
                    throw new ValueExceedException(column.getColumnName(), ((String) entryValue).length(), column.getMaxLength(), "(when check row valid in table)");
            }
        }
    }

    private Boolean containsRow(Row row) {
        return this.index.contains(row.getEntries().get(this.primaryIndex));
    }

    public String getTableFolderPath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "tables";
    }

    public String getTablePath() {
        return this.getTableFolderPath() + File.separator + this.tableName;
    }

    public String getTableMetaPath() {
        return this.getTablePath() + Global.META_SUFFIX;
    }

    public String toString() {
        StringBuilder s = new StringBuilder("Table " + this.tableName + ": ");
        for (Column column : this.columns) s.append("\t(").append(column.toString()).append(')');
        return s.toString() + "\n";
    }

}
