package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.common.Global;
import com.sun.org.apache.bcel.internal.Const;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


// TODO: add lock control
// TODO: complete readLog() function according to writeLog() for recovering transaction

public class Manager {
    private HashMap<String, Database> databases;
    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private static HashMap<String, ReentrantReadWriteLock> databaseLock = new HashMap<>();
    public Database currentDatabase;
    public ArrayList<Long> currentSessions;
    public ArrayList<Long> waitSessions;
    public static SQLHandler sqlHandler;
    public HashMap<Long, ArrayList<String>> x_lockDict;


    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager() {
        // TODO: init possible additional variables
        databases = new HashMap<>();
        currentDatabase = null;
        currentSessions = new ArrayList<Long>();
        sqlHandler = new SQLHandler(this);
        x_lockDict = new HashMap<>();
        File managerFolder = new File(Global.DBMS_DIR + File.separator + "data");
        if (!managerFolder.exists())
            managerFolder.mkdirs();
        this.recover();
    }

    public void deleteDatabase(String databaseName) {
        try {
            // TODO: add lock control
            if (!databases.containsKey(databaseName))
                throw new DatabaseNotExistException(databaseName);
            Database database = databases.get(databaseName);
            database.dropDatabase();
            databases.remove(databaseName);

        } finally {
            // TODO: add lock control
        }
    }

    public void switchDatabase(String databaseName) {
        try {
            // TODO: add lock control
            if (!databases.containsKey(databaseName))
                throw new DatabaseNotExistException(databaseName);
            currentDatabase = databases.get(databaseName);
        } finally {
            // TODO: add lock control
        }
    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }

    public Database getCurrentDatabase() {
        return currentDatabase;
    }

    // utils:

    // Lock example: quit current manager
    public void quit() {
        try {
            lock.writeLock().lock();
            for (Database database : databases.values())
                database.quit();
            persist();
            databases.clear();
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Database get(String databaseName) {
        try {
            // TODO: add lock control
            if (!databases.containsKey(databaseName))
                throw new DatabaseNotExistException(databaseName);
            return databases.get(databaseName);
        } finally {
            // TODO: add lock control
        }
    }

    public void createDatabaseIfNotExists(String databaseName) {
        try {
            // TODO: add lock control
            if (!databases.containsKey(databaseName))
                databases.put(databaseName, new Database(databaseName));
            if (currentDatabase == null) {
                try {
                    // TODO: add lock control
                    if (!databases.containsKey(databaseName))
                        throw new DatabaseNotExistException(databaseName);
                    currentDatabase = databases.get(databaseName);
                } finally {
                    // TODO: add lock control
                }
            }
        } finally {
            // TODO: add lock control
        }
    }

    public void persist() {
        try {
            FileOutputStream fos = new FileOutputStream(Manager.getManagerDataFilePath());
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            for (String databaseName : databases.keySet())
                writer.write(databaseName + "\n");
            writer.close();
            fos.close();
        } catch (Exception e) {
            throw new FileIOException(Manager.getManagerDataFilePath());
        }
    }

    public void persistDatabase(String databaseName) {
        try {
            // TODO: add lock control
            Database database = databases.get(databaseName);
            database.quit();
            persist();
        } finally {
            // TODO: add lock control
        }
    }


    // Log control and recover from logs.
    public void writeLog(String statement, long session) {
        String logFilename = this.currentDatabase.getDatabaseLogFilePath();
        if(!databaseLock.containsKey(this.currentDatabase.getDatabaseName())) {
            databaseLock.put(this.currentDatabase.getDatabaseName(), new ReentrantReadWriteLock());
        }
        ReentrantReadWriteLock myLock = databaseLock.get(this.currentDatabase.getDatabaseName());
        try {
            myLock.writeLock().lock();
            System.out.printf("logFilename: %s\n", logFilename);
            FileWriter writer = new FileWriter(logFilename, true);
            String logString = String.format("%d|%s\n", session, statement);
            writer.write(logString);
            System.out.printf("writeLog: %s\n", logString);
            writer.close();
        } catch (Exception e) {
            throw new FileIOException(logFilename);
        } finally {
            myLock.writeLock().unlock();
        }
    }

    // TODO: read Log in transaction to recover.
    public void readLog(String databaseName) {
        String logFilename = Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "log";
        File file = new File(logFilename);
        // 检查对应文件是否正确
        if(!file.exists() || !file.isFile()) {
            System.out.printf("readLog: something about the file %s went wrong\n", logFilename);
            return;
        }

        System.out.printf("reading log of database %s from %s\n", databaseName, logFilename);
        System.out.println("Database log file size: " + file.length() + " Byte");
        System.out.println("Read WAL log to recover database.");
        // 检查database专用读写锁是否存在，若不存在则创建一个
        if(!databaseLock.containsKey(databaseName)) {
            databaseLock.put(databaseName, new ReentrantReadWriteLock());
        }
        ReentrantReadWriteLock myLock = databaseLock.get(databaseName);
        try {
            // 上锁
            myLock.readLock().lock();
            InputStreamReader reader = new InputStreamReader(new FileInputStream(file));
            BufferedReader bufferedReader = new BufferedReader(reader);

            String line;
            ArrayList<String> lines = new ArrayList<>();
            // 记录每个session的最后一次commit
            HashMap<String, Integer>lastCommitOfSession = new HashMap<>();
            int index = 0;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                String[] contents = line.split("\\|");
                String session = contents[0];
                String command = contents[1];
                System.out.printf("session: %s, command: %s\n", session, command);
                if (command.equals("commit")) {
                    System.out.printf("commit here\n");
                    lastCommitOfSession.put(session, index);
                    System.out.printf("last commit of session %s is %d\n",session, lastCommitOfSession.containsKey(session)?lastCommitOfSession.get(session):-1);
                }
                lines.add(line);
                index++;
            }
            System.out.println("read over");
            sqlHandler.evaluate("begin transaction", 114514);
            sqlHandler.evaluate("use " + databaseName, 114514);
            sqlHandler.evaluate("commit", 114514);
            for(int i = 0; i < lines.size();++i) {
                String[] contents = lines.get(i).split("\\|");
                String session = contents[0];
                String command = contents[1];
                System.out.printf("session: %s, command: %s\n", session, command);
                if(lastCommitOfSession.containsKey(session) &&
                    lastCommitOfSession.get(session) >= i &&
                    !command.equals("commit") &&
                    !command.equals("begin transaction")) {
                    System.out.printf("i = %d, lastCommit = %d, redo!\n", i, lastCommitOfSession.get(session));
                    sqlHandler.evaluate("begin transaction", 114514);
                    sqlHandler.evaluate(command, 114514);
                    sqlHandler.evaluate("commit", 114514);

                }
            }
            bufferedReader.close();
            reader.close();


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
          myLock.readLock().unlock();
        }
        try {
            myLock.writeLock().lock();
            try
            {
                FileWriter writer = new FileWriter(logFilename);
                writer.write( "");
                writer.close();
            } catch (IOException e)
            {
                e.printStackTrace();
            }
            persistDatabase(databaseName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            myLock.writeLock().unlock();
        }
    }

    public void recover() {
        File managerDataFile = new File(Manager.getManagerDataFilePath());
        System.out.printf("managerDataFile: %s\n", managerDataFile.getPath());
        if (!managerDataFile.isFile()) return;
        try {
            System.out.println("??!! try to recover manager");
            InputStreamReader reader = new InputStreamReader(new FileInputStream(managerDataFile));
            BufferedReader bufferedReader = new BufferedReader(reader);
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.printf("databaseName: %s\n", line);
                createDatabaseIfNotExists(line);
                readLog(line);
            }
            bufferedReader.close();
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Get positions
    public static String getManagerDataFilePath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + "manager";
    }
}
