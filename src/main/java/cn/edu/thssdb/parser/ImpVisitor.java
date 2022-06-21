package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.ArrayList;
import java.util.List;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 *     K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 *         K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 *
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 *
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if(currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null)  return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        return null;
    }

    /**
     创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    /**
     * TODO
     * 创建表格
     */
    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            if (manager.currentDatabase == null) {
                throw new DatabaseNotExistException();
            }

            String table_name = ctx.table_name().getText().toLowerCase();

            int column_counts = ctx.column_def().size();
            // Column(String name, ColumnType type, int primary, boolean notNull, int maxLength)
            Column[] columns = new Column[column_counts];
            System.out.printf("column_counts = %d\n", column_counts);

//            int primary_keys_counts = ctx.table_constraint().column_name().size();

            for (int i = 0; i < column_counts; i++) {
                ColumnType type;
                String name = ctx.column_def().get(i).column_name().getText();

                List<ParseTree> type_children = ctx.column_def().get(i).type_name().children;
                String tmp = type_children.get(0).getText().toLowerCase();
                boolean is_not_null = false;
                int is_primary_key = 0;
                int max_length = 0;

                System.out.println("2");
                switch (tmp) {
                    case "int":
                        type = ColumnType.INT;
                        break;
                    case "long":
                        type = ColumnType.LONG;
                        break;
                    case "double":
                        type = ColumnType.DOUBLE;
                        break;
                    case "string":
                        type = ColumnType.STRING;
                        max_length = Integer.parseInt(type_children.get(2).getText());
                        break;
                    default:
                        type = ColumnType.DOUBLE;
                        System.out.println("Wrong Type!");
                }




                List<SQLParser.Column_constraintContext> column_constraint_list = ctx.column_def().get(i).column_constraint();
                for (int j = 0; j < column_constraint_list.size(); j++) {
                    if (column_constraint_list.get(j).K_NOT() != null) {
                        //存在not null结点
                        is_not_null = true;
                    }
                    if (column_constraint_list.get(j).K_PRIMARY() != null) {
                        is_primary_key = 1;
                    }
                }

                columns[i] = new Column(name, type, is_primary_key, is_not_null, max_length);
            }
            System.out.println("");
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
//        manager.getCurrentDatabase().create(table_name, columns);
        return "create table " + " successfully";
    }


    /**
     * TODO
     表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {return null;}

    /**
     * TODO
     表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {return null;}

    /**
     退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }
}
