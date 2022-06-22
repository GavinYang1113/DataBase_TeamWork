package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.v4.runtime.tree.ParseTree;
import cn.edu.thssdb.schema.Column.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 * K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 * K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 * <p>
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 * <p>
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
        if (currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null) return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        if (ctx.show_meta_stmt() != null) return new QueryResult(visitShow_meta_stmt(ctx.show_meta_stmt()));
        return null;
    }

    /**
     * 创建数据库
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
     * 删除数据库
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
     * 切换数据库
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
     * 删除表格
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

            int column_counts = ctx.column_def().size();
            Column[] res_columns = new Column[column_counts];

            //生成column
            for (int i = 0; i < column_counts; i++) {
                ColumnType type;
                String name = ctx.column_def().get(i).column_name().getText();
                boolean is_not_null = false;
                int is_primary_key = 0;
                int max_length = 128;

                //type子树
                List<ParseTree> type_tree_children = ctx.column_def().get(i).type_name().children;
                String type_name = type_tree_children.get(0).getText().toLowerCase();

                switch (type_name) {
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
                        max_length = Integer.parseInt(type_tree_children.get(2).getText());
                        break;
                    default:
                        type = ColumnType.INT;
                        System.out.println("Wrong Type!");
                }

                //处理属性定义紧接着的约束声明：not null 和 primary key
                List<SQLParser.Column_constraintContext> column_constraint_list = ctx.column_def().get(i).column_constraint();
                if (column_constraint_list != null) {
                    for (int j = 0; j < column_constraint_list.size(); j++) {
                        if (column_constraint_list.get(j).K_NOT() != null) {
                            //存在not null结点
                            is_not_null = false;
                        }
                        if (column_constraint_list.get(j).K_PRIMARY() != null) {
                            //primary key
                            is_primary_key = 1;
                        }
                    }
                }
                res_columns[i] = new Column(name, type, is_primary_key, is_not_null, max_length);
            }

            //处理在语句末尾定义的primary key
            SQLParser.Table_constraintContext table_constraints = ctx.table_constraint();
            List<SQLParser.Column_nameContext> primary_key_attr_list = table_constraints.column_name();
            if (res_columns != null) {
                for (Column res_item : res_columns) {
                    for (SQLParser.Column_nameContext key_item : primary_key_attr_list) {
                        if (res_item.getColumnName().equals(key_item.children.get(0).getText())) {
                            res_item.setPrimary(1);
                        }
                    }
                }
            }


            //如果是主键则必须为Not Null
            for (Column res_item : res_columns) {
                if (res_item.isPrimary()) {
                    res_item.setNotNull(true);
                }
            }

            //建表
            String table_name = ctx.table_name().getText().toLowerCase();
            GetCurrentDB().create(table_name, res_columns);

            return "Create table " + table_name + ".";
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        } catch (NumberFormatException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * TODO
     * 表格查询
     */
    @Override
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {

        try {
            String table_name = ctx.table_name().getText().toLowerCase();
            Table table = GetCurrentDB().get(table_name);

            int column_count = table.columns.size();
            String show_data = "[" + table_name + "]\n";//表头

            for (int i = 0; i < column_count; i++) {
                Column colunm_item = table.columns.get(i);

                //每一列的名称和类型
                show_data += colunm_item.getColumnName() + "   " + colunm_item.getColumnType().toString().toLowerCase(Locale.ROOT);
                if (colunm_item.getColumnType().toString().toLowerCase(Locale.ROOT).equals("string")) {
                    show_data += "(" + colunm_item.getMaxLength() + ")";
                }

                if (colunm_item.isPrimary()) {
                    show_data += "   Primary Key";
                }
                if (colunm_item.cantBeNull()) {
                    show_data += "   Not Null";
                }

                show_data += "\n";
            }
            return show_data;
        } catch (Exception e) {
            return e.toString();
        }
    }

    /**
     * TODO
     * 表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        String table_name = ctx.table_name().getText().toLowerCase();
        Table table = GetCurrentDB().get(table_name);
        List<SQLParser.Column_nameContext> column_list = ctx.column_name();//属性名
        List<SQLParser.Value_entryContext> value_list = ctx.value_entry();//值

        if (column_list.size() > 0) {

        } else {

        }


        return null;
    }

    /**
     * TODO
     * 表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        return null;
    }

    /**
     * TODO
     * 表格项更新
     */
    public int getColumnIndex(Table table, String target) {
        for(int i = 0; i < table.columns.size(); i ++)
            if(table.columns.get(i).getColumnName().equals(target))
                return i;
        return -1;
    }

    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        try {
            String table_name = ctx.table_name().getText().toLowerCase();
            String update_name = ctx.column_name().getText().toLowerCase();
            String update_value = ctx.expression().comparer().literal_value().getText();


            // 需要 update 的 column 索引
            Table table = GetCurrentDB().get(table_name);
            int update_index = getColumnIndex(table, update_name);
            if(update_index < 0) throw new Exception("Fail to find column " + update_name);


            String condition_name = ctx.multiple_condition().condition().expression(0).comparer().column_full_name().column_name().getText().toLowerCase();
            String condition_value = ctx.multiple_condition().condition().expression(1).comparer().literal_value().getText();

            // 需要 where 的 column 并不存在, 抛出异常
            if(getColumnIndex(table, condition_name) < 0) throw new Exception("Fail to find column " + condition_name);


            ArrayList<Row> update_rows = new ArrayList<>();

            for(Row row: update_rows) {
                // void update(Cell primaryCell, Row newRow)
                ArrayList<Cell> entries = new ArrayList<>(row.getEntries());
                entries.set(update_index, Column.parseEntry(update_value, table.columns.get(update_index)));
                table.update(row.getEntries().get(table.getPrimaryIndex()), new Row(entries));
            }

            System.out.println("test");

            return "Update table " + table_name + ".";
        }catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     * 表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        return null;
    }

    /**
     * 退出
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

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }
}
