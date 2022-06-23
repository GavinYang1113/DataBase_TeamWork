package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.ImpVisitor;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;
import jdk.nashorn.internal.ir.LiteralNode;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

import static cn.edu.thssdb.schema.Column.parseEntry;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {

  public ArrayList<Column> columns;
  public ArrayList<Row> rows;
  public QueryTable(Table table) {
    this.columns = new ArrayList<>();
    for (Column column : table.columns) {
      Column new_column = new Column(table.tableName + "." + column.getColumnName(),
              column.getColumnType(), column.getPrimary(), column.cantBeNull(), column.getMaxLength());
      this.columns.add(new_column);
    }
    Iterator<Row> rowIterator = table.iterator();
    this.rows = ImpVisitor.filter(rowIterator, columns, null);
  }

  public QueryTable(QueryTable left_table, QueryTable right_table, SQLParser.ConditionContext join_condition) {
    this.rows = new ArrayList<>();
    ArrayList<Column> common_columns = new ArrayList<>();

    // join_condition不为空，则为笛卡尔积
    if (join_condition != null) {
      (this.columns = new ArrayList<>(left_table.columns)).addAll(right_table.columns);

      for (Row left_row : left_table.rows) {
        for (Row right_row : right_table.rows) {
          Row new_row = new Row();
          new_row.getEntries().addAll(left_row.getEntries());
          new_row.getEntries().addAll(right_row.getEntries());
          this.rows.add(new_row);
        }
      }
      ArrayList<Row> filtered_rows = new ArrayList<>();
      if(join_condition.expression().get(1).comparer().children.get(0) instanceof SQLParser.Literal_valueContext){
        filtered_rows = ImpVisitor.filter(this.rows.iterator(), this.columns, join_condition);
      }
      else{
        filtered_rows = on_filter(this.rows.iterator(), this.columns, join_condition);
      }
      this.rows = filtered_rows;
    }

    // join_condition为空，为自然连接
    else {
      for(Column column_left : left_table.columns){
        for(Column column_right : right_table.columns){
          String left_column_real_name = get_real_column_name(column_left.getColumnName().toLowerCase());
          String right_column_real_name = get_real_column_name(column_right.getColumnName().toLowerCase());
          if(left_column_real_name.equals(right_column_real_name)){
            common_columns.add(column_right);
          }
        }
      }
      // 如果共同属性不存在则退化为笛卡尔积
      if(common_columns.size() == 0){
        (this.columns = new ArrayList<>(left_table.columns)).addAll(right_table.columns);

        for (Row left_row : left_table.rows) {
          for (Row right_row : right_table.rows) {
            Row new_row = new Row();
            new_row.getEntries().addAll(left_row.getEntries());
            new_row.getEntries().addAll(right_row.getEntries());
            this.rows.add(new_row);
          }
        }

        ArrayList<Row> filtered_rows = on_filter(this.rows.iterator(), this.columns, join_condition);
        this.rows = filtered_rows;
      }
      // 自然连接
      else{
        (this.columns = new ArrayList<>(left_table.columns)).addAll(right_table.columns);
        for(int i = 0; i < common_columns.size(); i++) {
          int temp_remove_index = ImpVisitor.get_column_index(this.columns, common_columns.get(i).getColumnName().toLowerCase());
          this.columns.remove(temp_remove_index);
        }
        for (Row left_row : left_table.rows) {
          for (Row right_row : right_table.rows) {
            Row new_row = new Row();
            new_row.getEntries().addAll(left_row.getEntries());
            int flag = 1;
            for (Column column : common_columns){
              String common_column_real_name = get_real_column_name(column.getColumnName().toLowerCase());
              if (left_row.getEntries().get(get_real_name_column_index(left_table.columns,common_column_real_name)).value.compareTo(right_row.getEntries().get(get_real_name_column_index(right_table.columns, common_column_real_name)).value) != 0){
                flag = 0;
                break;
              }
            }
            if (flag == 1){
              //添加
              for(Column column : right_table.columns){
                if(ImpVisitor.get_column_index(common_columns, column.getColumnName().toLowerCase()) == -1){
                  new_row.getEntries().add(right_row.getEntries().get(ImpVisitor.get_column_index(right_table.columns, column.getColumnName().toLowerCase())));
                }
              }
              this.rows.add(new_row);
            }
            else{
              continue;
            }
          }
        }
      }
    }
  }

  public static ArrayList<Row> on_filter(Iterator<Row> it, ArrayList<Column> columns, SQLParser.ConditionContext condition) {
    ArrayList<Row> return_array_list = new ArrayList<>();
    String left_column_name = null;
    int left_column_index = 0;
    SQLParser.ComparatorContext comparator = null;
    String right_column_name = null;
    int right_column_index = 0;

    if (condition != null){
      left_column_name = condition.expression().get(0).comparer().column_full_name().getText().toLowerCase();
      left_column_index = ImpVisitor.get_column_index(columns, left_column_name);
      comparator = condition.comparator();
      right_column_name = condition.expression().get(1).comparer().column_full_name().getText().toLowerCase();
      right_column_index = ImpVisitor.get_column_index(columns, right_column_name);
    }

    while (it.hasNext()) {
      Row row = it.next();
      boolean flag1 = false;
      Cell left_column_value = row.getEntries().get(left_column_index);
      Cell right_column_value = row.getEntries().get(right_column_index);

      if (comparator == null) {
        flag1 = true;
      } else if (comparator.EQ() != null) {
        if (left_column_value.compareTo(right_column_value) == 0)
          flag1 = true;
      } else if (comparator.NE() != null) {
        if (left_column_value.compareTo(right_column_value) != 0)
          flag1 = true;
      } else if (comparator.LE() != null) {
        if (left_column_value.compareTo(right_column_value) <= 0)
          flag1 = true;
      } else if (comparator.GE() != null) {
        if (left_column_value.compareTo(right_column_value) >= 0)
          flag1 = true;
      } else if (comparator.LT() != null) {
        if (left_column_value.compareTo(right_column_value) < 0)
          flag1 = true;
      } else if (comparator.GT() != null) {
        if (left_column_value.compareTo(right_column_value) > 0)
          flag1 = true;
      }
      if (flag1) {
        return_array_list.add(row);
      }
    }
    return return_array_list;
  }

  public static String get_real_column_name(String table_name){
    String ret_string = "";
    int i = table_name.indexOf('.');
    ret_string = table_name.substring(i+1);
    return ret_string;
  }

  public static int get_real_name_column_index(ArrayList<Column> columns, String real_column_name) {
    String this_table_real_column_name;
    for (int i = 0; i < columns.size(); i++) {
      this_table_real_column_name = get_real_column_name(columns.get(i).getColumnName());
      if (this_table_real_column_name.equals(real_column_name)) {
        return i;
      }
    }
    return -1;
  }
  @Override
  public boolean hasNext() {
    // TODO
    return true;
  }

  @Override
  public Row next() {
    // TODO
    return null;
  }
}