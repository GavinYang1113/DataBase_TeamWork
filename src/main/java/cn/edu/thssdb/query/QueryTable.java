package cn.edu.thssdb.query;

import cn.edu.thssdb.parser.ImpVisitor;
import cn.edu.thssdb.parser.SQLParser;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.schema.Table;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Designed for the select query with join/filtering...
 * hasNext() looks up whether the select result contains a next row
 * next() returns a row, plz keep an iterator.
 */

public class QueryTable implements Iterator<Row> {



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