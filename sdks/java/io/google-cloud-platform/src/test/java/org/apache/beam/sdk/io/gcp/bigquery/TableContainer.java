package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableRow;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by relax on 3/30/17.
 */
class TableContainer {
  Table table;
  List<TableRow> rows;
  List<String> ids;

  TableContainer(Table table) {
    this.table = table;
    this.rows = new ArrayList<>();
    this.ids = new ArrayList<>();
  }

  TableContainer addRow(TableRow row, String id) {
    rows.add(row);
    ids.add(id);
    return this;
  }

  Table getTable() {
    return table;
  }

  List<TableRow> getRows() {
    return rows;
  }
}
