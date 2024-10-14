package com.techmonad.catalog;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.catalog.Catalog;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.DataFile;

import java.util.*;


public class RestCatalog {

    public static void main(String[] args) {

        try {

            RESTCatalog catalog = CatalogService.getCatalog();
            Namespace nyc = Namespace.of("db");
            TableIdentifier name = TableIdentifier.of(nyc, "customers");
            Table table = catalog.loadTable(name);
            TableScan scan = table.newScan();

            TableScan filteredScan = scan.filter(Expressions.equal("date", "2022-06-28")).select("customer_name", "date");
            Iterable<CombinedScanTask> result = filteredScan.planTasks();


            CombinedScanTask task = result.iterator().next();
            DataFile dataFile = task.files().iterator().next().file();
            System.out.println(dataFile);

            //CloseableIterable<Record> result = IcebergGenerics.read(table).build();
            //for (Record r: result) {
            //    System.out.println(r);
            //}


        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
