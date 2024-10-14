package com.techmonad.catalog;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;

public class CatalogService {

    public static RESTCatalog getCatalog() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
        properties.put(CatalogProperties.URI, "http://127.0.0.1:9001/iceberg/");
        RESTCatalog catalog = new RESTCatalog();
        Configuration conf = new Configuration();
        catalog.setConf(conf);
        catalog.initialize("test", properties);
        return catalog;
    }
}
