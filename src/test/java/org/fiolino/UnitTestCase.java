package org.fiolino;

import org.fiolino.common.ioc.Beans;
import org.junit.BeforeClass;

public class UnitTestCase {

    @BeforeClass
    public static void beforeClass() throws Exception {
        Beans.loadProperties("/test-application.properties");
    }
}
