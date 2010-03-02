/**
 * Copyright (C) 2009 kiy0taka.org
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.kiy0taka.dbunit;

import static org.kiy0taka.dbunit.DataSetBuilder.dataSet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;

/**
 * JUnit Runner implementation for DbUnit.
 * @author kiy0taka
 */
public class DbUnitRunner extends BlockJUnit4ClassRunner {

    private static final ResourceBundle BUNDLE;

    static {
        BUNDLE = PropertyResourceBundle.getBundle("dbunit-runner");
        loadDriver(BUNDLE.getString("driver"));
    }

    protected static void loadDriver(String driverName) {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private enum DataSetType {
        xml() {
            public IDataSet createDataSet(URL url) throws DataSetException, IOException {
                return new FlatXmlDataSet(url);
            }
        },
        xls() {
            public IDataSet createDataSet(URL url) throws DataSetException, IOException {
                return new XlsDataSet(url.openStream());
            }
        };
        public abstract IDataSet createDataSet(URL url) throws DataSetException, IOException;
    }

    protected DataSource dataSource;

    protected Connection testConnection;

    protected String jdbcUrl = BUNDLE.getString("url");

    protected String username = BUNDLE.getString("username");

    protected String password = BUNDLE.getString("password");

    /**
     * Constract Runner for DbUnit.
     * @param testClass Test Class
     * @throws InitializationError Initialization error
     */
    public DbUnitRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    protected Statement methodBlock(final FrameworkMethod method) {
        Statement stmt = super.methodBlock(method);
        DbUnitTest ann = method.getAnnotation(DbUnitTest.class);
        return ann == null ? stmt : new DbUnitStatement(ann, stmt);
    }

    protected List<FrameworkMethod> computeTestMethods() {
        Set<FrameworkMethod> set = new HashSet<FrameworkMethod>(super.computeTestMethods());
        set.addAll(getTestClass().getAnnotatedMethods(DbUnitTest.class));
        return new ArrayList<FrameworkMethod>(set);
    }

    protected Object createTest() throws Exception {
        Object result = super.createTest();
        dataSource = createDataSource();
        List<FrameworkField> connFields = getTestClass().getAnnotatedFields(TestConnection.class);
        if (!connFields.isEmpty()) {
            testConnection = dataSource.getConnection();
            for (FrameworkField ff : connFields) {
                final Field f = ff.getField();
                AccessController.doPrivileged(new SetAccessibleAction(f));
                f.set(result, testConnection);
            }
        }
        List<FrameworkField> dsFields = getTestClass().getAnnotatedFields(TestDataSource.class);
        if (!dsFields.isEmpty()) {
            for (FrameworkField ff : dsFields) {
                final Field f = ff.getField();
                AccessController.doPrivileged(new SetAccessibleAction(f));
                f.set(result, dataSource);
            }
        }
        return result;
    }

    protected DataSource createDataSource() {
        BasicDataSource result = new BasicDataSource();
        result.setUsername(username);
        result.setPassword(password);
        result.setUrl(jdbcUrl);
        return result;
    }

    private static class SetAccessibleAction implements PrivilegedAction<Object> {

        private Field field;

        public SetAccessibleAction(Field field) {
            this.field = field;
        }

        public Object run() {
            field.setAccessible(true);
            return null;
        }
    }

    protected class DbUnitStatement extends Statement {
        private DbUnitTest ann;
        private Statement stmt;

        protected DbUnitStatement(DbUnitTest ann, Statement stmt) {
            this.ann = ann;
            this.stmt = stmt;
        }

        public void evaluate() throws Throwable {
            IDatabaseConnection conn = createDatabaseConnection();
            try {
                IDataSet initData = dataSet(load(ann.init())).nullValue(ann.nullValue()).toDataSet();
                ann.operation().toDatabaseOperation().execute(conn, initData);
                stmt.evaluate();
                if (testConnection != null) {
                    testConnection.commit();
                }
            } catch (Throwable e) {
                if (testConnection != null) {
                    testConnection.rollback();
                }
                throw e;
            } finally {
                if (testConnection != null) {
                    testConnection.close();
                }
                conn.close();
            }
            if (!ann.expected().isEmpty()) {
                assertTables();
            }
        }

        protected void assertTables() {
            IDatabaseConnection conn = createDatabaseConnection();
            try {
                IDataSet expected = dataSet(load(ann.expected()))
                    .excludeColumns(ann.excludeColumns())
                    .nullValue(ann.nullValue())
                    .rtrim(ann.rtrim())
                    .toDataSet();
                IDataSet actual = dataSet(conn.createDataSet(expected.getTableNames()))
                    .excludeColumns(ann.excludeColumns())
                    .rtrim(ann.rtrim())
                    .toDataSet();
                Assertion.assertEquals(expected, actual);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            } catch (DatabaseUnitException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    conn.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        protected IDataSet load(String path) {
            URL url = getTestClass().getJavaClass().getResource(path);
            if (url == null) {
                throw new RuntimeException(new FileNotFoundException(path));
            }
            String suffix = path.substring(path.lastIndexOf('.') + 1).toLowerCase(Locale.getDefault());
            try {
                return DataSetType.valueOf(suffix).createDataSet(url);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        protected IDatabaseConnection createDatabaseConnection() {
            try {
                return new DatabaseDataSourceConnection(dataSource);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
