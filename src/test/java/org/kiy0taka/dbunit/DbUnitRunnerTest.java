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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;

import javax.sql.DataSource;

import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.dataset.xml.FlatXmlProducer;
import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.kiy0taka.dbunit.DbUnitRunner.DbUnitStatement;
import org.kiy0taka.dbunit.DbUnitTest.Operation;
import org.xml.sax.InputSource;

public class DbUnitRunnerTest {

    @TestConnection
    public Connection publicConnectionWithAnnotation;

    @TestConnection
    protected Connection protectedConnectionWithAnnotation;

    @TestConnection
    Connection packageConnectionWithAnnotation;

    @TestConnection
    private Connection privateConnectionWithAnnotation;

    protected Connection publicConnectionNonAnnotation;

    protected Connection protectedConnectionNonAnnotation;

    Connection packageConnectionNonAnnotation;

    private Connection privateConnectionNonAnnotation;

    @TestDataSource
    public DataSource publicDataSourceWithAnnotation;

    @TestDataSource
    protected DataSource protectedDataSourceWithAnnotation;

    @TestDataSource
    DataSource packageDataSourceWithAnnotation;

    @TestDataSource
    private DataSource privateDataSourceWithAnnotation;

    public DataSource publicDataSourceNonAnnotation;

    protected DataSource protectedDataSourceNonAnnotation;

    DataSource packageDataSourceNonAnnotation;

    private DataSource privateDataSourceNonAnnotation;

    @Test
    public void loadDriver() {
        DbUnitRunner.loadDriver("org.h2.Driver");
    }

    @Test
    public void loadDriver_failure() {
        try {
            DbUnitRunner.loadDriver("dirver.not.found");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof ClassNotFoundException);
        }
    }

    @Test
    public void methodBlock_no_annotation() throws Throwable {
        FrameworkMethod method = new FrameworkMethod(getMethod());
        Statement stmt = new DbUnitRunner(getClass()).methodBlock(method);
        assertFalse(stmt instanceof DbUnitStatement);
    }

    @Test
    @DbUnitTest(init="test.xml")
    public void methodBlock_with_annotation() throws Throwable {
        FrameworkMethod method = new FrameworkMethod(getMethod());
        Statement stmt = new DbUnitRunner(getClass()).methodBlock(method);
        assertTrue(stmt instanceof DbUnitStatement);
    }

    @Test
    public void createDataSource() throws InitializationError {
        assertNotNull(new DbUnitRunner(getClass()).createDataSource());
    }

    @Test
    public void createDataSource_failure() throws InitializationError {
        try {
            DbUnitRunner runner = new DbUnitRunner(getClass());
            runner.jdbcUrl = "not found.";
            runner.createDataSource();
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof SQLException);
        }
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_success() throws Throwable {

        Connection conn = mock(Connection.class);

        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.testConnection = conn;
        runner.new DbUnitStatement(getAnnotation(), mockStatement()).evaluate();

        verify(conn).commit();
        verify(conn).close();
        verifyNoMoreInteractions(conn);
    }

    @Test
    @DbUnitTest(init="test.xml", expected="test.xml", operation=Operation.NONE)
    public void evaluate_assert_tables_failure() throws Throwable {
        Connection conn = mock(Connection.class);

        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.testConnection = conn;
        try {
            runner.new DbUnitStatement(getAnnotation(), mockStatement()) {
                protected void assertTables() {
                    assertTrue("Expected", false);
                }
            }.evaluate();
        } catch (AssertionError e) {
            assertEquals("Expected", e.getMessage());
        }
    }

    @Test
    @DbUnitTest(init="test.xml", expected="test.xml", operation=Operation.NONE)
    public void evaluate_exception_occured_at_test_method() throws Throwable {
        final Exception failureCause = new Exception("test error");
        Connection conn = mock(Connection.class);

        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.testConnection = conn;
        try {
            runner.new DbUnitStatement(getAnnotation(), new Statement() {
                public void evaluate() throws Throwable {
                    throw failureCause;
                }
            }).evaluate();
            fail("Expecting Exception");
        } catch (Exception e) {
            assertSame(failureCause, e);
        }
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_exception_occured_at_commit() throws Throwable {

        SQLException commitFailure = new SQLException("commit failure");
        Connection conn = mock(Connection.class);
        doThrow(commitFailure).when(conn).commit();

        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.testConnection = conn;
        try {
            runner.new DbUnitStatement(getAnnotation(), mockStatement()).evaluate();
            fail("Expecting RuntimeException");
        } catch (SQLException e) {
            assertSame(commitFailure, e);
        }
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_no_test_connection() throws Throwable {
        new DbUnitRunner(getClass()).new DbUnitStatement(getAnnotation(), mockStatement()).evaluate();
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_exception_occured_at_test_method_no_test_connection() throws Throwable {
        final Exception failureCause = new Exception("test error");
        DbUnitRunner runner = new DbUnitRunner(getClass());
        try {
            runner.new DbUnitStatement(getAnnotation(), new Statement() {
                public void evaluate() throws Throwable {
                    throw failureCause;
                }
            }).evaluate();
            fail("Expecting Exception");
        } catch (Exception e) {
            assertSame(failureCause, e);
        }
    }

    @Test
    public void load_xml() throws Exception {
        IDataSet dataSet = new FlatXmlDataSet(
            new FlatXmlProducer(new InputSource(getClass().getResourceAsStream("test.xml"))));
        IDataSet actual = new DbUnitRunner(getClass()).new DbUnitStatement(null, null).load("test.xml");
        Assertion.assertEquals(dataSet, actual);
    }

    @Test
    public void load_xls() throws Exception {
        IDataSet expected = new XlsDataSet(getClass().getResource("test.xls").openStream());
        IDataSet actual = new DbUnitRunner(getClass()).new DbUnitStatement(null, null).load("test.xls");
        Assertion.assertEquals(expected, actual);
    }

    @Test
    public void load_file_not_found() throws Exception {
        try {
            new DbUnitRunner(getClass()).new DbUnitStatement(null, null).load("not_found");
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
        }
    }

    @Test
    public void load_invalid_format() throws Exception {
        try {
            new DbUnitRunner(getClass()).new DbUnitStatement(null, null).load("invalid.xml");
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof DataSetException);
        }
    }

    @Test
    public void load_unknown_type() throws Exception {
        try {
            new DbUnitRunner(getClass()).new DbUnitStatement(null, null).load("test.txt");
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void createTest_Connection() throws Exception {
        final Connection conn = mock(Connection.class);
        final DataSource ds = mock(DataSource.class);
        when(ds.getConnection()).thenReturn(conn);
        DbUnitRunnerTest test = (DbUnitRunnerTest) new DbUnitRunner(getClass()) {
            protected DataSource createDataSource() {
                return ds;
            }
        }.createTest();

        assertSame("public with anntation", conn, test.publicConnectionWithAnnotation);
        assertSame("protected with anntation", conn, test.protectedConnectionWithAnnotation);
        assertSame("package with anntation", conn, test.packageConnectionWithAnnotation);
        assertSame("private with anntation", conn, test.privateConnectionWithAnnotation);

        assertNull("public non anntation", test.publicConnectionNonAnnotation);
        assertNull("protected non anntation", test.protectedConnectionNonAnnotation);
        assertNull("package non anntation", test.packageConnectionNonAnnotation);
        assertNull("private non anntation", test.privateConnectionNonAnnotation);
    }

    @Test
    public void createTest_DataSource() throws Exception {
        final DataSource ds = mock(DataSource.class);
        DbUnitRunnerTest test = (DbUnitRunnerTest) new DbUnitRunner(getClass()) {
            protected DataSource createDataSource() {
                return ds;
            }
        }.createTest();

        assertSame("public with anntation", ds, test.publicDataSourceWithAnnotation);
        assertSame("protected with anntation", ds, test.protectedDataSourceWithAnnotation);
        assertSame("package with anntation", ds, test.packageDataSourceWithAnnotation);
        assertSame("private with anntation", ds, test.privateDataSourceWithAnnotation);

        assertNull("public non anntation", test.publicDataSourceNonAnnotation);
        assertNull("protected non anntation", test.protectedDataSourceNonAnnotation);
        assertNull("package non anntation", test.packageDataSourceNonAnnotation);
        assertNull("private non anntation", test.privateDataSourceNonAnnotation);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_success() throws Throwable {
        IDataSet dataSet = new FlatXmlDataSet(
            new FlatXmlProducer(new InputSource(getClass().getResourceAsStream("test.xml"))));
        final IDatabaseConnection conn = mock(IDatabaseConnection.class);
        when(conn.createDataSet((String[]) anyObject())).thenReturn(dataSet);

        new DbUnitRunner(getClass()).new DbUnitStatement(getAnnotation(), null) {
            protected IDatabaseConnection createDatabaseConnection() {
                return conn;
            }
        }.assertTables();

        verify(conn).createDataSet((String[]) anyObject());
        verify(conn).close();
        verifyNoMoreInteractions(conn);
    }

    @Test(expected=AssertionError.class)
    @DbUnitTest(init="", expected="assertFailure.xml")
    public void assertTables_failure() throws Throwable {
        IDataSet dataSet = new FlatXmlDataSet(
            new FlatXmlProducer(new InputSource(getClass().getResourceAsStream("test.xml"))));
        final IDatabaseConnection conn = mock(IDatabaseConnection.class);
        when(conn.createDataSet((String[]) anyObject())).thenReturn(dataSet);

        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.new DbUnitStatement(getAnnotation(), null) {
            protected IDatabaseConnection createDatabaseConnection() {
                return conn;
            }
        }.assertTables();

        verify(conn).close();
        verifyNoMoreInteractions(conn);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_sqlexception_occured() throws Throwable {
        SQLException failureCause = new SQLException("failure");

        final IDatabaseConnection conn = mock(IDatabaseConnection.class);
        when(conn.createDataSet((String[]) anyObject())).thenThrow(failureCause);

        DbUnitRunner runner = new DbUnitRunner(getClass());
        try {
            runner.new DbUnitStatement(getAnnotation(), null) {
                protected IDatabaseConnection createDatabaseConnection() {
                    return conn;
                }
            }.assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(failureCause, e.getCause());
        }
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_databaseunitexception_occured() throws Throwable {
        DataSetException failureCause = new DataSetException("failure");

        IDataSet dataSet = mock(IDataSet.class);
        when(dataSet.getTableNames()).thenThrow(failureCause);

        final IDatabaseConnection conn = mock(IDatabaseConnection.class);
        when(conn.createDataSet((String[]) anyObject())).thenReturn(dataSet);

        DbUnitRunner runner = new DbUnitRunner(getClass());
        try {
            runner.new DbUnitStatement(getAnnotation(), null) {
                protected IDatabaseConnection createDatabaseConnection() {
                    return conn;
                }
            }.assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(failureCause, e.getCause());
        }
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_exception_occured_at_close() throws Throwable {
        SQLException closeFailure = new SQLException("close failure");
        IDataSet dataSet = new FlatXmlDataSet(
            new FlatXmlProducer(new InputSource(getClass().getResourceAsStream("test.xml"))));

        final IDatabaseConnection conn = mock(IDatabaseConnection.class);
        when(conn.createDataSet((String[]) anyObject())).thenReturn(dataSet);
        doThrow(closeFailure).when(conn).close();

        DbUnitRunner runner = new DbUnitRunner(getClass());
        try {
            runner.new DbUnitStatement(getAnnotation(), null) {
                protected IDatabaseConnection createDatabaseConnection() {
                    return conn;
                }
            }.assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(closeFailure, e.getCause());
        }
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_exception_occured_at_create_conn() throws Throwable {
        final RuntimeException createFailure = new RuntimeException("create failure");

        DbUnitRunner runner = new DbUnitRunner(getClass());
        try {
            runner.new DbUnitStatement(getAnnotation(), null) {
                protected IDatabaseConnection createDatabaseConnection() {
                    throw createFailure;
                }
            }.assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(createFailure, e);
        }
    }

    @Test
    public void createDatabaseConnection() throws InitializationError {
        assertNotNull(new DbUnitRunner(getClass()).new DbUnitStatement(null, null).createDatabaseConnection());
    }

    @Test
    public void createDatabaseConnection_with_boolean_Property() throws InitializationError {
        Properties properties = new Properties();
        properties.setProperty("http://www.dbunit.org/features/batchedStatements", "true");
        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.configProperties = properties;
        IDatabaseConnection connection = runner.new DbUnitStatement(null, null).createDatabaseConnection();
        DatabaseConfig config = connection.getConfig();
        assertTrue((Boolean) config.getProperty("http://www.dbunit.org/features/batchedStatements"));
    }

    @Test
    public void createDatabaseConnection_with_Object_Property() throws InitializationError {
        Properties properties = new Properties();
        properties.setProperty("http://www.dbunit.org/properties/datatypeFactory",
            "org.dbunit.ext.postgresql.PostgresqlDataTypeFactory");
        DbUnitRunner runner = new DbUnitRunner(getClass());
        runner.configProperties = properties;
        IDatabaseConnection connection = runner.new DbUnitStatement(null, null).createDatabaseConnection();
        DatabaseConfig config = connection.getConfig();

        assertEquals(PostgresqlDataTypeFactory.class,
            config.getProperty("http://www.dbunit.org/properties/datatypeFactory").getClass());
    }

    @Test
    public void optionalValue() throws IOException {
        ResourceBundle bundle = new PropertyResourceBundle(new StringReader("key=value"));
        assertEquals("value", DbUnitRunner.optionalValue(bundle, "key"));
    }

    @Test
    public void optionalValue_missing() throws IOException {
        ResourceBundle bundle = new PropertyResourceBundle(new StringReader(""));
        assertNull(DbUnitRunner.optionalValue(bundle, "key"));
    }

    @Test
    public void executeUpdate() throws InitializationError, SQLException {
        DbUnitRunner runner = new DbUnitRunner(getClass());
        PreparedStatement stmt = mock(PreparedStatement.class);
        Connection conn = mock(Connection.class);
        IDatabaseConnection dbc = mock(IDatabaseConnection.class);
        when(dbc.getConnection()).thenReturn(conn);
        when(conn.prepareStatement("update ...")).thenReturn(stmt);
        runner.new DbUnitStatement(null, null).executeUpdate(dbc, "update ...");
    }

    @Test
    public void executeUpdate_2sql() throws InitializationError, SQLException {
        DbUnitRunner runner = new DbUnitRunner(getClass());
        PreparedStatement stmt = mock(PreparedStatement.class);
        Connection conn = mock(Connection.class);
        IDatabaseConnection dbc = mock(IDatabaseConnection.class);
        when(dbc.getConnection()).thenReturn(conn);
        when(conn.prepareStatement("update ...")).thenReturn(stmt);
        when(conn.prepareStatement("insert ...")).thenReturn(stmt);
        runner.new DbUnitStatement(null, null).executeUpdate(dbc, "update ...", "insert ...");
    }

    @Test(expected=SQLException.class)
    public void executeUpdate_error() throws InitializationError, SQLException {
        DbUnitRunner runner = new DbUnitRunner(getClass());
        Connection conn = mock(Connection.class);
        IDatabaseConnection dbc = mock(IDatabaseConnection.class);
        when(dbc.getConnection()).thenReturn(conn);
        when(conn.prepareStatement("update ...")).thenThrow(new SQLException());
        runner.new DbUnitStatement(null, null).executeUpdate(dbc, "update ...");
    }

    private Method getMethod() {
        return getMethod(2);
    }

    private Method getMethod(int stackDepth) {
        try {
            return getClass().getMethod(new Throwable().getStackTrace()[stackDepth].getMethodName());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DbUnitTest getAnnotation() {
        return getMethod(2).getAnnotation(DbUnitTest.class);
    }

    private Statement mockStatement() {
        return new Statement() {
            public void evaluate() throws Throwable {}
        };
    }
}
