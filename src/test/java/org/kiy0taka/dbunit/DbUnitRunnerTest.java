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

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.createStrictMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.Assertion;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.excel.XlsDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.junit.Test;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.Statement;
import org.kiy0taka.dbunit.DbUnitRunner.DbUnitStatement;
import org.kiy0taka.dbunit.DbUnitTest.Operation;

public class DbUnitRunnerTest {

    @TestConnection
    protected Connection publicWithAnnotation;

    @TestConnection
    protected Connection protectedWithAnnotation;

    @TestConnection
    Connection packageWithAnnotation;

    @TestConnection
    private Connection privateWithAnnotation;

    protected Connection publicNonAnnotation;

    protected Connection protectedNonAnnotation;

    Connection packageNonAnnotation;

    private Connection privateNonAnnotation;

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
    public void createConnection() throws InitializationError {
        assertNotNull(new DbUnitRunner(getClass()).createConnection(false));
    }

    @Test
    public void createConnection_failure() throws InitializationError {
        try {
            DbUnitRunner runner = new DbUnitRunner(getClass());
            runner.jdbcUrl = "not found.";
            runner.createConnection(false);
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof SQLException);
        }
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_success() throws Throwable {

        final Connection conn = createStrictMock(Connection.class);
        conn.commit();
        conn.close();
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                testConnection = conn;
                return new DbUnitStatement(ann, new Statement() {
                    public void evaluate() throws Throwable {}
                });
            }
        }.createStatment().evaluate();

        verify(conn);
    }

    @Test
    @DbUnitTest(init="test.xml", expected="test.xml", operation=Operation.NONE)
    public void evaluate_assert_table_failure() throws Throwable {
        final Connection conn = createStrictMock(Connection.class);
        conn.commit();
        conn.close();
        replay(conn);
        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    testConnection = conn;
                    return new DbUnitStatement(ann, new Statement() {
                        public void evaluate() throws Throwable {}
                    }) {
                        @Override
                        protected void assertTables() {
                            assertTrue("Expected", false);
                        }
                    };
                }
            }.createStatment().evaluate();
        } catch (AssertionError e) {
            assertEquals("Expected", e.getMessage());
        }
        verify(conn);
    }

    @Test
    @DbUnitTest(init="test.xml", expected="test.xml", operation=Operation.NONE)
    public void evaluate_exception_occured_at_test_method() throws Throwable {
        final Exception failureCause = new Exception("test error");
        final Connection conn = createStrictMock(Connection.class);
        conn.rollback();
        conn.close();
        replay(conn);
        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    testConnection = conn;
                    return new DbUnitStatement(ann, new Statement() {
                        public void evaluate() throws Throwable {
                            throw failureCause;
                        }
                    });
                }
            }.createStatment().evaluate();
            fail("Expecting Exception");
        } catch (Exception e) {
            assertSame(failureCause, e);
        }
        verify(conn);
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_exception_occured_at_commit() throws Throwable {

        SQLException commitFailure = new SQLException("commit failure");
        final Connection conn = createStrictMock(Connection.class);
        conn.commit();
        expectLastCall().andThrow(commitFailure);
        conn.rollback();
        conn.close();
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    testConnection = conn;
                    return new DbUnitStatement(ann, new Statement() {
                        public void evaluate() throws Throwable {}
                    });
                }
            }.createStatment().evaluate();
            fail("Expecting RuntimeException");
        } catch (SQLException e) {
            assertSame(commitFailure, e);
        }

        verify(conn);
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_no_test_connection() throws Throwable {
        final DbUnitTest ann = getAnnotation();
        new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                return new DbUnitStatement(ann, new Statement() {
                    public void evaluate() throws Throwable {}
                });
            }
        }.createStatment().evaluate();
    }

    @Test
    @DbUnitTest(init="test.xml", operation=Operation.NONE)
    public void evaluate_exception_occured_at_test_method_no_test_connection() throws Throwable {
        final Exception failureCause = new Exception("test error");
        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(ann, new Statement() {
                        public void evaluate() throws Throwable {
                            throw failureCause;
                        }
                    });
                }
            }.createStatment().evaluate();
            fail("Expecting Exception");
        } catch (Exception e) {
            assertSame(failureCause, e);
        }
    }

    @Test
    public void load_xml() throws Exception {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("test.xml"));
        IDataSet actual = new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                return new DbUnitStatement(null, null);
            }
        }.createStatment().load("test.xml");
        Assertion.assertEquals(dataSet, actual);
    }

    @Test
    public void load_xls() throws Exception {
        IDataSet expected = new XlsDataSet(getClass().getResource("test.xls").openStream());
        IDataSet actual = new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                return new DbUnitStatement(null, null);
            }
        }.createStatment().load("test.xls");
        Assertion.assertEquals(expected, actual);
    }

    @Test
    public void load_file_not_found() throws Exception {
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(null, null);
                }
            }.createStatment().load("not_found");
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof FileNotFoundException);
        }
    }

    @Test
    public void load_invalid_format() throws Exception {
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(null, null);
                }
            }.createStatment().load("invalid.xml");
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof DataSetException);
        }
    }

    @Test
    public void load_unknown_type() throws Exception {
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(null, null);
                }
            }.createStatment().load("test.txt");
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
        }
    }

    @Test
    public void createTest() throws Exception {
        final Connection conn = createNiceMock(Connection.class);
        DbUnitRunnerTest test = (DbUnitRunnerTest) new DbUnitRunner(getClass()) {
            protected Connection createConnection(boolean autoCommit) { return conn; }
        }.createTest();

        assertSame("public with anntation", conn, test.publicWithAnnotation);
        assertSame("protected with anntation", conn, test.protectedWithAnnotation);
        assertSame("package with anntation", conn, test.packageWithAnnotation);
        assertSame("private with anntation", conn, test.privateWithAnnotation);

        assertNull("public non anntation", test.publicNonAnnotation);
        assertNull("protected non anntation", test.protectedNonAnnotation);
        assertNull("package non anntation", test.packageNonAnnotation);
        assertNull("private non anntation", test.privateNonAnnotation);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_success() throws Throwable {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("test.xml"));
        final IDatabaseConnection conn = createStrictMock(IDatabaseConnection.class);
        expect(conn.createDataSet((String[]) anyObject())).andReturn(dataSet);
        conn.close();
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                return new DbUnitStatement(ann, null) {
                    protected IDatabaseConnection createDatabaseConnection() {
                        return conn;
                    }
                };
            }
        }.createStatment().assertTables();
        verify(conn);
    }

    @Test(expected=AssertionError.class)
    @DbUnitTest(init="", expected="assertFailure.xml")
    public void assertTables_failure() throws Throwable {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("test.xml"));
        final IDatabaseConnection conn = createStrictMock(IDatabaseConnection.class);
        expect(conn.createDataSet((String[]) anyObject())).andReturn(dataSet);
        conn.close();
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                return new DbUnitStatement(ann, null) {
                    protected IDatabaseConnection createDatabaseConnection() {
                        return conn;
                    }
                };
            }
        }.createStatment().assertTables();
        verify(conn);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_sqlexception_occured() throws Throwable {
        SQLException failureCause = new SQLException("failure");

        final IDatabaseConnection conn = createStrictMock(IDatabaseConnection.class);
        expect(conn.createDataSet((String[]) anyObject())).andThrow(failureCause);
        conn.close();
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(ann, null) {
                        protected IDatabaseConnection createDatabaseConnection() {
                            return conn;
                        }
                    };
                }
            }.createStatment().assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(failureCause, e.getCause());
        }
        verify(conn);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_databaseunitexception_occured() throws Throwable {
        DataSetException failureCause = new DataSetException("failure");

        IDataSet dataSet = createStrictMock(IDataSet.class);
        expect(dataSet.getTableNames()).andThrow(failureCause);
        replay(dataSet);

        final IDatabaseConnection conn = createStrictMock(IDatabaseConnection.class);
        expect(conn.createDataSet((String[]) anyObject())).andReturn(dataSet);
        conn.close();
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(ann, null) {
                        protected IDatabaseConnection createDatabaseConnection() {
                            return conn;
                        }
                    };
                }
            }.createStatment().assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(failureCause, e.getCause());
        }
        verify(dataSet);
        verify(conn);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_exception_occured_at_close() throws Throwable {
        SQLException closeFailure = new SQLException("close failure");
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("test.xml"));

        final IDatabaseConnection conn = createStrictMock(IDatabaseConnection.class);
        expect(conn.createDataSet((String[]) anyObject())).andReturn(dataSet);
        conn.close();
        expectLastCall().andThrow(closeFailure);
        replay(conn);

        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(ann, null) {
                        protected IDatabaseConnection createDatabaseConnection() {
                            return conn;
                        }
                    };
                }
            }.createStatment().assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(closeFailure, e.getCause());
        }
        verify(conn);
    }

    @Test
    @DbUnitTest(init="", expected="test.xml")
    public void assertTables_exception_occured_at_create_conn() throws Throwable {
        final RuntimeException createFailure = new RuntimeException("create failure");

        final DbUnitTest ann = getAnnotation();
        try {
            new DbUnitRunner(getClass()) {
                protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(ann, null) {
                        protected IDatabaseConnection createDatabaseConnection() {
                            throw createFailure;
                        }
                    };
                }
            }.createStatment().assertTables();
            fail("Expecting RuntimeException");
        } catch (RuntimeException e) {
            assertSame(createFailure, e);
        }
    }

    @Test
    public void createDatabaseConnection() throws InitializationError {
        assertNotNull(new DbUnitRunner(getClass()) {
            protected DbUnitStatement createStatment() {
                    return new DbUnitStatement(null, null) {
                };
            }
        }.createStatment().createDatabaseConnection());
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

}
