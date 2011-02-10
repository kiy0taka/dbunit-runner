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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(DbUnitRunner.class)
public class SampleTestCaseNoTestConnectionTest {

    private Connection conn;

    @Before
    public void setUp() throws SQLException {
        conn = DriverManager.getConnection("jdbc:h2:target/db;SCHEMA=dev", "scott", "tiger");
        conn.setAutoCommit(false);
    }

    @Test
    public void junit() {
        assertTrue(true);
    }

    @DbUnitTest(init="sample/emp.xml")
    public void dbunit_read() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select count(*) from emp");
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1));
        } finally {
            close(conn, stmt, rs);
        }
    }

    @DbUnitTest(init="sample/emp.xml", expected="sample/emp_expected.xml")
    public void dbunit_write() throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(
                "insert into emp (empno, ename, job, mgr, hiredate, sal, comm, deptno) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setInt(1, 7566);
            stmt.setString(2, "JONES");
            stmt.setString(3, "MANAGER");
            stmt.setInt(4, 7839);
            stmt.setDate(5, Date.valueOf("1981-04-02"));
            stmt.setBigDecimal(6, new BigDecimal("2975"));
            stmt.setBigDecimal(7, new BigDecimal("100"));
            stmt.setInt(8, 20);
            assertEquals(1, stmt.executeUpdate());
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignore) {}
            throw e;
        } finally {
            close(conn, stmt, null);
        }
    }

    @DbUnitTest(init="sample/emp.xml",
        expected="sample/emp_expected_exclude_empno.xml", excludeColumns="empno")
    public void dbunit_write_exclude_column() throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(
                "insert into emp (empno, ename, job, mgr, hiredate, sal, comm, deptno) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setInt(1, 7566);
            stmt.setString(2, "JONES");
            stmt.setString(3, "MANAGER");
            stmt.setInt(4, 7839);
            stmt.setDate(5, Date.valueOf("1981-04-02"));
            stmt.setBigDecimal(6, new BigDecimal("2975"));
            stmt.setBigDecimal(7, new BigDecimal("100"));
            stmt.setInt(8, 20);
            assertEquals(1, stmt.executeUpdate());
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignore) {}
            throw e;
        } finally {
            close(conn, stmt, null);
        }
    }

    @DbUnitTest(init="sample/emp.xml",
        expected="sample/emp_expected_exclude_empno_ename.xml", excludeColumns={"empno", "ename"})
    public void dbunit_write_exclude_columns() throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(
                "insert into emp (empno, ename, job, mgr, hiredate, sal, comm, deptno) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setInt(1, 7566);
            stmt.setString(2, "JONES");
            stmt.setString(3, "MANAGER");
            stmt.setInt(4, 7839);
            stmt.setDate(5, Date.valueOf("1981-04-02"));
            stmt.setBigDecimal(6, new BigDecimal("2975"));
            stmt.setBigDecimal(7, new BigDecimal("100"));
            stmt.setInt(8, 20);
            assertEquals(1, stmt.executeUpdate());
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignore) {}
            throw e;
        } finally {
            close(conn, stmt, null);
        }
    }

    @DbUnitTest(init="sample/emp_with_null.xml",
        expected="sample/emp_expected_with_null.xml", nullValue="[null]")
    public void dbunit_write_null_column() throws SQLException {
        PreparedStatement stmt = null;
        try {
            stmt = conn.prepareStatement(
                "insert into emp (empno, ename, job, mgr, hiredate, sal, comm, deptno) " +
                "values (?, ?, ?, ?, ?, ?, ?, ?)");
            stmt.setInt(1, 7566);
            stmt.setString(2, "JONES");
            stmt.setString(3, "MANAGER");
            stmt.setInt(4, 7839);
            stmt.setDate(5, Date.valueOf("1981-04-02"));
            stmt.setBigDecimal(6, new BigDecimal("2975"));
            stmt.setBigDecimal(7, new BigDecimal("100"));
            stmt.setNull(8, Types.INTEGER);
            assertEquals(1, stmt.executeUpdate());
            conn.commit();
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException ignore) {}
            throw e;
        } finally {
            close(conn, stmt, null);
        }
    }



    @DbUnitTest(init="sample/emp.xml", sql="alter sequence my_seq restart with 100")
    public void dbunit_sql() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery("select NEXTVAL('my_seq')");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        } finally {
            close(conn, stmt, rs);
        }
    }

    private void close(Connection conn, Statement stmt, ResultSet rs) {
        SQLException failureCause = null;
        try {
            if (rs != null) rs.close();
        } catch (SQLException e) {
            failureCause = e;
        }
        try {
            if (stmt != null) stmt.close();
        } catch (SQLException e) {
            failureCause = failureCause == null ? e : failureCause;
        }
        try {
            if (conn != null) conn.close();
        } catch (SQLException e) {
            failureCause = failureCause == null ? e : failureCause;
        }
        if (failureCause != null) throw new RuntimeException(failureCause);
    }

}
