package org.kiy0taka.dbunit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.*;

import java.io.IOException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.junit.Test;

public class DataSetBuilderTest {

    @Test
    public void toDataSet() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("emp_with_nullvalue.xml"));
        IDataSet actual = new DataSetBuilder(dataSet).toDataSet();
        assertSame(dataSet, actual);
    }

    @Test
    public void nullValue() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("emp_with_nullvalue.xml"));
        IDataSet actualDataSet = new DataSetBuilder(dataSet).nullValue("[null]").toDataSet();
        ITable actualTable = actualDataSet.getTable("emp");
        assertNull(actualTable.getValue(0, "job"));
        assertNull(actualTable.getValue(1, "hiredate"));
        assertNull(actualTable.getValue(2, "comm"));
    }

    @Test
    public void nullValue_no_nullValue() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("emp.xml"));
        IDataSet actualDataSet = new DataSetBuilder(dataSet).nullValue("[null]").toDataSet();
        ITable actualTable = actualDataSet.getTable("emp");
        assertNotNull(actualTable.getValue(0, "job"));
        assertNotNull(actualTable.getValue(1, "hiredate"));
        assertNotNull(actualTable.getValue(2, "comm"));
    }

    @Test
    public void nullValue_excludeColumns() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("emp_with_nullvalue.xml"));
        assertTrue(containsColumn(dataSet.getTable("emp"), "empno"));

        IDataSet actualDataSet = new DataSetBuilder(dataSet).excludeColumns("empno").nullValue("[null]").toDataSet();
        ITable actualTable = actualDataSet.getTable("emp");

        assertFalse(containsColumn(actualTable, "empno"));
        assertNull(actualTable.getValue(0, "job"));
        assertNull(actualTable.getValue(1, "hiredate"));
        assertNull(actualTable.getValue(2, "comm"));
    }

    @Test
    public void excludeColumns_nullValue() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new FlatXmlDataSet(getClass().getResource("emp_with_nullvalue.xml"));
        assertTrue(containsColumn(dataSet.getTable("emp"), "empno"));

        IDataSet actualDataSet = new DataSetBuilder(dataSet).nullValue("[null]").excludeColumns("empno").toDataSet();
        ITable actualTable = actualDataSet.getTable("emp");

        assertFalse(containsColumn(actualTable, "empno"));
        assertNull(actualTable.getValue(0, "job"));
        assertNull(actualTable.getValue(1, "hiredate"));
        assertNull(actualTable.getValue(2, "comm"));
    }

    private boolean containsColumn(ITable table, String columnName) throws DataSetException {
        for (Column column : table.getTableMetaData().getColumns()) {
            if (column.getColumnName().equals("empno")) {
                return true;
            }
        }
        return false;
    }
}
