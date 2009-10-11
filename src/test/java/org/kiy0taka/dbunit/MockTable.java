package org.kiy0taka.dbunit;

import java.math.BigDecimal;
import java.sql.Date;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;

public class MockTable implements ITable {

    public static Column EMPNO = new Column("empno", DataType.INTEGER);
    public static Column ENAME = new Column("ename", DataType.VARCHAR);
    public static Column HIREDATE = new Column("ename", DataType.VARCHAR);
    public static Column SAL = new Column("sal", DataType.DECIMAL);

    private ITableMetaData tableMetaData;

    private Object[][] data;

    public MockTable(Object[][] data) {
        this(data, EMPNO, ENAME, HIREDATE, SAL);
    }

    public MockTable(Object[][] data, Column... columns) {
        this.data = data;
        tableMetaData = new DefaultTableMetaData("emp", columns);
    }

    @Override
    public int getRowCount() {
        return data.length;
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return tableMetaData;
    }

    @Override
    public Object getValue(int row, String column) throws DataSetException {
        int index = 0;
        for (Column c : tableMetaData.getColumns()) {
            if (c.getColumnName().equals(column)) break;
            index++;
        }
        return data[row][index];
    }

    public static Date d(String s) {
        return Date.valueOf(s);
    }

    public static BigDecimal b(String s) {
        return new BigDecimal(s);
    }
}
