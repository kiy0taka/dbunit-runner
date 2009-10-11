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

import static org.junit.Assert.assertSame;
import static org.kiy0taka.dbunit.MockTable.EMPNO;
import static org.kiy0taka.dbunit.MockTable.ENAME;
import static org.kiy0taka.dbunit.MockTable.HIREDATE;
import static org.kiy0taka.dbunit.MockTable.SAL;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;

import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.junit.Test;

public class DataSetBuilderTest {

    @Test
    public void toDataSet() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new DefaultDataSet(new MockTable(new Object[][] {
            {7369, "SMITH", Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {7499, "ALLEN", Date.valueOf("1981-02-20"), new BigDecimal("1600.00")},
            {7521, "WARD", Date.valueOf("1981-02-22"), new BigDecimal("1250.00")}
        }));
        IDataSet actual = new DataSetBuilder(dataSet).toDataSet();
        assertSame(dataSet, actual);
    }

    @Test
    public void nullValue() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new DefaultDataSet(new MockTable(new Object[][] {
            {7369, "[null]", Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {7499, "ALLEN", "[null]", new BigDecimal("1600.00")},
            {7521, "WARD", Date.valueOf("1981-02-22"), "[null]"}
        }));
        IDataSet actual = new DataSetBuilder(dataSet).nullValue("[null]").toDataSet();
        IDataSet expected = new DefaultDataSet(new MockTable(new Object[][] {
            {7369, null, Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {7499, "ALLEN", null, new BigDecimal("1600.00")},
            {7521, "WARD", Date.valueOf("1981-02-22"), null}
        }));
        Assertion.assertEquals(actual, expected);
    }

    @Test
    public void nullValue_no_nullValue() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new DefaultDataSet(new MockTable(new Object[][] {
            {7369, "SMITH", Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {7499, "ALLEN", Date.valueOf("1981-02-20"), new BigDecimal("1600.00")},
            {7521, "WARD", Date.valueOf("1981-02-22"), new BigDecimal("1250.00")}
        }));
        IDataSet actual = new DataSetBuilder(dataSet).nullValue("[null]").toDataSet();
        IDataSet expected = new DefaultDataSet(new MockTable(new Object[][] {
            {7369, "SMITH", Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {7499, "ALLEN", Date.valueOf("1981-02-20"), new BigDecimal("1600.00")},
            {7521, "WARD", Date.valueOf("1981-02-22"), new BigDecimal("1250.00")}
        }));
        Assertion.assertEquals(expected, actual);
    }

    @Test
    public void rtrim_true() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new DefaultDataSet(new MockTable(new Object[][] {
            {1, "aaa"},
            {2, "bbb   "},
            {3, "   ccc"},
            {4, "  ddd   "},
            {5, "  e e e   "}
        }, EMPNO, ENAME));
        IDataSet actual = new DataSetBuilder(dataSet).rtrim(true).toDataSet();
        IDataSet expected = new DefaultDataSet(new MockTable(new Object[][] {
            {1, "aaa"},
            {2, "bbb"},
            {3, "   ccc"},
            {4, "  ddd"},
            {5, "  e e e"}
        }, EMPNO, ENAME));
        Assertion.assertEquals(expected, actual);
    }

    @Test
    public void rtrim_false() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new DefaultDataSet(new MockTable(new Object[][] {
            {1, "aaa"},
            {2, "bbb   "},
            {3, "   ccc"},
            {4, "  ddd   "},
            {5, "  e e e   "}
        }, EMPNO, ENAME));
        IDataSet actual = new DataSetBuilder(dataSet).rtrim(false).toDataSet();
        IDataSet expected = new DefaultDataSet(new MockTable(new Object[][] {
            {1, "aaa"},
            {2, "bbb   "},
            {3, "   ccc"},
            {4, "  ddd   "},
            {5, "  e e e   "}
        }, EMPNO, ENAME));
        Assertion.assertEquals(expected, actual);
    }

    @Test
    public void nullValue_excludeColumns_rtrim() throws IOException, DatabaseUnitException {
        IDataSet dataSet = new DefaultDataSet(new MockTable(new Object[][] {
            {7369, "[null]", Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {7499, "ALLEN", "[null]", new BigDecimal("1600.00")},
            {7521, " WARD ", Date.valueOf("1981-02-22"), "[null]"}
        }));
        IDataSet actual = new DataSetBuilder(dataSet)
            .nullValue("[null]")
            .excludeColumns("empno")
            .rtrim(true).toDataSet();
        IDataSet expected = new DefaultDataSet(new MockTable(new Object[][] {
            {null, Date.valueOf("1980-12-17"), new BigDecimal("800.00")},
            {"ALLEN", null, new BigDecimal("1600.00")},
            {" WARD", Date.valueOf("1981-02-22"), null}
        }, ENAME, HIREDATE, SAL));
        Assertion.assertEquals(expected, actual);
    }

}
