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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.dbunit.operation.DatabaseOperation;


/**
 * DbUnit test method annotation.
 * @author kiy0taka
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface DbUnitTest {

    /**
     * Initial dataset file.
     */
    String init();

    /**
     * Expected dataset file.
     */
    String expected() default "";

    /**
     * Database setup operation.
     */
    Operation operation() default Operation.CLEAN_INSERT;

    /**
     * Exclude column names.
     * (i.e. {"empno", "emname"} or {"emp.empno", "dept.deptno"})
     */
    String[] excludeColumns() default "";

    /**
     * Convert this value to null.
     */
    String nullValue() default "";

    /**
     * @return
     */
    boolean rtrim() default false;

    /**
     * Annotation of Database operation.
     * @author kiy0taka
     */
    public enum Operation {

        /**
         * @see DatabaseOperation#NONE
         */
        NONE(DatabaseOperation.NONE),

        /**
         * @see DatabaseOperation#UPDATE
         */
        UPDATE(DatabaseOperation.UPDATE),

        /**
         * @see DatabaseOperation#INSERT
         */
        INSERT(DatabaseOperation.INSERT),

        /**
         * @see DatabaseOperation#REFRESH
         */
        REFRESH(DatabaseOperation.REFRESH),

        /**
         * @see DatabaseOperation#DELETE
         */
        DELETE(DatabaseOperation.DELETE),

        /**
         * @see DatabaseOperation#DELETE_ALL
         */
        DELETE_ALL(DatabaseOperation.DELETE_ALL),

        /**
         * @see DatabaseOperation#TRUNCATE_TABLE
         */
        TRUNCATE_TABLE(DatabaseOperation.TRUNCATE_TABLE),

        /**
         * @see DatabaseOperation#CLEAN_INSERT
         */
        CLEAN_INSERT(DatabaseOperation.CLEAN_INSERT);

        private DatabaseOperation operation;

        private Operation(DatabaseOperation operation) {
            this.operation = operation;
        }

        /**
         * Convert to {@link DatabaseOperation}.
         * @return {@link DatabaseOperation}
         */
        public DatabaseOperation toDatabaseOperation() {
            return operation;
        }
    }
}
