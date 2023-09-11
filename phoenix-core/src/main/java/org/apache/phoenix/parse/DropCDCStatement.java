package org.apache.phoenix.parse;

import org.apache.phoenix.jdbc.PhoenixStatement;

public class DropCDCStatement extends MutableStatement {
    private final TableName tableName;
    private final NamedNode cdcObjName;
    private final boolean ifExists;

    public DropCDCStatement(NamedNode cdcObjName, TableName tableName, boolean ifExists) {
        this.cdcObjName = cdcObjName;
        this.tableName = tableName;
        this.ifExists = ifExists;
    }

    public TableName getTableName() {
        return tableName;
    }

    public NamedNode getCdcObjName() {
        return cdcObjName;
    }

    @Override
    public int getBindCount() {
        return 0;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public PhoenixStatement.Operation getOperation() {
        return PhoenixStatement.Operation.DELETE;
    }
}
