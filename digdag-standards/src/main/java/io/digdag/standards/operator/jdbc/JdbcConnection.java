package io.digdag.standards.operator.jdbc;

import io.digdag.standards.operator.redshift.RedshiftConnection;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public interface JdbcConnection
    extends AutoCloseable
{
    String buildCreateTableStatement(String selectSql, TableReference targetTable);

    String buildInsertStatement(String selectSql, TableReference targetTable);

    Exception validateStatement(String sql);

    void executeScript(String sql);

    void executeScript(String sql, List<Object> params);

    void executeUpdate(String sql);

    void executeUpdate(String sql, List<Object> params);

    void executeReadOnlyQuery(String sql, Consumer<JdbcResultSet> resultHandler)
        throws NotReadOnlyException;

    TransactionHelper getStrictTransactionHelper(String statusTableName, Duration cleanupDuration);

    default String escapeTableReference(TableReference ref)
    {
        if (ref.getSchema().isPresent()) {
            return escapeIdent(ref.getSchema().get()) + "." + escapeIdent(ref.getName());
        }
        else {
            return escapeIdent(ref.getName());
        }
    }

    String escapeIdent(String ident);

    void close();
}
