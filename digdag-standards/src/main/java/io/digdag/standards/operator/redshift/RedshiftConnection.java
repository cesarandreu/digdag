package io.digdag.standards.operator.redshift;

import com.google.common.base.Optional;
import io.digdag.client.config.ConfigException;
import io.digdag.standards.operator.pg.PgConnection;
import io.digdag.standards.operator.jdbc.TransactionHelper;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import io.digdag.client.config.Config;
import io.digdag.standards.operator.jdbc.LockConflictException;
import io.digdag.util.DurationParam;
import io.digdag.standards.operator.jdbc.AbstractJdbcConnection;
import io.digdag.standards.operator.jdbc.AbstractPersistentTransactionHelper;
import io.digdag.standards.operator.jdbc.DatabaseException;
import io.digdag.standards.operator.jdbc.JdbcResultSet;
import io.digdag.standards.operator.jdbc.TransactionHelper;
import io.digdag.standards.operator.jdbc.NotReadOnlyException;
import static java.util.Locale.ENGLISH;
import static org.postgresql.core.Utils.escapeIdentifier;

public class RedshiftConnection
    extends PgConnection
{
    @VisibleForTesting
    public static RedshiftConnection open(RedshiftConnectionConfig config)
    {
        return new RedshiftConnection(config.openConnection());
    }

    protected RedshiftConnection(Connection connection)
    {
        super(connection);
    }

    @Override
    public TransactionHelper getStrictTransactionHelper(String statusTableName, Duration cleanupDuration)
    {
        return new RedshiftPersistentTransactionHelper(statusTableName, cleanupDuration);
    }

    private static String escapeParam(String param)
    {
        // TODO: Implement!
        return param;
    }

    Map.Entry<String, List<Object>> buildCopyStatement(CopyConfig copyConfig)
    {
        List<Object> paramsInSql = new ArrayList<>();
        StringBuilder sb = new StringBuilder();

        Consumer<Object> addParam =
                param -> {
                    sb.append(" '?'");
                    paramsInSql.add(param);
                };

        sb.append(
                String.format("COPY %s FROM '%s'\n",
                        escapeParam(copyConfig.tableName),
                        escapeParam(copyConfig.from)));

        // credentials
        // TODO: Support ENCRYPTED
        sb.append(
                String.format("CREDENTIALS '%s'\n",
                        escapeParam(
                                String.format("aws_access_key_id=%s;aws_secret_access_key=%s;token=%s",
                                        copyConfig.accessKeyId, copyConfig.secretAccessKey, copyConfig.sessionToken))));
        // manifests
        if (copyConfig.manifest.or(false)) {
            sb.append("MANIFEST\n");
        }

        // Data Format Parameters
        if (copyConfig.csv.isPresent()) {
            sb.append("CSV");
            addParam.accept(copyConfig.csv.get());
            sb.append("\n");
        }

        if (copyConfig.delimiter.isPresent()) {
            sb.append("DELIMITER");
            addParam.accept(copyConfig.delimiter.get());
            sb.append("\n");
        }

        if (copyConfig.fixedwidth.isPresent()) {
            sb.append("FIXEDWIDTH");
            addParam.accept(copyConfig.fixedwidth.get());
            sb.append("\n");
        }

        if (copyConfig.json.isPresent()) {
            sb.append("JSON");
            addParam.accept(copyConfig.json.get());
            sb.append("\n");
        }

        if (copyConfig.avro.isPresent()) {
            sb.append("AVRO");
            addParam.accept(copyConfig.avro.get());
            sb.append("\n");
        }

        if (copyConfig.gzip.isPresent()) {
            sb.append("GZIP");
            sb.append("\n");
        }

        if (copyConfig.bzip2.isPresent()) {
            sb.append("BZIP2");
            sb.append("\n");
        }

        if (copyConfig.lzop.isPresent()) {
            sb.append("LZOP");
            sb.append("\n");
        }

        // Data Conversion Parameters
        if (copyConfig.acceptanydate.isPresent()) {
            sb.append("ACCEPTANYDATE");
            sb.append("\n");
        }

        if (copyConfig.acceptinvchars.isPresent()) {
            sb.append("ACCEPTINVCHARS");
            addParam.accept(copyConfig.acceptinvchars.get());
            sb.append("\n");
        }

        if (copyConfig.blanksasnull.isPresent()) {
            sb.append("BLANKSASNULL");
            sb.append("\n");
        }

        if (copyConfig.dateformat.isPresent()) {
            sb.append("DATEFORMAT");
            addParam.accept(copyConfig.dateformat.get());
            sb.append("\n");
        }

        if (copyConfig.emptyasnull.isPresent()) {
            sb.append("EMPTYASNULL");
            sb.append("\n");
        }

        if (copyConfig.encoding.isPresent()) {
            sb.append("ENCODING");
            addParam.accept(copyConfig.encoding.get());
            sb.append("\n");
        }

        if (copyConfig.escape.isPresent()) {
            sb.append("ESCAPE");
            sb.append("\n");
        }

        if (copyConfig.explicitIds.isPresent()) {
            sb.append("EXPLICIT_IDS");
            sb.append("\n");
        }

        if (copyConfig.fillrecord.isPresent()) {
            sb.append("FILLRECORD");
            sb.append("\n");
        }

        if (copyConfig.ignoreblanklines.isPresent()) {
            sb.append("IGNOREBLANKLINES");
            sb.append("\n");
        }

        if (copyConfig.ignoreheader.isPresent()) {
            sb.append("IGNOREHEADER");
            addParam.accept(copyConfig.encoding.get());
            sb.append("\n");
        }

        if (copyConfig.nullAs.isPresent()) {
            sb.append("NULL AS");
            addParam.accept(copyConfig.nullAs.get());
            sb.append("\n");
        }

        if (copyConfig.removequotes.isPresent()) {
            sb.append("REMOVEQUOTES");
            sb.append("\n");
        }

        if (copyConfig.roundec.isPresent()) {
            sb.append("ROUNDEC");
            sb.append("\n");
        }

        if (copyConfig.timeformat.isPresent()) {
            sb.append("TIMEFORMAT");
            addParam.accept(copyConfig.timeformat.get());
            sb.append("\n");
        }

        if (copyConfig.trimblanks.isPresent()) {
            sb.append("TRIMBLANKS");
            sb.append("\n");
        }

        if (copyConfig.truncatecolumns.isPresent()) {
            sb.append("TRUNCATECOLUMNS");
            sb.append("\n");
        }

        // Data Load Operations
        if (copyConfig.comprows.isPresent()) {
            sb.append("COMPROWS");
            addParam.accept(copyConfig.comprows.get());
            sb.append("\n");
        }

        if (copyConfig.compupdate.isPresent()) {
            sb.append("COMPUPDATE");
            addParam.accept(copyConfig.compupdate.get());
            sb.append("\n");
        }

        if (copyConfig.maxerror.isPresent()) {
            sb.append("MAXERROR");
            addParam.accept(copyConfig.maxerror.get());
            sb.append("\n");
        }

        if (copyConfig.noload.isPresent()) {
            sb.append("NOLOAD");
            sb.append("\n");
        }

        if (copyConfig.statupdate.isPresent()) {
            sb.append("STATUPDATE");
            addParam.accept(copyConfig.statupdate.get());
            sb.append("\n");
        }

        return new AbstractMap.SimpleEntry<>(sb.toString(), paramsInSql);
    }

    public class RedshiftPersistentTransactionHelper
            extends PgPersistentTransactionHelper
    {
        RedshiftPersistentTransactionHelper(String statusTableName, Duration cleanupDuration)
        {
            super(statusTableName, cleanupDuration);
        }

        @Override
        protected String buildCreateTable()
        {
            // Redshift doesn't support timestamptz type. Timestamp type is always UTC.
            return String.format(ENGLISH,
                    "CREATE TABLE IF NOT EXISTS %s" +
                    " (query_id text NOT NULL UNIQUE, created_at timestamp NOT NULL, completed_at timestamp)",
                    escapeIdent(statusTableName));
        }
    }

    @FunctionalInterface
    interface CopyConfigConfigurator
    {
        void config(CopyConfig orig);
    }

    static class CopyConfig
    {
        String accessKeyId;
        String secretAccessKey;
        String sessionToken;

        String tableName;
        Optional<String> columnList = Optional.absent();
        String from;
        Optional<Integer> readratio = Optional.absent();
        Optional<Boolean> manifest = Optional.absent();
        Optional<Boolean> encrypted = Optional.absent();
        Optional<Boolean> region = Optional.absent();

        Optional<String> csv = Optional.absent();
        Optional<String> delimiter = Optional.absent();
        Optional<String> fixedwidth = Optional.absent();
        Optional<String> json = Optional.absent();
        Optional<String> avro = Optional.absent();
        Optional<Boolean> gzip = Optional.absent();
        Optional<Boolean> bzip2 = Optional.absent();
        Optional<Boolean> lzop = Optional.absent();

        Optional<Boolean> acceptanydate = Optional.absent();
        Optional<String> acceptinvchars = Optional.absent();
        Optional<Boolean> blanksasnull = Optional.absent();
        Optional<String> dateformat = Optional.absent();
        Optional<Boolean> emptyasnull = Optional.absent();
        Optional<String> encoding = Optional.absent();
        Optional<Boolean> escape = Optional.absent();
        Optional<Boolean> explicitIds = Optional.absent();
        Optional<Boolean> fillrecord = Optional.absent();
        Optional<Boolean> ignoreblanklines = Optional.absent();
        Optional<Integer> ignoreheader = Optional.absent();
        Optional<String> nullAs = Optional.absent();
        Optional<Boolean> removequotes = Optional.absent();
        Optional<Boolean> roundec = Optional.absent();
        Optional<String> timeformat = Optional.absent();
        Optional<Boolean> trimblanks = Optional.absent();
        Optional<Boolean> truncatecolumns = Optional.absent();
        Optional<Integer> comprows = Optional.absent();
        Optional<String> compupdate = Optional.absent();
        Optional<Integer> maxerror = Optional.absent();
        Optional<Boolean> noload = Optional.absent();
        Optional<String> statupdate = Optional.absent();

        private CopyConfig()
        {
        }

        private void validate()
        {
            if (accessKeyId == null || secretAccessKey == null) {
                throw new ConfigException("'accessKeyId' or 'secretAccessKey' shouldn't be null");
            }

            if (tableName == null || from == null) {
                throw new ConfigException("'tableName' or 'from' shouldn't be null");
            }

            if (csv.isPresent()) {
                if (fixedwidth.isPresent() || removequotes.isPresent() || escape.isPresent()) {
                    throw new ConfigException("CSV cannot be used with FIXEDWIDTH, REMOVEQUOTES, or ESCAPE");
                }
            }
            if (delimiter.isPresent()) {
                if (fixedwidth.isPresent()) {
                    throw new ConfigException("DELIMITER cannot be used with FIXEDWIDTH");
                }
            }
            if (json.isPresent()) {
                if (csv.isPresent() || delimiter.isPresent() || escape.isPresent() || fillrecord.isPresent()
                    || avro.isPresent() || fixedwidth.isPresent() || ignoreblanklines.isPresent() || nullAs.isPresent()
                    || readratio.isPresent() || removequotes.isPresent()) {
                    throw new ConfigException("JSON cannot be used with CSV, DELIMITER, AVRO, ESCAPE, FILLRECORD, FIXEDWIDTH, IGNOREBLANKLINES, NULL AS, READRATIO or REMOVEQUOTES");
                }
            }
            if (avro.isPresent()) {
                // As for AVRO, the documents doesn't mention any limitation. So we left the minimum validations here
                if (csv.isPresent() || delimiter.isPresent() || json.isPresent() || fixedwidth.isPresent()) {
                    throw new ConfigException("AVRO cannot be used with CSV, DELIMITER, JSON or FIXEDWIDTH");
                }
            }
            // As for FIXEDWIDTH, combinations with other format are already validated
        }

        public static CopyConfig configure(CopyConfigConfigurator configurator)
        {
            CopyConfig copyConfig = new CopyConfig();
            configurator.config(copyConfig);
            copyConfig.validate();
            return copyConfig;
        }
    }
}
