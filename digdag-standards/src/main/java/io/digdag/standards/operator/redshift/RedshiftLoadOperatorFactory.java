package io.digdag.standards.operator.redshift;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.policy.Policy;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.Credentials;
import com.amazonaws.services.securitytoken.model.GetFederationTokenRequest;
import com.amazonaws.services.securitytoken.model.GetFederationTokenResult;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigElement;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.*;
import io.digdag.standards.operator.jdbc.*;
import io.digdag.util.DurationParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.stream.Stream;

import static io.digdag.spi.TaskExecutionException.buildExceptionErrorConfig;

public class RedshiftLoadOperatorFactory
        implements OperatorFactory
{
    private static final String POLL_INTERVAL = "pollInterval";
    private static final int INITIAL_POLL_INTERVAL = 1;
    private static final int MAX_POLL_INTERVAL = 1200;

    private static final String OPERATOR_TYPE = "redshift_load";
    private final TemplateEngine templateEngine;

    private static final String QUERY_ID = "queryId";

    @Inject
    public RedshiftLoadOperatorFactory(TemplateEngine templateEngine)
    {
        this.templateEngine = templateEngine;
    }

    public String getType()
    {
        return OPERATOR_TYPE;
    }

    @Override
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new RedshiftLoadOperator(projectPath, request, templateEngine);
    }


    @VisibleForTesting
    static class RedshiftLoadOperator
        extends AbstractJdbcJobOperator<RedshiftConnectionConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        @VisibleForTesting
        RedshiftLoadOperator(Path projectPath, TaskRequest request, TemplateEngine templateEngine)
        {
            super(projectPath, request, templateEngine);
        }

        /* TODO: This method name should be connectionConfig() or something? */
        @Override
        protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return RedshiftConnectionConfig.configure(secrets, params);
        }

        /* TODO: This method should be in XxxxConnectionConfig ? */
        @Override
        protected RedshiftConnection connect(RedshiftConnectionConfig connectionConfig)
        {
            return RedshiftConnection.open(connectionConfig);
        }

        @Override
        protected String type()
        {
            return OPERATOR_TYPE;
        }

        @Override
        protected boolean strictTransaction(Config params)
        {
            // TODO: DRY
            if (params.getOptional("strict_transaction", Boolean.class).isPresent()) {
                // RedShift doesn't support "SELECT FOR UPDATE" statement
                logger.warn("'strict_transaction' is ignored in 'redshift' operator");
            }
            return false;
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("aws.*");
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig(TaskExecutionContext ctx)
        {
            return ctx.secrets().getSecrets("aws.redshift");
        }

        @VisibleForTesting
        Credentials createSessionCredential(SecretProvider secretProvider)
        {
            SecretProvider awsSecrets = secretProvider.getSecrets("aws");
            SecretProvider redshiftSecrets = awsSecrets.getSecrets("redshift");
            SecretProvider redshiftLoadSecrets = awsSecrets.getSecrets("redshift_load");

            String keyOfAccess = "access-key-id";
            String accessKeyId =
                    redshiftLoadSecrets.getSecretOptional(keyOfAccess)
                    .or(redshiftSecrets.getSecretOptional(keyOfAccess))
                    .or(() -> awsSecrets.getSecret(keyOfAccess));

            String keyOfSecret = "secret-access-key";
            String secretAccessKey =
                    redshiftLoadSecrets.getSecretOptional(keyOfSecret)
                    .or(redshiftSecrets.getSecretOptional(keyOfSecret))
                    .or(() -> awsSecrets.getSecret(keyOfSecret));

            // In real applications, the following code is part of your trusted code. It has
            // your security credentials you use to obtain temporary security credentials.
            AWSSecurityTokenServiceClient stsClient =
                    new AWSSecurityTokenServiceClient(new BasicAWSCredentials(accessKeyId, secretAccessKey));

            GetFederationTokenRequest federationTokenRequest = new GetFederationTokenRequest();
            // TODO: This should be configurable?
            federationTokenRequest.setDurationSeconds(3600 * 6);
            federationTokenRequest.setName("Digdag Redshift Operator");

            // Define the policy and add to the request.
            Policy policy = new Policy();
            // Define the policy here.
            // Add the policy to the request.
            federationTokenRequest.setPolicy(policy.toJson());

            GetFederationTokenResult federationTokenResult =
                    stsClient.getFederationToken(federationTokenRequest);

            return federationTokenResult.getCredentials();
        }

        @VisibleForTesting
        RedshiftConnection.CopyConfig createCopyConfig(Config config, Credentials sessionCredential)
        {
            return RedshiftConnection.CopyConfig.configure(
                    copyConfig -> {
                        copyConfig.accessKeyId = sessionCredential.getAccessKeyId();
                        copyConfig.secretAccessKey = sessionCredential.getSecretAccessKey();
                        copyConfig.sessionToken = sessionCredential.getSessionToken();

                        copyConfig.tableName = config.get("table_name", String.class);
                        copyConfig.columnList = config.getOptional("column_list", String.class);
                        copyConfig.from = config.get("from", String.class);
                        copyConfig.readratio = config.getOptional("readratio", Integer.class);
                        copyConfig.manifest = config.getOptional("manifest", Boolean.class);
                        copyConfig.encrypted = config.getOptional("encrypted", Boolean.class);
                        copyConfig.region = config.getOptional("region", Boolean.class);

                        copyConfig.csv = config.getOptional("csv", String.class);
                        copyConfig.delimiter = config.getOptional("delimiter", String.class);
                        copyConfig.fixedwidth = config.getOptional("fixedwidth", String.class);
                        copyConfig.json = config.getOptional("json", String.class);
                        copyConfig.avro = config.getOptional("avro", String.class);
                        copyConfig.gzip = config.getOptional("gzip", Boolean.class);
                        copyConfig.bzip2 = config.getOptional("bzip2", Boolean.class);
                        copyConfig.lzop = config.getOptional("lzop", Boolean.class);

                        copyConfig.acceptanydate = config.getOptional("acceptanydate", Boolean.class);
                        copyConfig.acceptinvchars = config.getOptional("acceptinvchars", String.class);
                        copyConfig.blanksasnull = config.getOptional("blanksasnull", Boolean.class);
                        copyConfig.dateformat = config.getOptional("dateformat", String.class);
                        copyConfig.emptyasnull = config.getOptional("emptyasnull", Boolean.class);
                        copyConfig.encoding = config.getOptional("encoding", String.class);
                        copyConfig.escape = config.getOptional("escape", Boolean.class);
                        copyConfig.explicitIds = config.getOptional("explicit_ids", Boolean.class);
                        copyConfig.fillrecord = config.getOptional("fillrecord", Boolean.class);
                        copyConfig.ignoreblanklines = config.getOptional("ignoreblanklines", Boolean.class);
                        copyConfig.ignoreheader = config.getOptional("ignoreheader", Integer.class);
                        copyConfig.nullAs = config.getOptional("null_as", String.class);
                        copyConfig.removequotes = config.getOptional("removequotes", Boolean.class);
                        copyConfig.roundec = config.getOptional("roundec", Boolean.class);
                        copyConfig.timeformat = config.getOptional("timeformat", String.class);
                        copyConfig.trimblanks = config.getOptional("trimblanks", Boolean.class);
                        copyConfig.truncatecolumns = config.getOptional("truncatecolumns", Boolean.class);
                        copyConfig.comprows = config.getOptional("comprows", Integer.class);
                        copyConfig.compupdate = config.getOptional("compupdate", String.class);
                        copyConfig.maxerror = config.getOptional("maxerror", Integer.class);
                        copyConfig.noload = config.getOptional("noload", Boolean.class);
                        copyConfig.statupdate = config.getOptional("statupdate", String.class);
                    });
        }

        @Override
        protected TaskResult run(TaskExecutionContext ctx, Config params, Config state, RedshiftConnectionConfig connectionConfig)
        {
            Credentials sessionCredential = createSessionCredential(ctx.secrets());

            RedshiftConnection.CopyConfig copyConfig = createCopyConfig(params, sessionCredential);

            boolean strictTransaction = strictTransaction(params);

            String statusTableName;
            DurationParam statusTableCleanupDuration;
            if (strictTransaction) {
                statusTableName = params.get("status_table", String.class, "__digdag_status");
                statusTableCleanupDuration = params.get("status_table_cleanup", DurationParam.class,
                        DurationParam.of(Duration.ofHours(24)));
            }
            else {
                statusTableName = null;
                statusTableCleanupDuration = null;
            }

            UUID queryId;
            // generate query id
            if (!state.has(QUERY_ID)) {
                // this is the first execution of this task
                logger.debug("Generating query id for a new {} task", type());
                queryId = UUID.randomUUID();
                state.set(QUERY_ID, queryId);
                throw TaskExecutionException.ofNextPolling(0, ConfigElement.copyOf(state));
            }
            queryId = state.get(QUERY_ID, UUID.class);

            try (RedshiftConnection connection = connect(connectionConfig)) {
                Map.Entry<String, List<Object>> result = connection.buildCopyStatement(copyConfig);
                String query = result.getKey();
                List<Object> paramsInSql = result.getValue();

                Exception statementError = connection.validateStatement(query);
                if (statementError != null) {
                    throw new ConfigException("Given query is invalid", statementError);
                }

                TransactionHelper txHelper;
                if (strictTransaction) {
                    txHelper = connection.getStrictTransactionHelper(statusTableName,
                            statusTableCleanupDuration.getDuration());
                }
                else {
                    txHelper = new NoTransactionHelper();
                }

                txHelper.prepare();

                boolean executed = txHelper.lockedTransaction(queryId, () -> {
                    connection.executeUpdate(query, paramsInSql);
                });

                if (!executed) {
                    logger.debug("Query is already completed according to status table. Skipping statement execution.");
                }

                try {
                    txHelper.cleanup();
                }
                catch (Exception ex) {
                    logger.warn("Error during cleaning up status table. Ignoring.", ex);
                }

                return TaskResult.defaultBuilder(request).build();
            }
            catch (LockConflictException ex) {
                int pollingInterval = state.get(POLL_INTERVAL, Integer.class, INITIAL_POLL_INTERVAL);
                // Set next interval for exponential backoff
                state.set(POLL_INTERVAL, Math.min(pollingInterval * 2, MAX_POLL_INTERVAL));
                throw TaskExecutionException.ofNextPolling(pollingInterval, ConfigElement.copyOf(state));
            }
            catch (DatabaseException ex) {
                // expected error that should suppress stacktrace by default
                String message = String.format("%s [%s]", ex.getMessage(), ex.getCause().getMessage());
                throw new TaskExecutionException(message, buildExceptionErrorConfig(ex));
            }
        }
    }
}
