package io.digdag.standards.operator.redshift;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.AbstractJdbcJobOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

public class RedshiftOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "redshift";
    private final TemplateEngine templateEngine;

    @Inject
    public RedshiftOperatorFactory(TemplateEngine templateEngine)
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
        return new RedshiftOperator(projectPath, request, templateEngine);
    }

    private static class RedshiftOperator
        extends AbstractJdbcJobOperator<RedshiftConnectionConfig>
    {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        RedshiftOperator(Path projectPath, TaskRequest request, TemplateEngine templateEngine)
        {
            super(projectPath, request, templateEngine);
        }

        @Override
        protected RedshiftConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return RedshiftConnectionConfig.configure(secrets, params);
        }

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
            return ctx.secrets().getSecrets("aws.readshift");
        }
    }
}
