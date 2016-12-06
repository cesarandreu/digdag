package io.digdag.standards.operator.pg;

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

import java.nio.file.Path;
import java.util.List;

public class PgOperatorFactory
        implements OperatorFactory
{
    private static final String OPERATOR_TYPE = "pg";
    private final TemplateEngine templateEngine;

    @Inject
    public PgOperatorFactory(TemplateEngine templateEngine)
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
        return new PgOperator(projectPath, request, templateEngine);
    }

    static class PgOperator
        extends AbstractJdbcJobOperator<PgConnectionConfig>
    {
        PgOperator(Path projectPath, TaskRequest request, TemplateEngine templateEngine)
        {
            super(projectPath, request, templateEngine);
        }

        @Override
        protected PgConnectionConfig configure(SecretProvider secrets, Config params)
        {
            return PgConnectionConfig.configure(secrets, params);
        }

        @Override
        protected PgConnection connect(PgConnectionConfig connectionConfig)
        {
            return PgConnection.open(connectionConfig);
        }

        @Override
        protected String type()
        {
            return OPERATOR_TYPE;
        }

        @Override
        public List<String> secretSelectors()
        {
            return ImmutableList.of("pg.*");
        }

        @Override
        protected SecretProvider getSecretsForConnectionConfig(TaskExecutionContext ctx)
        {
            return ctx.secrets().getSecrets(type());
        }
    }
}
