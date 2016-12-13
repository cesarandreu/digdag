package io.digdag.standards.operator.redshift;

import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.spi.Operator;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import io.digdag.standards.operator.jdbc.JdbcOpTestHelper;
import io.digdag.standards.operator.pg.PgOperatorFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedshiftLoadOperatorFactoryTest
{
    private JdbcOpTestHelper testHelper = new JdbcOpTestHelper();
    private RedshiftLoadOperatorFactory operatorFactory;

    @Before
    public void setUp()
    {
        operatorFactory = testHelper.injector().getInstance(RedshiftLoadOperatorFactory.class);
    }

    @Test
    public void getKey()
    {
        assertThat(operatorFactory.getType(), is("redshift_load"));
    }

    @Test
    public void newOperator()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table_name", "my_table",
                "from", "s3://my-bucket/my-path",
                "csv", ""
        );
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        Operator operator = operatorFactory.newOperator(testHelper.projectPath(), taskRequest);
        assertThat(operator, is(instanceOf(RedshiftLoadOperatorFactory.RedshiftLoadOperator.class)));
    }

    @Test
    public void createCopyConfig()
            throws IOException
    {
        Map<String, Object> configInput = ImmutableMap.of(
                "table_name", "my_table",
                "from", "s3://my-bucket/my-path",
                "csv", ""
        );
        TaskRequest taskRequest = testHelper.createTaskRequest(configInput, Optional.absent());
        RedshiftLoadOperatorFactory.RedshiftLoadOperator operator = (RedshiftLoadOperatorFactory.RedshiftLoadOperator) operatorFactory.newOperator(testHelper.projectPath(), taskRequest);
        assertThat(operator, is(instanceOf(RedshiftLoadOperatorFactory.RedshiftLoadOperator.class)));

        Credentials credentials = mock(Credentials.class);
        when(credentials.getAccessKeyId()).thenReturn("my-access-key-id");
        when(credentials.getSecretAccessKey()).thenReturn("my-secret-access-key");
        when(credentials.getSessionToken()).thenReturn("my-session-token");

        RedshiftConnection.CopyConfig copyConfig = operator.createCopyConfig(testHelper.createConfig(configInput), credentials);

        Connection connection = mock(Connection.class);

        RedshiftConnection redshiftConnection = new RedshiftConnection(connection);
        Map.Entry<String, List<Object>> result = redshiftConnection.buildCopyStatement(copyConfig);

        System.out.println(result.getKey());
    }
}