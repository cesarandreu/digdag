package io.digdag.standards.operator.redshift;

import com.amazonaws.services.securitytoken.model.Credentials;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.digdag.client.DigdagClient;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.SecretProvider;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TemplateEngine;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RedshiftLoadOperatorTest
{
    @Test
    public void createCopyStatement()
            throws Exception
    {
        TemporaryFolder tempFolder = new TemporaryFolder();
        tempFolder.create();
        File projectDir = tempFolder.getRoot();
        projectDir.deleteOnExit();

        TaskRequest taskRequest = mock(TaskRequest.class);
        TemplateEngine templateEngine = mock(TemplateEngine.class);
        RedshiftLoadOperatorFactory.RedshiftLoadOperator operator = new RedshiftLoadOperatorFactory.RedshiftLoadOperator(projectDir.toPath(), taskRequest, templateEngine);

        Config config = new ConfigFactory(DigdagClient.objectMapper())
                .create(ImmutableMap
                        .builder()
                        .put("table_name", "my_table")
                        .put("from", "s3://my-bucket/my-path")
                        .put("csv", "")
                        .build()
                );

        /*
        SecretProvider redshiftSecrets = mock(SecretProvider.class);
        when(redshiftSecrets.getSecretOptional("access-key-id"))
                .thenReturn(Optional.of("test-redshift-access-key-id"));
        when(redshiftSecrets.getSecretOptional("secret-access-key"))
                .thenReturn(Optional.of("test-redshift-secret-access-key"));

        SecretProvider redshiftLoadSecrets = mock(SecretProvider.class);
        when(redshiftLoadSecrets.getSecretOptional("access-key-id"))
                .thenReturn(Optional.of("test-redshift-load-access-key-id"));
        when(redshiftLoadSecrets.getSecretOptional("secret-access-key"))
                .thenReturn(Optional.of("test-redshift-load-secret-access-key"));

        SecretProvider awsSecrets = mock(SecretProvider.class);
        when(awsSecrets.getSecretOptional(anyString())).thenReturn(Optional.absent());
        when(awsSecrets.getSecrets("redshift")).thenReturn(redshiftSecrets);
        when(awsSecrets.getSecrets("redshift_load")).thenReturn(redshiftLoadSecrets);

        SecretProvider secrets = mock(SecretProvider.class);
        when(secrets.getSecrets("aws")).thenReturn(awsSecrets);
        */

        Credentials credentials = mock(Credentials.class);
        when(credentials.getAccessKeyId()).thenReturn("my-access-key-id");
        when(credentials.getSecretAccessKey()).thenReturn("my-secret-access-key");
        when(credentials.getSessionToken()).thenReturn("my-session-token");

        RedshiftConnection.CopyConfig copyConfig = operator.createCopyConfig(config, credentials);

        Connection connection = mock(Connection.class);

        RedshiftConnection redshiftConnection = new RedshiftConnection(connection);
        Map.Entry<String, List<Object>> result = redshiftConnection.buildCopyStatement(copyConfig);

        System.out.println(result.getKey());
    }
}