package io.digdag.util;

import io.digdag.spi.SecretProvider;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserSecretTemplate
{
    private static final Pattern TEMPLATE_PATTERN = Pattern.compile("\\$\\{secret:([\\s\\S]+?)\\}");

    public static UserSecretTemplate of(String source)
    {
        return new UserSecretTemplate(source);
    }

    private final String source;

    private UserSecretTemplate(String source)
    {
        this.source = source;
    }

    public boolean containsSecrets()
    {
        return TEMPLATE_PATTERN.matcher(source).find();
    }

    public String format(SecretProvider secrets)
    {
        return replaceAll(source, TEMPLATE_PATTERN, (m) -> secrets.getSecret(m.group(1).trim()));
    }

    private static String replaceAll(String source, Pattern pattern, Function<Matcher, String> converter)
    {
        StringBuffer sb = new StringBuffer();

        Matcher m = pattern.matcher(source);
        while (m.find()) {
            m.appendReplacement(sb, converter.apply(m));
        }
        m.appendTail(sb);

        return sb.toString();
    }
}
