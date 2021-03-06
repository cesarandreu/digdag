package io.digdag.core.database.migrate;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.skife.jdbi.v2.Handle;

public interface Migration
{
    static Pattern MIGRATION_NAME_PATTERN = Pattern.compile("Migration_([0-9]{14})_([A-Za-z]+)");

    default String getVersion()
    {
        Matcher m = MIGRATION_NAME_PATTERN.matcher(getClass().getSimpleName());
        if (!m.matches()) {
            throw new AssertionError("Invalid migration class name: " + getClass().getSimpleName());
        }
        return m.group(1);
    }

    void migrate(Handle handle, MigrationContext context);
}
