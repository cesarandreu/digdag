package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.google.inject.Inject;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigFactory;

public class ScheduleStarter
{
    private final ConfigFactory cf;
    private final RepositoryStoreManager rm;
    private final WorkflowExecutor exec;

    @Inject
    public ScheduleStarter(
            ConfigFactory cf,
            RepositoryStoreManager rm,
            WorkflowExecutor exec)
    {
        this.cf = cf;
        this.rm = rm;
        this.exec = exec;
    }

    public StoredSession start(int workflowId, TimeZone timeZone, ScheduleTime time)
    {
        StoredWorkflowSourceWithRepository wf = rm.getWorkflowDetailsById(workflowId);

        Session trigger = createScheduleSession(cf, timeZone, time.getScheduleTime());

        return exec.submitWorkflow(
                wf,
                trigger,
                SessionRelation.ofWorkflow(wf.getRepository().getSiteId(), wf.getRepository().getId(), wf.getId()),
                time.getRunTime());
    }

    private static Session createScheduleSession(ConfigFactory cf,
            TimeZone timeZone, Date scheduleTime)
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
        df.setTimeZone(timeZone);
        String sessionName = df.format(scheduleTime);

        Config sessionParams = cf.create()
            .set("schedule_time", scheduleTime.getTime() / 1000)
            .set("timezone", timeZone.getID());
            //.set("time_zone_offset", /*how to calculate using TimeZone API? needs joda-time?*/)

        return Session.sessionBuilder()
            .name(sessionName)
            .params(sessionParams)
            .options(SessionOptions.empty())
            .build();
    }
}