package io.digdag.core.workflow;

import com.google.common.base.Optional;
import io.digdag.core.Limits;
import io.digdag.core.repository.ProjectStore;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.StoredProject;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionAttempt;
import io.digdag.core.session.SessionStore;
import io.digdag.core.session.SessionTransaction;
import io.digdag.core.session.StoredSessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;

import static java.util.Locale.ENGLISH;

public class WorkflowSubmitter
{
    private final int siteId;
    private final SessionTransaction transaction;
    private final ProjectStore projectStore;
    private final SessionStore sessionStore;

    WorkflowSubmitter(int siteId, SessionTransaction transaction, ProjectStore projectStore, SessionStore sessionStore)
    {
        this.siteId = siteId;
        this.transaction = transaction;
        this.projectStore = projectStore;
        this.sessionStore = sessionStore;
    }

    public StoredSessionAttemptWithSession submitDelayedAttempt(
            AttemptRequest ar,
            Optional<Long> dependentSessionId)
        throws ResourceNotFoundException, AttemptLimitExceededException, SessionAttemptConflictException
    {
        int projId = ar.getStored().getProjectId();
        Session session = Session.of(projId, ar.getWorkflowName(), ar.getSessionTime());

        SessionAttempt attempt = SessionAttempt.of(
                ar.getRetryAttemptName(),
                ar.getSessionParams(),
                ar.getTimeZone(),
                Optional.of(ar.getStored().getWorkflowDefinitionId()));

        TaskConfig.validateAttempt(attempt);

        try {
            long activeAttempts = transaction.getActiveAttemptCount();

            if (activeAttempts + 1 > Limits.maxAttempts()) {
                throw new AttemptLimitExceededException("Too many attempts running. Limit: " + Limits.maxAttempts() + ", Current: " + activeAttempts);
            }

            return transaction.putAndLockSession(session, (store, storedSession) -> {
                StoredProject proj = projectStore.getProjectById(projId);
                if (proj.getDeletedAt().isPresent()) {
                    throw new ResourceNotFoundException(String.format(ENGLISH,
                                "Project id={} name={} is already deleted",
                                proj.getId(), proj.getName()));
                }
                StoredSessionAttempt storedAttempt = store.insertDelayedAttempt(storedSession.getId(), projId, attempt, dependentSessionId);  // this may throw ResourceConflictException
                return StoredSessionAttemptWithSession.of(siteId, storedSession, storedAttempt);
            });
        }
        catch (ResourceConflictException sessionAlreadyExists) {
            StoredSessionAttemptWithSession conflicted;
            if (ar.getRetryAttemptName().isPresent()) {
                conflicted = sessionStore
                    .getAttemptByName(session.getProjectId(), session.getWorkflowName(), session.getSessionTime(), ar.getRetryAttemptName().get());
            }
            else {
                conflicted = sessionStore
                    .getLastAttemptByName(session.getProjectId(), session.getWorkflowName(), session.getSessionTime());
            }
            throw new SessionAttemptConflictException("Session already exists", sessionAlreadyExists, conflicted);
        }
    }
}
