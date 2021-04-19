package com.github.kagkarlsson.scheduler;

import com.github.kagkarlsson.jdbc.*;
import com.github.kagkarlsson.scheduler.exceptions.ExecutionException;
import com.github.kagkarlsson.scheduler.jdbc.JdbcTaskRepository;
import com.github.kagkarlsson.scheduler.task.Execution;
import com.github.kagkarlsson.scheduler.task.TaskInstance;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.commons.util.ReflectionUtils;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static java.util.Collections.*;
import static org.apache.commons.lang3.RandomStringUtils.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class JdbcTaskRepositoryExceptionsTest {
    JdbcTaskRepository jdbcTaskRepository;

    @Mock
    JdbcRunner mockJdbcRunner;

    private String expectedTableName;

    @BeforeEach
    public void setup() throws NoSuchFieldException, IllegalAccessException {
        expectedTableName = randomAlphanumeric(5);
        jdbcTaskRepository = new JdbcTaskRepository(null, true, null, expectedTableName, null, null);

        Field jdbcRunnerField = JdbcTaskRepository.class.getDeclaredField("jdbcRunner");
        ReflectionUtils.makeAccessible(jdbcRunnerField);
        jdbcRunnerField.set(jdbcTaskRepository, mockJdbcRunner);
    }

    @Test
    public void createIfNotExistsFailsToAddNewTask() {
        when(mockJdbcRunner.query(ArgumentMatchers.eq("select * from " + expectedTableName + " where task_name = ? and task_instance = ?"), any(PreparedStatementSetter.class), (ResultSetMapper<List<Execution>>) any(ResultSetMapper.class)))
            .thenReturn(emptyList());
        SQLRuntimeException rootCause = new SQLRuntimeException("SQL GO BOOM!!!");
        when(mockJdbcRunner.execute(ArgumentMatchers.eq("insert into " + expectedTableName + "(task_name, task_instance, task_data, execution_time, picked, version) values(?, ?, ?, ?, ?, ?)"), any(PreparedStatementSetter.class)))
            .thenThrow(rootCause);

        ExecutionException actualException = assertThrows(ExecutionException.class, () -> {
            jdbcTaskRepository.createIfNotExists(new Execution(Instant.now(), new TaskInstance(randomAlphanumeric(10), randomAlphanumeric(10))));
        });
        assertEquals("Failed to add new execution.", actualException.getMessage());
        assertEquals(rootCause, actualException.getCause());
    }

    @Test
    public void getExecutionIsMoreThanOne() {
        TaskInstance expectedTaskInstance = new TaskInstance(randomAlphanumeric(10), randomAlphanumeric(10));

        when(mockJdbcRunner.query(ArgumentMatchers.eq("select * from " + expectedTableName + " where task_name = ? and task_instance = ?"), any(PreparedStatementSetter.class), (ResultSetMapper<List<Execution>>) any(ResultSetMapper.class)))
            .thenReturn(Lists.newArrayList(new Execution(Instant.now(), expectedTaskInstance), new Execution(Instant.now(), expectedTaskInstance)));

        ExecutionException actualException = assertThrows(ExecutionException.class, () -> {
            jdbcTaskRepository.getExecution(expectedTaskInstance);
        });
        assertEquals("Found more than one matching execution for task name/id combination: '" + expectedTaskInstance.getTaskName() + "'/'" + expectedTaskInstance.getId() + "'", actualException.getMessage());
    }

    @ParameterizedTest(name = "Remove ends up removing {0} records")
    @ValueSource(ints = {0, 2})
    public void removesUnexpectedNumberOfRows(int removalCount) {
        when(mockJdbcRunner.execute(ArgumentMatchers.eq("delete from " + expectedTableName + " where task_name = ? and task_instance = ? and version = ?"), any(PreparedStatementSetter.class)))
            .thenReturn(removalCount);

        ExecutionException actualException = assertThrows(ExecutionException.class, () -> {
            jdbcTaskRepository.remove(new Execution(Instant.now(), new TaskInstance(randomAlphanumeric(10), randomAlphanumeric(10))));
        });
        assertEquals("Expected one execution to be removed, but removed " + removalCount + ". Indicates a bug.", actualException.getMessage());
    }

    @ParameterizedTest(name = "Reschedule without new data ends up modifying {0} records")
    @ValueSource(ints = {0, 2})
    public void rescheduleUpdatesUnexpectedNumberOfRowsWithoutNewData(int updateCount) {
        when(mockJdbcRunner.execute(ArgumentMatchers.eq("update " + expectedTableName + " set " +
                "picked = ?, " +
                "picked_by = ?, " +
                "last_heartbeat = ?, " +
                "last_success = ?, " +
                "last_failure = ?, " +
                "consecutive_failures = ?, " +
                "execution_time = ?, " +
                "version = version + 1 " +
                "where task_name = ? " +
                "and task_instance = ? " +
                "and version = ?"),
            any(PreparedStatementSetter.class)))
            .thenReturn(updateCount);

        ExecutionException actualException = assertThrows(ExecutionException.class, () -> {
            jdbcTaskRepository.reschedule(
                new Execution(Instant.now(), new TaskInstance(randomAlphanumeric(10), randomAlphanumeric(10))),
                Instant.now(),
                null,
                null,
                0
            );
        });
        assertEquals("Expected one execution to be updated, but updated " + updateCount + ". Indicates a bug.", actualException.getMessage());
    }

    @ParameterizedTest(name = "Reschedule with new data ends up modifying {0} records")
    @ValueSource(ints = {0, 2})
    public void rescheduleUpdatesUnexpectedNumberOfRowsWithNewData(int updateCount) {
        when(mockJdbcRunner.execute(ArgumentMatchers.eq("update " + expectedTableName + " set " +
                "picked = ?, " +
                "picked_by = ?, " +
                "last_heartbeat = ?, " +
                "last_success = ?, " +
                "last_failure = ?, " +
                "consecutive_failures = ?, " +
                "execution_time = ?, " +
                "task_data = ?, " +
                "version = version + 1 " +
                "where task_name = ? " +
                "and task_instance = ? " +
                "and version = ?"),
            any(PreparedStatementSetter.class)))
            .thenReturn(updateCount);

        ExecutionException actualException = assertThrows(ExecutionException.class, () -> {
            jdbcTaskRepository.reschedule(
                new Execution(Instant.now(), new TaskInstance(randomAlphanumeric(10), randomAlphanumeric(10))),
                Instant.now(),
                new HashMap(),
                null,
                null,
                0
            );
        });
        assertEquals("Expected one execution to be updated, but updated " + updateCount + ". Indicates a bug.", actualException.getMessage());
    }

}
