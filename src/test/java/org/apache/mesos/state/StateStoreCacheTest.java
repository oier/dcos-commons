package org.apache.mesos.state;

import org.apache.curator.test.TestingServer;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.curator.CuratorStateStore;
import org.apache.mesos.offer.TaskUtils;
import org.apache.mesos.testing.CuratorTestUtils;
import org.apache.mesos.testutils.TaskTestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Tests for {@link StateStoreCache}
 */
public class StateStoreCacheTest {
    private static final String ROOT_ZK_PATH = "/test-root-path";

    private static final FrameworkID FRAMEWORK_ID = FrameworkID.newBuilder().setValue("foo").build();
    private static final FrameworkID FRAMEWORK_ID2 = FrameworkID.newBuilder().setValue("foo2").build();

    private static final String PROP_KEY = "property";
    private static final byte[] PROP_VAL = "someval".getBytes(Charset.defaultCharset());
    private static final String PROP_KEY2 = "property2";
    private static final byte[] PROP_VAL2 = "someval2".getBytes(Charset.defaultCharset());

    private static final String TASK_NAME = "task";
    private static final TaskInfo TASK = TaskTestUtils.getTaskInfo(Collections.emptyList()).toBuilder()
            .setName(TASK_NAME)
            .setTaskId(TaskUtils.toTaskId(TASK_NAME))
            .build();
    private static final TaskStatus STATUS = Protos.TaskStatus.newBuilder()
            .setTaskId(TASK.getTaskId())
            .setState(TaskState.TASK_KILLING)
            .build();
    private static final String TASK_NAME2 = "task2";
    private static final TaskInfo TASK2 = TaskTestUtils.getTaskInfo(Collections.emptyList()).toBuilder()
            .setName(TASK_NAME2)
            .setTaskId(TaskUtils.toTaskId(TASK_NAME2))
            .build();
    private static final TaskStatus STATUS2 = Protos.TaskStatus.newBuilder()
            .setTaskId(TASK2.getTaskId())
            .setState(TaskState.TASK_ERROR)
            .build();

    private static TestingServer testZk;
    private StateStore store;
    private StateStoreCache cache;

    @BeforeClass
    public static void beforeAll() throws Exception {
        testZk = new TestingServer();
    }

    @Before
    public void beforeEach() throws Exception {
        CuratorTestUtils.clear(testZk);
        store = new CuratorStateStore(ROOT_ZK_PATH, testZk.getConnectString());
        cache = new StateStoreCache(store);
    }

    @After
    public void afterEach() {
        ((CuratorStateStore) store).closeForTesting();
    }

    @Test
    public void testFrameworkIdSingleThread() {
        cache.consistencyCheckForTests();
        assertFalse(cache.fetchFrameworkId().isPresent());
        cache.storeFrameworkId(FRAMEWORK_ID);
        cache.consistencyCheckForTests();
        assertEquals(FRAMEWORK_ID, cache.fetchFrameworkId().get());
        cache.storeFrameworkId(FRAMEWORK_ID2);
        cache.consistencyCheckForTests();
        assertEquals(FRAMEWORK_ID2, cache.fetchFrameworkId().get());
        cache.clearFrameworkId();
        cache.consistencyCheckForTests();
        assertFalse(cache.fetchFrameworkId().isPresent());
    }

    @Test
    public void testFrameworkIdMultiThread() throws InterruptedException {
        runThreads(new Runnable() {
            @Override
            public void run() {
                cache.consistencyCheckForTests();
                cache.storeFrameworkId(FRAMEWORK_ID);
                cache.consistencyCheckForTests();
                cache.storeFrameworkId(FRAMEWORK_ID2);
                cache.consistencyCheckForTests();
                cache.clearFrameworkId();
                cache.consistencyCheckForTests();
            }
        });
    }

    @Test
    public void testPropertiesSingleThread() {
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchPropertyKeys().isEmpty());
        cache.storeProperty(PROP_KEY, PROP_VAL);
        cache.consistencyCheckForTests();
        assertEquals(1, cache.fetchPropertyKeys().size());
        assertEquals(PROP_VAL, cache.fetchProperty(PROP_KEY));
        cache.storeProperty(PROP_KEY2, PROP_VAL2);
        cache.consistencyCheckForTests();
        assertEquals(2, cache.fetchPropertyKeys().size());
        assertEquals(PROP_VAL, cache.fetchProperty(PROP_KEY));
        assertEquals(PROP_VAL2, cache.fetchProperty(PROP_KEY2));
        cache.clearProperty(PROP_KEY);
        cache.consistencyCheckForTests();
        assertEquals(1, cache.fetchPropertyKeys().size());
        assertEquals(PROP_VAL2, cache.fetchProperty(PROP_KEY2));
        cache.clearProperty(PROP_KEY2);
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchPropertyKeys().isEmpty());
    }

    @Test
    public void testPropertiesMultiThread() throws InterruptedException {
        runThreads(new Runnable() {
            @Override
            public void run() {
                cache.consistencyCheckForTests();
                cache.storeProperty(PROP_KEY, PROP_VAL);
                cache.consistencyCheckForTests();
                cache.storeProperty(PROP_KEY2, PROP_VAL2);
                cache.consistencyCheckForTests();
                cache.clearProperty(PROP_KEY);
                cache.consistencyCheckForTests();
                cache.clearProperty(PROP_KEY2);
                cache.consistencyCheckForTests();
            }
        });
    }

    @Test
    public void testTaskInfoSingleThread() {
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchTaskNames().isEmpty());
        cache.storeTasks(Arrays.asList(TASK));
        cache.consistencyCheckForTests();
        assertEquals(1, cache.fetchTaskNames().size());
        assertEquals(TASK, cache.fetchTask(TASK_NAME).get());
        cache.storeTasks(Arrays.asList(TASK2));
        cache.consistencyCheckForTests();
        assertEquals(2, cache.fetchTaskNames().size());
        assertEquals(TASK, cache.fetchTask(TASK_NAME).get());
        assertEquals(TASK2, cache.fetchTask(TASK_NAME2).get());
        cache.clearTask(TASK_NAME);
        cache.consistencyCheckForTests();
        assertEquals(1, cache.fetchTaskNames().size());
        assertEquals(TASK2, cache.fetchTask(TASK_NAME2).get());
        cache.clearTask(TASK_NAME2);
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchTaskNames().isEmpty());
        cache.storeTasks(Arrays.asList(TASK, TASK2));
        cache.consistencyCheckForTests();
        assertEquals(2, cache.fetchTaskNames().size());
        assertEquals(TASK, cache.fetchTask(TASK_NAME).get());
        assertEquals(TASK2, cache.fetchTask(TASK_NAME2).get());
        cache.clearTask(TASK_NAME);
        cache.clearTask(TASK_NAME2);
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchTaskNames().isEmpty());
    }

    @Test
    public void testTaskInfoMultiThread() throws InterruptedException {
        runThreads(new Runnable() {
            @Override
            public void run() {
                cache.consistencyCheckForTests();
                cache.storeTasks(Arrays.asList(TASK));
                cache.consistencyCheckForTests();
                cache.storeTasks(Arrays.asList(TASK2));
                cache.consistencyCheckForTests();
                cache.clearTask(TASK_NAME);
                cache.consistencyCheckForTests();
                cache.clearTask(TASK_NAME2);
                cache.consistencyCheckForTests();
                cache.storeTasks(Arrays.asList(TASK, TASK2));
                cache.consistencyCheckForTests();
                cache.clearTask(TASK_NAME);
                cache.clearTask(TASK_NAME2);
                cache.consistencyCheckForTests();
            }
        });
    }

    @Test
    public void testTaskStatusSingleThread() {
        cache.consistencyCheckForTests();
        cache.storeTasks(Arrays.asList(TASK, TASK2));
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchStatuses().isEmpty());
        cache.storeStatus(STATUS);
        cache.consistencyCheckForTests();
        assertEquals(1, cache.fetchStatuses().size());
        assertEquals(STATUS, cache.fetchStatus(TASK_NAME).get());
        cache.storeStatus(STATUS2);
        cache.consistencyCheckForTests();
        assertEquals(2, cache.fetchStatuses().size());
        assertEquals(STATUS, cache.fetchStatus(TASK_NAME).get());
        assertEquals(STATUS2, cache.fetchStatus(TASK_NAME2).get());
        cache.clearTask(TASK_NAME);
        cache.consistencyCheckForTests();
        assertEquals(1, cache.fetchStatuses().size());
        assertEquals(STATUS2, cache.fetchStatus(TASK_NAME2).get());
        cache.clearTask(TASK_NAME2);
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchStatuses().isEmpty());
        cache.storeTasks(Arrays.asList(TASK, TASK2));
        cache.consistencyCheckForTests();
        cache.storeStatus(STATUS);
        cache.consistencyCheckForTests();
        cache.storeStatus(STATUS2);
        cache.consistencyCheckForTests();
        assertEquals(2, cache.fetchStatuses().size());
        assertEquals(STATUS, cache.fetchStatus(TASK_NAME).get());
        assertEquals(STATUS2, cache.fetchStatus(TASK_NAME2).get());
        cache.clearTask(TASK_NAME);
        cache.clearTask(TASK_NAME2);
        cache.consistencyCheckForTests();
        assertTrue(cache.fetchStatuses().isEmpty());
    }

    @Test
    public void testTaskStatusMultiThread() throws InterruptedException {
        // store tasks up-front so that status updates don't throw errors:
        cache.consistencyCheckForTests();
        cache.storeTasks(Arrays.asList(TASK, TASK2));
        cache.consistencyCheckForTests();
        runThreads(new Runnable() {
            @Override
            public void run() {
                // can't wipe tasks since that'll lead to errors due to storing taskstatus while taskinfo is missing
                cache.consistencyCheckForTests();
                cache.storeStatus(STATUS);
                cache.consistencyCheckForTests();
                cache.storeStatus(STATUS2);
                cache.consistencyCheckForTests();
                cache.storeTasks(Arrays.asList(TASK, TASK2));
                cache.consistencyCheckForTests();
                cache.storeStatus(STATUS);
                cache.consistencyCheckForTests();
                cache.storeStatus(STATUS2);
                cache.consistencyCheckForTests();
            }
        });
    }

    @Test
    public void testFrameworkIdFailures() {
        assertTrue(false);
    }

    @Test
    public void testPropertyFailures() {
        assertTrue(false);
    }

    @Test
    public void testTaskInfoFailures() {
        assertTrue(false);
    }

    @Test
    public void testTaskStatusFailures() {
        assertTrue(false);
    }

    private static void runThreads(Runnable r) throws InterruptedException {
        final String errorLock = "lock";
        final List<Throwable> errors = new ArrayList<>();
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Thread t = new Thread(r);
            t.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    synchronized (errorLock) {
                        errors.add(e);
                    }
                }
            });
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {
            t.join();
        }
        assertTrue(errors.toString(), errors.isEmpty());
    }
}
