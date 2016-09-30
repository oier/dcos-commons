package org.apache.mesos.state;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.curator.CuratorStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

/**
 * Thread-safe caching layer for an underlying {@link StateStore}.
 *
 * Writes are automatically forwarded to the underlying instance, while reads prioritize the local
 * instance. In order to maintain consistency, there should only be one StateStoreCache object per
 * process. In practice this works because there should only be one scheduler task/process
 * accessing the state data at any given time.
 *
 * Implementation note: All write operations always invoke the underlying storage before updating
 * the local cache. This avoids creating an inconsistent cache state if writing to the underlying
 * persistent store fails.
 */
public class StateStoreCache implements StateStore {

    private static final Logger logger = LoggerFactory.getLogger(CuratorStateStore.class);

    private static final ReadWriteLock _lock = new ReentrantReadWriteLock();
    private static final Lock readLock = _lock.readLock();
    private static final Lock writeLock = _lock.writeLock();

    private static StateStoreCache instance = null;

    private final StateStore store;

    private Optional<FrameworkID> frameworkId;

    private Map<String, TaskID> nameToId = new HashMap<>();
    private Map<TaskID, TaskInfo> idToTask = new HashMap<>();
    private Map<TaskID, TaskStatus> idToStatus = new HashMap<>();

    private Map<String, byte[]> properties = new HashMap<>();

    /**
     * Returns a cache instance. To ensure consistency, only one singleton cache instance may exist
     * in the process at a time. This function may be called multiple times, but only if the same
     * {@link StateStore} instance is provided each time.
     */
    public static StateStore getInstance(StateStore store) {
        if (instance == null) {
            instance = new StateStoreCache(store);
        } else if (instance.store != store) {
            // Disallow subsequent calls to getInstance() with different instances of StateStore.
            throw new IllegalStateException(String.format(
                    "StateStoreCache may only be used against a single instance of StateStore. " +
                    "got[%s] expected[%s]", store, instance.store));
        }
        return instance;
    }

    @VisibleForTesting
    StateStoreCache(StateStore store) throws StateStoreException {
        this.store = store;

        // Use bulk fetches to initialize cache with underlying storage state:
        frameworkId = store.fetchFrameworkId();
        for (TaskInfo task : store.fetchTasks()) {
            nameToId.put(task.getName(), task.getTaskId());
            idToTask.put(task.getTaskId(), task);
        }
        for (TaskStatus status : store.fetchStatuses()) {
            idToStatus.put(status.getTaskId(), status);
        }
        for (String key : store.fetchPropertyKeys()) {
            properties.put(key, store.fetchProperty(key));
        }
    }

    @Override
    public void storeFrameworkId(FrameworkID fwkId) throws StateStoreException {
        writeLock.lock();
        try {
            store.storeFrameworkId(fwkId);
            frameworkId = Optional.of(fwkId);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clearFrameworkId() throws StateStoreException {
        writeLock.lock();
        try {
            store.clearFrameworkId();
            frameworkId = Optional.empty();
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Optional<FrameworkID> fetchFrameworkId() throws StateStoreException {
        readLock.lock();
        try {
            return frameworkId;
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void storeTasks(Collection<TaskInfo> tasks) throws StateStoreException {
        writeLock.lock();
        try {
            store.storeTasks(tasks);
            for (TaskInfo task : tasks) {
                nameToId.put(task.getName(), task.getTaskId());
                idToTask.put(task.getTaskId(), task);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void storeStatus(TaskStatus status) throws StateStoreException {
        writeLock.lock();
        try {
            store.storeStatus(status);
            idToStatus.put(status.getTaskId(), status);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public void clearTask(String taskName) throws StateStoreException {
        writeLock.lock();
        try {
            store.clearTask(taskName);
            TaskID taskId = nameToId.remove(taskName);
            if (taskId != null) {
                idToTask.remove(taskId);
                idToStatus.remove(taskId);
            } else {
                logger.warn("Unable to find task named {} to remove. Known task names are: {}",
                        taskName, nameToId.keySet());
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public Collection<String> fetchTaskNames() throws StateStoreException {
        readLock.lock();
        try {
            return nameToId.keySet();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<TaskInfo> fetchTasks() throws StateStoreException {
        readLock.lock();
        try {
            return idToTask.values();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<TaskInfo> fetchTask(String taskName) throws StateStoreException {
        readLock.lock();
        try {
            TaskID taskId = nameToId.get(taskName);
            if (taskId == null) {
                return Optional.empty();
            }
            TaskInfo task = idToTask.get(taskId);
            if (task == null) {
                // If we have a name->ID mapping, we really should have a TaskInfo for that ID.
                throw new StateStoreException(String.format(
                        "Cache consistency error: Unable to find TaskInfo for known ID '%s'. Name=>ID[%s] TaskIDs[%s]",
                        taskName, nameToId, idToTask.keySet()));
            }
            return Optional.of(task);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Set<TaskStatus> getTaskStatuses() throws Exception {
        readLock.lock();
        try {
            return new HashSet<>(idToStatus.values());
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<TaskStatus> fetchStatuses() throws StateStoreException {
        readLock.lock();
        try {
            return idToStatus.values();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Optional<TaskStatus> fetchStatus(String taskName) throws StateStoreException {
        readLock.lock();
        try {
            TaskID taskId = nameToId.get(taskName);
            if (taskId == null) {
                // Task name doesn't exist at all.
                return Optional.empty();
            }
            TaskStatus status = idToStatus.get(taskId);
            if (status == null) {
                // Task name exists, but no status (storeTask was called but not yet storeStatus)
                return Optional.empty();
            }
            return Optional.of(status);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void storeProperty(String key, byte[] value) throws StateStoreException {
        writeLock.lock();
        try {
            store.storeProperty(key, value);
            properties.put(key, value);
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public byte[] fetchProperty(String key) throws StateStoreException {
        readLock.lock();
        try {
            return properties.get(key);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Collection<String> fetchPropertyKeys() throws StateStoreException {
        readLock.lock();
        try {
            return properties.keySet();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public void clearProperty(String key) throws StateStoreException {
        writeLock.lock();
        try {
            store.clearProperty(key);
            properties.remove(key);
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * A consistency check which may be called by tests to check for the following types of errors:
     * - Internal consistency errors (eg between nameToId/idToTask/idToStatus)
     * - External consistency errors between local state and underlying StateStore
     *
     * This function gets a state dump from the underlying StateStore and is therefore not expected
     * to be performant.
     *
     * @throws IllegalStateException in the event of any consistency failure
     */
    @VisibleForTesting
    void consistencyCheckForTests() {
        readLock.lock();
        try {
            // Phase 1: check internal consistency

            // If a name=>ID entry exists, a matching ID=>task entry must also exist (not the case for ID=>status)
            for (Map.Entry<String, TaskID> entry : nameToId.entrySet()) {
                if (!idToTask.containsKey(entry.getValue())) {
                    throw new IllegalStateException(String.format(
                            "idToTask is missing nameToId entry: %s", entry));
                }
            }
            // If an ID=>task entry exists, a matching name=>ID entry must also exist.
            for (Map.Entry<TaskID, TaskInfo> entry : idToTask.entrySet()) {
                if (!nameToId.containsValue(entry.getKey())) {
                    throw new IllegalStateException(String.format(
                            "nameToId is missing idToTask entry: %s", entry));
                }
            }
            // If an ID=>status entry exists, a matching name=>ID entry must also exist.
            for (Map.Entry<TaskID, TaskStatus> entry : idToStatus.entrySet()) {
                if (!nameToId.containsValue(entry.getKey())) {
                    throw new IllegalStateException(String.format(
                            "nameToId is missing idToStatus entry: %s", entry));
                }
            }
            // If an ID=>status entry exists, a matching ID=>task entry must also exist
            for (Map.Entry<TaskID, TaskStatus> entry : idToStatus.entrySet()) {
                if (!idToTask.containsKey(entry.getKey())) {
                    throw new IllegalStateException(String.format(
                            "nameToId is missing idToStatus entry: %s", entry));
                }
            }

            // Phase 2: check consistency with StateStore

            // Local framework ID should match stored framework ID
            Optional<FrameworkID> storeFrameworkId = store.fetchFrameworkId();
            if (!storeFrameworkId.equals(frameworkId)) {
                throw new IllegalStateException(String.format(
                        "Cache has frameworkId[%s] while storage has frameworkId[%s]",
                        frameworkId, storeFrameworkId));
            }
            // Local task names should match stored task names
            Set<String> storeNames = new HashSet<>(store.fetchTaskNames());
            if (!storeNames.equals(nameToId.keySet())) {
                throw new IllegalStateException(String.format(
                        "Cache has taskNames[%s] while storage has taskNames[%s]",
                        nameToId.keySet(), storeNames));
            }
            // Local TaskInfos should match stored TaskInfos
            Map<TaskID, TaskInfo> storeTasks = new HashMap<>();
            for (String taskName : storeNames) {
                TaskInfo task = store.fetchTask(taskName).get();
                storeTasks.put(task.getTaskId(), task);
            }
            if (!storeTasks.equals(idToTask)) {
                throw new IllegalStateException(String.format(
                        "Cache has taskInfos[%s] while storage has taskInfos[%s]",
                        idToTask, storeTasks));
            }
            // Local TaskStatuses should match stored TaskStatuses
            Map<TaskID, TaskStatus> storeStatuses = new HashMap<>();
            for (String taskName : storeNames) {
                Optional<TaskStatus> status = store.fetchStatus(taskName);
                if (status.isPresent()) {
                    storeStatuses.put(status.get().getTaskId(), status.get());
                }
            }
            if (!storeStatuses.equals(idToStatus)) {
                throw new IllegalStateException(String.format(
                        "Cache has taskStatuses[%s] while storage has taskStatuses[%s]",
                        idToStatus, storeStatuses));
            }
            // Local Properties should match stored Properties
            Map<String, byte[]> storeProperties = new HashMap<>();
            for (String propertyKey : store.fetchPropertyKeys()) {
                storeProperties.put(propertyKey, store.fetchProperty(propertyKey));
            }
            if (!storeProperties.keySet().equals(properties.keySet())) {
                throw new IllegalStateException(String.format(
                        "Cache has properties[%s] while storage has properties[%s]",
                        properties, storeProperties));
            }
            // manual deep comparison for the byte arrays:
            for (Map.Entry<String, byte[]> propEntry : properties.entrySet()) {
                byte[] storeVal = storeProperties.get(propEntry.getKey());
                if (!Arrays.equals(propEntry.getValue(), storeVal)) {
                    throw new IllegalStateException(String.format(
                            "Cache property value[%s=%s] doesn't match storage property value[%s=%s]",
                            propEntry.getKey(), propEntry.getValue(), propEntry.getKey(), storeVal));
                }
            }
        } catch (Throwable e) {
            StringBuilder stateDump = new StringBuilder();
            stateDump.append("Consistency validation failure: ");
            stateDump.append(e.getMessage());
            stateDump.append("\nState dump:\n");
            stateDump.append("- frameworkId: ");
            stateDump.append(frameworkId);
            stateDump.append("\n- nameToId: ");
            stateDump.append(nameToId);
            stateDump.append("\n- idToTask: ");
            stateDump.append(idToTask);
            stateDump.append("\n- idToStatus: ");
            stateDump.append(idToStatus);
            stateDump.append("\n- properties: ");
            stateDump.append(properties);
            stateDump.append('\n');
            throw new IllegalStateException(stateDump.toString(), e);
        } finally {
            readLock.unlock();
        }
    }
}
