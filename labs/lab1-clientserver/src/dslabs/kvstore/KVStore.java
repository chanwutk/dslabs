package dslabs.kvstore;

import dslabs.framework.Application;
import dslabs.framework.Command;
import dslabs.framework.Result;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;


@ToString
@EqualsAndHashCode
public class KVStore implements Application {

    public interface KVStoreCommand extends Command {
    }

    public interface SingleKeyCommand extends KVStoreCommand {
        String key();
    }

    @Data
    public static final class Get implements SingleKeyCommand {
        @NonNull private final String key;

        @Override
        public boolean readOnly() {
            return true;
        }
    }

    @Data
    public static final class Put implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    @Data
    public static final class Append implements SingleKeyCommand {
        @NonNull private final String key, value;
    }

    public interface KVStoreResult extends Result {
    }

    @Data
    public static final class GetResult implements KVStoreResult {
        @NonNull private final String value;
    }

    @Data
    public static final class KeyNotFound implements KVStoreResult {
    }

    @Data
    public static final class PutOk implements KVStoreResult {
    }

    @Data
    public static final class AppendResult implements KVStoreResult {
        @NonNull private final String value;
    }

    // Your code here...
    private static KeyNotFound KEY_NOT_FOUND = new KeyNotFound();
    private static PutOk PUT_OK = new PutOk();

    private Map<String, String> kv = new HashMap<>();

    @Override
    public KVStoreResult execute(Command command) {
        if (command instanceof Get) {
            Get g = (Get) command;
            // Your code here...
            if (kv.containsKey(g.key)) {
                return new GetResult(kv.get(g.key));
            } else {
                return KEY_NOT_FOUND;
            }
        }

        if (command instanceof Put) {
            Put p = (Put) command;
            // Your code here...
            kv.put(p.key, p.value);
            return PUT_OK;
        }

        if (command instanceof Append) {
            Append a = (Append) command;
            // Your code here...
            String key = a.key;
            String newValue = a.value;
            if (kv.containsKey(key)) {
                newValue = kv.get(key) + newValue;
            }
            kv.put(key, newValue);
            return new AppendResult(newValue);
        }

        throw new IllegalArgumentException();
    }
}
