
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.samza.SamzaException;
import org.apache.samza.serializers.Serde;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.UnsupportedEncodingException;

/**
 * A serializer for UTF-8 encoded JSON strings. JsonSerdeV3 differs from JsonSerde in that:
 * <ol>
 *   <li>
 *     It allows specifying the specific POJO type to deserialize to (using JsonSerdeV3(Class&lt;T&gt;)
 *     or JsonSerdeV3#of(Class&lt;T&gt;). JsonSerde always returns a LinkedHashMap&lt;String, Object&gt;
 *     upon deserialization.
 *   <li>
 *     It uses Jackson's default 'camelCase' property naming convention, which simplifies defining
 *     the POJO to bind to. JsonSerde enforces the 'dash-separated' property naming convention.
 * </ol>
 * This JsonSerdeV3 should be preferred over JsonSerde for High Level API applications, unless
 * backwards compatibility with the older data format (with dasherized names) is required.
 *
 * @param <T> the type of the POJO being (de)serialized.
 */
public class JsonSerdeV3<T> implements Serde<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSerdeV3.class);
    private final Class<T> clazz;
    private transient ObjectMapper mapper = new ObjectMapper();

    /**
     * Constructs a JsonSerdeV3 that returns a LinkedHashMap&lt;String, Object&lt; upon deserialization.
     */
    public JsonSerdeV3() {
        this(null);
    }

    /**
     * Constructs a JsonSerdeV3 that (de)serializes POJOs of class {@code clazz}.
     *
     * @param clazz the class of the POJO being (de)serialized.
     */
    public JsonSerdeV3(Class<T> clazz) {
        this.clazz = clazz;
    }

    public static <T> JsonSerdeV3<T> of(Class<T> clazz) {
        return new JsonSerdeV3<>(clazz);
    }

    public byte[] toBytes(T obj) {
        if (obj != null) {
            try {
                String str = mapper.writeValueAsString(obj);
                return str.getBytes("UTF-8");
            } catch (Exception e) {
                throw new SamzaException("Error serializing data.", e);
            }
        } else {
            return null;
        }
    }

    public T fromBytes(byte[] bytes) {
        if (bytes != null) {
            String str;
            try {
                str = new String(bytes, "UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new SamzaException("Error deserializing data", e);
            }

            try {
                if (clazz != null) {
                    return mapper.readValue(str, clazz);
                } else {
                    return mapper.readValue(str, new TypeReference<T>() { });
                }
            } catch (Exception e) {
                LOG.debug("Error deserializing data: " + str, e);
                throw new SamzaException("Error deserializing data", e);
            }
        } else {
            return null;
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        this.mapper = new ObjectMapper();
    }
}