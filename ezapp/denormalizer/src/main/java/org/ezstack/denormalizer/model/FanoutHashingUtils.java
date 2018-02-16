package org.ezstack.denormalizer.model;

import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.ezstack.ezapp.datastore.api.JoinAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.List;


public class FanoutHashingUtils {

    private static final Logger log = LoggerFactory.getLogger(FanoutHashingUtils.class);


    public static String getPartitionKey(Document document, QueryLevel queryLevel,
                                         List<JoinAttribute> joinAtts) {

        List<String> atts = Lists.transform(joinAtts,
                queryLevel == QueryLevel.OUTER ? JoinAttribute::getOuterAttribute : JoinAttribute::getInnerAttribute);

        // TODO: make this hash better

        Hasher hasher = Hashing.murmur3_128().newHasher();

        for (String att : atts) {
            hasher.putString(document.getValue(att).toString(), Charset.defaultCharset());
            hasher.putString("|", Charset.defaultCharset());
        }

        return hasher.hash().toString();
    }
}
