package org.ezstack.denormalizer.model;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import org.ezstack.ezapp.datastore.api.Document;
import org.ezstack.ezapp.datastore.api.JoinAttribute;
import org.ezstack.ezapp.datastore.api.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class FanoutHashingUtils {

    private static final Logger log = LoggerFactory.getLogger(FanoutHashingUtils.class);


    public static String getPartitionKey(Document document, QueryLevel queryLevel,
                                         Query query) {

        Hasher hasher = Hashing.murmur3_128().newHasher();

        hasher.putString(query.getMurmur3HashAsString(), Charsets.UTF_8);

        if (query.getJoin() == null) {
            hasher.putString("|", Charsets.UTF_8);
            hasher.putString(document.getKey(), Charsets.UTF_8);
        } else {
            List<JoinAttribute> joinAtts = query.getJoinAttributes();

            List<String> atts = Lists.transform(joinAtts,
                    queryLevel == QueryLevel.OUTER ? JoinAttribute::getOuterAttribute : JoinAttribute::getInnerAttribute);

            for (String att : atts) {
                hasher.putString("|", Charsets.UTF_8);
                hasher.putString(document.getValue(att).toString(), Charsets.UTF_8);
            }
        }

        return hasher.hash().toString();
    }
}
