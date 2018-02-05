package org.ezstack.denormalizer.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterators;
import org.ezstack.ezapp.datastore.api.Query;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class JoinQueryIndex {

    private Map<String, Map<String, Document>> subQueryResults;

    // For Jackson
    private JoinQueryIndex() {
    }

    public JoinQueryIndex(Query query) {
        subQueryResults = getJoinDataStructure(query);
    }

    private static Map<String, Map<String, Document>> getJoinDataStructure(Query query) {
        Query currentQuery = query;
        Integer index = 0;
        Map<String, Map<String, Document>> indexMap = new LinkedHashMap<>();
        do {
            indexMap.put(index.toString(), new LinkedHashMap<>());
            index++;
        } while (query.getJoin() != null);

        return indexMap;
    }

    @JsonAnySetter
    public void setSubQuery(String index, Map<String, Document> results) {
        int indexAsInt = -1;
        try {
            indexAsInt = Integer.parseInt(index);
        } catch (NumberFormatException e) {
            Throwables.throwIfUnchecked(new Error("Join query index not valid as it contains non-numeric attributes"));
        }

        checkArgument(indexAsInt >= 0, "Join query index not valid as it contains negative attributes");
        subQueryResults.put(index, results);
    }

    @JsonAnyGetter
    private Map<String, Map<String, Document>> getFullIndex() {
        return subQueryResults;
    }

    public Iterator<Document> getDocsAtIndex(Integer index) {
        Map<String, Document> docs = subQueryResults.get(index.toString());

        checkNotNull(docs, "The index {} is not contained in the join index", index);

        Set entrySet = docs.entrySet();

        return Iterators.transform(docs.entrySet().iterator(), Map.Entry::getValue);

    }

    public void putDocument(Integer index, Document document) {
        Map<String, Document> docs = subQueryResults.get(index);

        if (subQueryResults == null) {
            Throwables.throwIfUnchecked(new IndexOutOfBoundsException());
        }

        docs.put(index.toString(), document);
    }
}
