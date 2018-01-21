package org.ezstack.ezapp.datastore.db.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;

public class SearchHitIterator implements Iterator<SearchHit> {
    private final Client client;
    private SearchResponse scrollResponse;
    private Iterator<SearchHit> iter;

    /**
     * The amount of time elasticsearch should keep the scroll active.
     */
    private long scrollTimeMillis;

    public SearchHitIterator(Client client, SearchResponse scrollResponse) {
        this.client = client;
        this.scrollResponse = scrollResponse;
        scrollTimeMillis = 60000 * 2; // 2 minutes
        iter = scrollResponse.getHits().iterator();
    }

    public SearchHitIterator(Client client, SearchResponse scrollResponse, long scrollTimeMillis) {
        this.client = client;
        this.scrollResponse = scrollResponse;
        this.scrollTimeMillis = scrollTimeMillis;
        iter = scrollResponse.getHits().iterator();
    }

    @Override
    public boolean hasNext() {
        if (iter.hasNext()) {
            return true;
        }

        scrollResponse = client.prepareSearchScroll(scrollResponse.getScrollId())
                .setScroll(new TimeValue(scrollTimeMillis))
                .execute()
                .actionGet();
        iter = scrollResponse.getHits().iterator();
        return iter.hasNext();
    }

    @Override
    public SearchHit next() {
        hasNext();
        return iter.next();
    }
}
