package org.ezstack.ezapp.datastore.db.elasticsearch;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.util.Iterator;

public class SearchHitIterator implements Iterator<SearchHit> {
    private final Client _client;
    private SearchResponse _scrollResponse;
    private Iterator<SearchHit> _iter;

    /**
     * The amount of time elasticsearch should keep the scroll active.
     */
    private long _scrollTimeMillis;

    public SearchHitIterator(Client client, SearchResponse scrollResponse) {
        _client = client;
        _scrollResponse = scrollResponse;
        _scrollTimeMillis = 60000 * 2; // 2 minutes
        _iter = scrollResponse.getHits().iterator();
    }

    public SearchHitIterator(Client client, SearchResponse scrollResponse, long scrollTimeMillis) {
        _client = client;
        _scrollResponse = scrollResponse;
        _scrollTimeMillis = scrollTimeMillis;
        _iter = scrollResponse.getHits().iterator();
    }

    @Override
    public boolean hasNext() {
        if (_iter.hasNext()) {
            return true;
        }

        _scrollResponse = _client.prepareSearchScroll(_scrollResponse.getScrollId())
                .setScroll(new TimeValue(_scrollTimeMillis))
                .execute()
                .actionGet();
        _iter = _scrollResponse.getHits().iterator();
        return _iter.hasNext();
    }

    @Override
    public SearchHit next() {
        hasNext();
        return _iter.next();
    }
}
