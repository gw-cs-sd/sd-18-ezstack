package org.ezstack.ezapp.denormalizer.api;

public class Denormalization {

    public JoinResult join(String joinTable, String pointerName) {
        new Denormalization().join("table", "p").average("rating").assignTo("avgRating");
    }


}
