package org.ezstack.ezapp.denormalizer.api;

public abstract class DenormalizedResult implements Denormalization {

    private String _assignedAttribute;

    public void assignTo(String assignedAttribute) {
        _assignedAttribute = assignedAttribute;
    }
}
