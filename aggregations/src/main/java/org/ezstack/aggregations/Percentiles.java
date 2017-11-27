package org.ezstack.aggregations;

public class Percentiles {

    private long avg;
    private long fiftieth;
    private long ninetieth;
    private long ninetyFifth;
    private long ninetyNinth;

    public Percentiles(long avg, long fiftieth, long ninetieth, long ninetyFifth, long ninetyNinth) {
        this.avg = avg;
        this.fiftieth = fiftieth;
        this.ninetieth = ninetieth;
        this.ninetyFifth = ninetyFifth;
        this.ninetyNinth = ninetyNinth;
    }

    public long getAvg() {
        return avg;
    }

    public long getFiftieth() {
        return fiftieth;
    }

    public long getNinetieth() {
        return ninetieth;
    }

    public long getNinetyFifth() {
        return ninetyFifth;
    }

    public long getNinetyNinth() {
        return ninetyNinth;
    }

    public void setAvg(long avg) {
        this.avg = avg;
    }

    public void setFiftieth(long fiftieth) {
        this.fiftieth = fiftieth;
    }

    public void setNinetieth(long ninetieth) {
        this.ninetieth = ninetieth;
    }

    public void setNinetyFifth(long ninetyFifth) {
        this.ninetyFifth = ninetyFifth;
    }

    public void setNinetyNinth(long ninetyNinth) {
        this.ninetyNinth = ninetyNinth;
    }
}
