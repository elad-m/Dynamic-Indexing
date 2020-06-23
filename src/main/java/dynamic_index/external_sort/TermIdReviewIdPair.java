package dynamic_index.external_sort;

class TermIdReviewIdPair implements Comparable<TermIdReviewIdPair> {

    final int tid;
    final int rid;

    TermIdReviewIdPair(int tid, int rid) {
        this.tid = tid;
        this.rid = rid;
    }

    @Override
    public String toString() {
        return "TermIdReviewIdPair{" +
                "tid=" + tid +
                ", rid=" + rid +
                '}';
    }

    @Override
    public int compareTo(TermIdReviewIdPair anotherTidRid) {
        int compareResult = Integer.compare(this.tid, anotherTidRid.tid);
        if (compareResult == 0) {
            return Integer.compare(this.rid, anotherTidRid.rid);
        } else {
            return compareResult;
        }
    }


}
