package com.sjoerdmulder.trident.mongodb;

public abstract class MongoValueResult<T> {
    private Long txId;
    private Long currentTxId;
    private T previous;

    public Long getCurrentTxId() {
        return currentTxId;
    }

    public void setCurrentTxId(Long currentTxId) {
        this.currentTxId = currentTxId;
    }

    public T getPrevious() {
        return previous;
    }

    public void setPrevious(T previous) {
        this.previous = previous;
    }

    public Long getTxid() {
        return txId;
    }

    public void setTxId(Long txId) {
        this.txId = txId;
    }
}
