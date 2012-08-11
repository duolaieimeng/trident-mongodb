package com.sjoerdmulder.trident.mongodb;

/**
 * Abstract class which should be used by all POJO classes who wish to be used in a Transaction or Opaque state
 *
 * @author Sjoerd Mulder (http://github.com/sjoerdmulder)
 * @param <T> The class who implements this class
 */
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
