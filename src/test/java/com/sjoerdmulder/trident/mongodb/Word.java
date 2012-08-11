package com.sjoerdmulder.trident.mongodb;


public class Word extends MongoValueResult<Word> {

    String _id;
    Long count;

    public Word(){}

    public Word(Word word1, Word word2) {
        this(word1._id == null ? word2._id : word1._id, word1.count + word2.count);
    }

    public Word(String _id, Long count) {
        this._id = _id;
        this.count = count;
    }

}
