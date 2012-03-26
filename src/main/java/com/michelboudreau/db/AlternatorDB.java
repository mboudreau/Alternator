package com.michelboudreau.alternator;


import org.springframework.data.redis.core.RedisTemplate;

public class AlternatorDB {
	private RedisTemplate<String, String> template;

    public AlternatorDB(RedisTemplate template) {
	    // Start HTTP server

	    // Create RedisTemplate for data store
	    
        this.template = template;
    }

    public Long addWordWithItsMeaningToDictionary(String word, String meaning) {
        Long index = template.opsForList().rightPush(word, meaning);
        return index;
    }
}
