package com.michelboudreau.alternator;



import com.michelboudreau.*;
import org.springframework.data.redis.core.RedisTemplate;

public class AlternatorDB {

    private RedisTemplate<String, String> template;

    public AlternatorDB(RedisTemplate template) {
        this.template = template;
    }

    public Long addWordWithItsMeaningToDictionary(String word, String meaning) {
        Long index = template.opsForList().rightPush(word, meaning);
        return index;
    }
}
