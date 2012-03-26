package com.michelboudreau.test;



import com.michelboudreau.alternator.AlternatorDB;
import org.springframework.data.redis.core.RedisTemplate;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import org.apache.commons.logging.LogFactory;

public class AlternatorTest {

    private RedisTemplate<String, String> template;
    private AlternatorDB dao;


    @Before
    public void setUp() throws Exception {
        JedisConnectionFactory factory = new JedisConnectionFactory();
        factory.setUsePool(true);
        factory.setPort(6379);
        factory.setHostName("localhost");
        factory.afterPropertiesSet();
        RedisTemplate redtemp = new RedisTemplate();
        redtemp.setConnectionFactory(factory);
        this.template = redtemp;
        this.template.afterPropertiesSet();
        dao = new AlternatorDB(template);
    }

    @After
    public void tearDown() throws Exception {
        template.getConnectionFactory().getConnection().flushAll();
        template.getConnectionFactory().getConnection().close();
    }

    @Test
    public void shouldAddWordWithItsMeaningToDictionary() {
        Long index = dao.addWordWithItsMeaningToDictionary("lollop", "To move forward with a bounding, drooping motion.");
        Assert.assertTrue(index != null);

    }

    @Test
    public void shouldAddMeaningToAWordIfItExists() {
        Long index = dao.addWordWithItsMeaningToDictionary("lollop", "To move forward with a bounding, drooping motion.");
        Assert.assertTrue(index != null);
        index = dao.addWordWithItsMeaningToDictionary("lollop", "To hang loosely; droop; dangle.");
        System.out.println(index);

    }
}
