package net.sberg.jdbcgenericdao.springboot;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class SpringbootApplicationTests {

    @Autowired
    private JdbcGenericDao genericDao;

    @Test
    void initTest() throws Exception {
        assert true;
    }

}
