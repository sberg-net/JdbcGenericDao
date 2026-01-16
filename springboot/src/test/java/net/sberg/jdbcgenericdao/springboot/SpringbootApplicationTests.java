package net.sberg.jdbcgenericdao.springboot;

import lombok.RequiredArgsConstructor;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@RequiredArgsConstructor
class SpringbootApplicationTests {

    private final JdbcGenericDao genericDao;

    @Test
    void initTest() throws Exception {
        assert true;
    }

}
