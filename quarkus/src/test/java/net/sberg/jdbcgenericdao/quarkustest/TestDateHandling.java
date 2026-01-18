package net.sberg.jdbcgenericdao.quarkustest;


import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import net.sberg.jdbcgenericdao.quarkus.JdbcGenericDao;
import net.sberg.jdbcgenericdao.quarkustest.testentity.Dating;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestDateHandling {

    @Inject
    DataSource dataSource;

    @Inject
    JdbcGenericDao jdbcGenericDao;

    @BeforeEach
    void setupSchema() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS Dating");
            st.execute("CREATE TABLE Dating (id INT PRIMARY KEY, `desc` VARCHAR(64), date DATE, dateTime TIMESTAMP)");
            for (String s : Arrays.asList(
                    "INSERT INTO Dating (id, desc, date, dateTime) VALUES (1, 'justDate', '2023-11-20', null)",
                    "INSERT INTO Dating (id, desc, date, dateTime) VALUES (2, 'justDateTime', null, '2023-11-20 12:00:00')",
                    "INSERT INTO Dating (id, desc, date, dateTime) VALUES (3, 'all', '2026-01-09', '2026-01-09 21:23:09')")) {
                st.execute(s);
            }
        }
        jdbcGenericDao.initialize();
    }

    @Test
    public void testDateHandling() throws Exception {
        List<Dating> datings = jdbcGenericDao.selectMany(Dating.class.getName(), null, null);
        assert datings.size() == 3;
    }
}
