package net.sberg.jdbcgenericdao.springboot;

import net.sberg.jdbcgenericdao.core.DaoPlaceholderProperty;
import net.sberg.jdbcgenericdao.core.DaoProjectionBean;
import net.sberg.jdbcgenericdao.springboot.testentity.Person;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcGenericDaoSelectTest {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcGenericDao jdbcGenericDao;

    @BeforeAll
    void setupSchema() throws Exception {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("DROP TABLE IF EXISTS PERSON");
            st.execute("CREATE TABLE PERSON (ID INT PRIMARY KEY, FIRST_NAME VARCHAR(64), LAST_NAME VARCHAR(64))");
            for (String s : Arrays.asList(
                    "INSERT INTO PERSON (ID, FIRST_NAME, LAST_NAME) VALUES (1, 'John', 'Doe')",
                    "INSERT INTO PERSON (ID, FIRST_NAME, LAST_NAME) VALUES (2, 'Jane', 'Doe')",
                    "INSERT INTO PERSON (ID, FIRST_NAME, LAST_NAME) VALUES (3, 'Bob', 'Smith')")) {
                st.execute(s);
            }
        }
    }

    @Test
    void selectMany_byLastName_returnsEntities() throws Exception {
        List<DaoPlaceholderProperty> where = Collections.singletonList(new DaoPlaceholderProperty("lastName", "Doe"));
        @SuppressWarnings("rawtypes")
        List result = jdbcGenericDao.selectMany(Person.class.getName(), null, where);
        assertNotNull(result);
        assertEquals(2, result.size());

        Object first = result.get(0);
        assertInstanceOf(Person.class, first);
        Person p = (Person) first;
        assertEquals("Doe", p.getLastName());
    }

    @Test
    void selectOne_byId_returnsEntity() throws Exception {
        List<DaoPlaceholderProperty> where = Collections.singletonList(new DaoPlaceholderProperty("id", 1));
        Object result = jdbcGenericDao.selectOne(Person.class.getName(), null, where);
        assertNotNull(result);
        assertInstanceOf(Person.class, result);
        Person p = (Person) result;
        assertEquals(1, p.getId());
        assertEquals("John", p.getFirstName());
    }

    @Test
    void selectOne_atomProjection_returnsScalar() throws Exception {
        List<DaoPlaceholderProperty> where = Collections.singletonList(new DaoPlaceholderProperty("id", 2));
        DaoProjectionBean projection = new DaoProjectionBean(Collections.singletonList("firstName"), String.class, true);
        Object value = jdbcGenericDao.selectOne(Person.class.getName(), projection, where);
        assertInstanceOf(String.class, value);
        assertEquals("Jane", value);
    }
}
