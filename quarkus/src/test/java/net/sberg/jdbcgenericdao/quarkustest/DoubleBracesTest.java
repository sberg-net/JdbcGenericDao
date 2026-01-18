package net.sberg.jdbcgenericdao.quarkustest;


import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import net.sberg.jdbcgenericdao.core.DaoPlaceholderProperty;
import net.sberg.jdbcgenericdao.quarkus.JdbcGenericDao;
import net.sberg.jdbcgenericdao.quarkustest.testentity.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DoubleBracesTest {

    @Inject
    DataSource dataSource;

    @Inject
    JdbcGenericDao jdbcGenericDao;

    @BeforeEach
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
        jdbcGenericDao.initialize();
    }

    @Test
    void batchInsert() throws Exception {

        List<Object> persons = new ArrayList<>() {{
            add(new Person() {{
                setFirstName("Christian");
                setLastName("Dethloff");
            }});
            add(new Person() {{
                setFirstName("Marlen");
                setLastName("Dethloff");
            }});
        }};

        jdbcGenericDao.batchInsert(persons, Optional.empty());

        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 4)));
        assertNotNull(person);
    }

    @Test
    void insert() throws Exception {
        Person p = new Person() {{
            setFirstName("Christian");
            setLastName("Dethloff");
        }};

        jdbcGenericDao.insert(p, Optional.empty());

        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 4)));
        assertNotNull(person);
    }

    @Test
    void delete() throws Exception {
        Person p = new Person() {{
            setId(3);
        }};

        jdbcGenericDao.delete(p, Optional.empty());

        @SuppressWarnings("unchecked")
        List<Person> persons = jdbcGenericDao.selectMany(Person.class.getName(), null, null);
        assert(persons.size() == 2);
    }

    @Test
    void update() throws Exception {
        Person p = new Person() {{
            setId(3);
            setFirstName("Marlene");
        }};

        jdbcGenericDao.update(p, Optional.empty());

        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 3)));
        assert(person.getFirstName().equals("Marlene"));
    }

    @Test
    void batchUpdate() throws Exception {
        List<Object> pl = new ArrayList<>() {{
            add(new Person() {{
                setId(3);
                setFirstName("Marlene");
            }});
            add(new Person() {{
                setId(1);
                setFirstName("Stephan");
            }});
        }};

        jdbcGenericDao.batchUpdate(pl, Optional.empty());

        Person person = (Person)jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 1)));
        assert(person.getFirstName().equals("Stephan"));
        person = (Person)jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 3)));
        assert(person.getFirstName().equals("Marlene"));
        person = (Person)jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 2)));
        assert(person.getFirstName().equals("Jane"));
    }
}
