package net.sberg.jdbcgenericdao.quarkustest;


import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import net.sberg.jdbcgenericdao.core.DaoPlaceholderProperty;
import net.sberg.jdbcgenericdao.quarkus.JdbcGenericDao;
import net.sberg.jdbcgenericdao.quarkustest.testentity.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JdbcGenericDaoTest {

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
    void delete_bySql() throws Exception {
        List<DaoPlaceholderProperty> placeholders = List.of(new DaoPlaceholderProperty("lastName", "Smith"));
        jdbcGenericDao.delete("DELETE FROM PERSON WHERE LAST_NAME = ?", placeholders);
        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertNull(person);
        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 2)));
        assertNotNull(person);
    }

    @Test
    void delete_byObject() throws Exception {
        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertNotNull(person);
        jdbcGenericDao.delete(person, Optional.empty());
        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertNull(person);
    }

    @Test
    void delete_byId() throws Exception {
        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertNotNull(person);
        jdbcGenericDao.delete(person.getId(), Person.class.getName(), Optional.empty());
        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertNull(person);
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
    void batchUpdate() throws Exception {
        List<Object> persons = new ArrayList<>();

        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        person.setFirstName("uschi");
        persons.add(person);

        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 2)));
        person.setFirstName("uschi");
        persons.add(person);

        jdbcGenericDao.batchUpdate(persons, Optional.empty());

        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertEquals("uschi", person.getFirstName());

        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 2)));
        assertEquals("uschi", person.getFirstName());
    }

    @Test
    void update_byObject() throws Exception {
        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        person.setFirstName("uschi");

        jdbcGenericDao.update(person, Optional.empty());

        person = (Person) jdbcGenericDao.selectOne(Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 3)));
        assertEquals("uschi", person.getFirstName());
    }

    @Test
    void update_bySql() throws Exception {
        jdbcGenericDao.update("update Person set FIRST_NAME='uschi' where id = ?", Person.class.getName(),
                List.of(new DaoPlaceholderProperty("id", 3)));

        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 3)));
        assertEquals("uschi", person.getFirstName());
    }

    @Test
    void insert() throws Exception {
        Person p = new Person();
        p.setFirstName("Christian");
        p.setLastName("Dethloff");

        jdbcGenericDao.insert(p, Optional.empty());

        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 4)));
        assertNotNull(person);
    }

    @Test
    @SuppressWarnings("unchecked")
    void selectMany_byEntityName() throws Exception {
        List<Person> persons = jdbcGenericDao.selectMany(Person.class.getName(), null, null);
        assertEquals(3, persons.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    void selectMany_bySql() throws Exception {
        List<Person> persons = jdbcGenericDao.selectMany("select * from Person", Person.class.getName(),
                null, null);
        assertEquals(3, persons.size());
    }

    @Test
    void selectOne_byEntityName() throws Exception {
        Person person = (Person) jdbcGenericDao.selectOne(Person.class.getName(), null,
                List.of(new DaoPlaceholderProperty("id", 1)));
        assertNotNull(person);
    }

    @Test
    void selectOne_bySql() throws Exception {
        Person person = (Person) jdbcGenericDao.selectOne("select * from Person where id = ?", Person.class.getName(),
                null, List.of(new DaoPlaceholderProperty("id", 1)));
        assertNotNull(person);
    }
}
