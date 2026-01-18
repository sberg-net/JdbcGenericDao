package net.sberg.jdbcgenericdao.quarkustest;

import io.quarkus.runtime.Startup;
import jakarta.inject.Inject;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

@Startup
public class TestStartUp {

    @Inject
    DataSource dataSource;

    @Startup
    public void preInitDb() {
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE TABLE Dating (id INT PRIMARY KEY, desc VARCHAR(64), date DATE, dateTime TIMESTAMP)");
            st.execute("CREATE TABLE PERSON (ID INT PRIMARY KEY, FIRST_NAME VARCHAR(64), LAST_NAME VARCHAR(64))");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
