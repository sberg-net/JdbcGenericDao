package net.sberg.jdbcgenericdao.quarkus.testentity;

import lombok.Data;
import net.sberg.jdbcgenericdao.core.DaoDescriptorClass;
import net.sberg.jdbcgenericdao.core.DaoDescriptorElement;

@Data
@DaoDescriptorClass(dbTable = "PERSON", primaryKey = "ID")
public class Person {

    @DaoDescriptorElement(dbProperty = "ID", notNull = true)
    private Integer id;

    @DaoDescriptorElement(dbProperty = "FIRST_NAME")
    private String firstName;

    @DaoDescriptorElement(dbProperty = "LAST_NAME")
    private String lastName;
}
