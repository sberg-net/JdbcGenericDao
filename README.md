# Use JDBCGenericDao with a quarkus framework

## include via pom.xml

*add to pom.xml*

```xml
...
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>
...
<dependency>
    <groupId>com.github.sberg-net.JdbcGenericDao</groupId>
    <artifactId>quarkus</artifactId>
    <version>1.2.1</version>
</dependency>
...
<plugins>
    <plugin>
        <groupId>io.smallrye</groupId>
        <artifactId>jandex-maven-plugin</artifactId>
        <version>3.5.3</version>
        <executions>
            <execution>
                <id>make-index</id>
                <goals>
                    <goal>jandex</goal>
                </goals>
            </execution>
        </executions>
    </plugin>
</plugins>
```
* repository: https://jitpack.io/ is the maven artifact repository
* plugin: `jandex-maven-plugin` is need to scan external classes for quarkus annotations and include them in to lifecycle.

## quarkus application settings

*add package to scan into application.yaml*

```yaml
quarkus:
  index-dependency:
    JdbdGenericDao:
      group-id: com.github.sberg-net.JdbcGenericDao
```
*oder application.properties*
```
quarkus.index-dependency.JdbdGenericDao.group-id: com.github.sberg-net.JdbcGenericDao
```

## set parameter for JDBCGenericDao to scan JdbdGenericDao relevant classes
*JdbcDao scan into application.yaml*

```yaml
jdbcGenericDao:
  scanPackage: net.sberg.eldix4kim
```
*oder application.properties*
```
jdbcGenericDao.scanPackage: net.sberg.eldix4kim
```
* `net.sberg.eldix4kim` is an example package name
