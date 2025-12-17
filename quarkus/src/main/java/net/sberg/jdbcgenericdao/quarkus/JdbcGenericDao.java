package net.sberg.jdbcgenericdao.quarkus;


import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import net.sberg.jdbcgenericdao.core.DaoPlaceholderProperty;
import net.sberg.jdbcgenericdao.core.DaoProjectionBean;
import net.sberg.jdbcgenericdao.core.JdbcGenericDaoNativeJava;
import net.sberg.jdbcgenericdao.core.DaoDescriptorBean;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.util.List;

@ApplicationScoped
public class JdbcGenericDao {

    @ConfigProperty(name = "jdbcGenericDao.scanPackage")
    String scanPackage;

    @Inject
    DataSource dataSource;

    private JdbcGenericDaoNativeJava jdbcGenericDaoNativeJava;

    @PostConstruct
    public void init() throws Exception {
        jdbcGenericDaoNativeJava = new JdbcGenericDaoNativeJava(dataSource);
        jdbcGenericDaoNativeJava.init(scanPackage);
    }

    public Object selectOne(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return jdbcGenericDaoNativeJava.selectOne(entityName, daoProjectionBean, placeholders);
    }

    public Object selectOne(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return jdbcGenericDaoNativeJava.selectOne(sql, entityName, daoProjectionBean, placeholders);
    }

    public List selectMany(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return jdbcGenericDaoNativeJava.selectMany(entityName, daoProjectionBean, placeholders);
    }

    public List selectMany(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return jdbcGenericDaoNativeJava.selectMany(sql, entityName, daoProjectionBean, placeholders);
    }

    @Transactional
    public Object insert(Object entity) throws Exception {
        return jdbcGenericDaoNativeJava.insert(entity);
    }

    public int getNextId(String entityName) throws Exception {
        return jdbcGenericDaoNativeJava.getNextId(entityName);
    }

    @Transactional
    public void delete(Object entity) throws Exception {
        jdbcGenericDaoNativeJava.delete(entity);
    }

    private void delete(int id, DaoDescriptorBean daoDescriptorBean) throws Exception {
        jdbcGenericDaoNativeJava.delete(id, daoDescriptorBean);
    }

    @Transactional
    public void delete(int id, String entityName) throws Exception {
        jdbcGenericDaoNativeJava.delete(id, entityName);
    }

    @Transactional
    public void delete(String sql, List<DaoPlaceholderProperty> placeholders) throws Exception {
        jdbcGenericDaoNativeJava.delete(sql, placeholders);
    }

    @Transactional
    public Object update(Object entity) throws Exception {
        return jdbcGenericDaoNativeJava.update(entity);
    }

    @Transactional
    public int update(String sql, String entityName, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return jdbcGenericDaoNativeJava.update(sql, entityName, placeholders);
    }
}
