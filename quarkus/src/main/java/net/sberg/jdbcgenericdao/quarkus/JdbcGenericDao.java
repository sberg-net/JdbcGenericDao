package net.sberg.jdbcgenericdao.quarkus;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import lombok.extern.slf4j.Slf4j;
import net.sberg.jdbcgenericdao.core.DaoPlaceholderProperty;
import net.sberg.jdbcgenericdao.core.DaoProjectionBean;
import net.sberg.jdbcgenericdao.core.GenericJdbcDao;
import net.sberg.jdbcgenericdao.core.DaoDescriptorBean;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.util.List;

@ApplicationScoped
@Slf4j
public class JdbcGenericDao {

    @ConfigProperty(name = "jdbcGenericDao.scanPackage")
    String scanPackage;

    @Inject
    DataSource dataSource;

    private GenericJdbcDao delegate;

    @PostConstruct
    public void init() throws Exception {
        delegate = new GenericJdbcDao(dataSource);
        delegate.init(scanPackage);
    }

    public Object selectOne(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return delegate.selectOne(entityName, daoProjectionBean, placeholders);
    }

    public Object selectOne(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return delegate.selectOne(sql, entityName, daoProjectionBean, placeholders);
    }

    public List selectMany(String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return delegate.selectMany(entityName, daoProjectionBean, placeholders);
    }

    public List selectMany(String sql, String entityName, DaoProjectionBean daoProjectionBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return delegate.selectMany(sql, entityName, daoProjectionBean, placeholders);
    }

    @Transactional
    public Object insert(Object entity) throws Exception {
        return delegate.insert(entity);
    }

    public int getNextId(String entityName) throws Exception {
        return delegate.getNextId(entityName);
    }

    @Transactional
    public void delete(Object entity) throws Exception {
        delegate.delete(entity);
    }

    private void delete(int id, DaoDescriptorBean daoDescriptorBean) throws Exception {
        delegate.delete(id, daoDescriptorBean);
    }

    @Transactional
    public void delete(int id, String entityName) throws Exception {
        delegate.delete(id, entityName);
    }

    @Transactional
    public void delete(String sql, List<DaoPlaceholderProperty> placeholders) throws Exception {
        delegate.delete(sql, placeholders);
    }

    @Transactional
    public Object update(Object entity) throws Exception {
        return delegate.update(entity);
    }

    @Transactional
    public int update(String sql, String entityName, List<DaoPlaceholderProperty> placeholders) throws Exception {
        return delegate.update(sql, entityName, placeholders);
    }
}
