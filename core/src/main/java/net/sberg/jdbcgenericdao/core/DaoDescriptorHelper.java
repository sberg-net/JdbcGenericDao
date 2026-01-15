package net.sberg.jdbcgenericdao.core;

import org.apache.commons.lang3.ClassUtils;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.JarURLConnection;
import java.net.URL;
import java.text.MessageFormat;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

public class DaoDescriptorHelper {

    public static final String properties = "properties";
    public static final String unknown = "_unknown_";

    private static final String insertTemplate = "insert into {0} ({1}) values ({2})";
    private static final String updateTemplate = "update {0} set {1} where {2}";
    private static final String deleteTemplate = "delete from {0} where {1}";
    private static final String selectMaxIdTemplate = "select max({0}) from {1}";
    private static final String selectSimpleTemplate = "select {0} from {1} where {2}";
    private static final String placeHolderTemplate = "{0} = ?";

    private final List<String> annotatedClasses = new ArrayList<>();
    private final Map<String, List<String>> annotatedElements = new HashMap<>();

    public String createInsertStatement(DaoDescriptorBean daoDescriptorBean, Optional<String> tableName) throws Exception {
        String projection = daoDescriptorBean.getAllDbProperties().stream().map(String::valueOf).collect(Collectors.joining(", "));
        String placeholders = daoDescriptorBean.getAllDbProperties().stream().map(o -> "?").collect(Collectors.joining(", "));
        return MessageFormat.format(insertTemplate, tableName.isEmpty()?daoDescriptorBean.getDbTable():tableName.get(), projection, placeholders);
    }

    public String createUpdateStatement(DaoDescriptorBean daoDescriptorBean, Optional<String> tableName) throws Exception {
        List<String> dbProperties = new ArrayList<>(daoDescriptorBean.getAllDbProperties());
        dbProperties.remove(daoDescriptorBean.getPrimaryKey());
        String placeholders = dbProperties.stream().map(o -> o + " = ?").collect(Collectors.joining(", "));
        return MessageFormat.format(updateTemplate, tableName.isEmpty()?daoDescriptorBean.getDbTable():tableName.get(), placeholders, MessageFormat.format(placeHolderTemplate, daoDescriptorBean.getPrimaryKey()));
    }

    public String createSelectMaxIdStatement(DaoDescriptorBean daoDescriptorBean, Optional<String> tableName) throws Exception {
        return MessageFormat.format(selectMaxIdTemplate, daoDescriptorBean.getPrimaryKey(), tableName.isEmpty()?daoDescriptorBean.getDbTable():tableName.get());
    }

    public String createSelectSimpleStatement(DaoProjectionBean daoProjectionBean, DaoDescriptorBean daoDescriptorBean, List<DaoPlaceholderProperty> placeholders) throws Exception {
        StringBuilder params = new StringBuilder("1=1");
        if (placeholders != null && !placeholders.isEmpty()) {
            params = new StringBuilder();
            DaoPlaceholderProperty daoPlaceholderProperty;
            DaoDescriptorProperty daoDescriptorProperty;
            for (DaoPlaceholderProperty placeholder : placeholders) {
                daoPlaceholderProperty = placeholder;
                daoDescriptorProperty = daoDescriptorBean.getProperties().get(daoPlaceholderProperty.getProperty());
                if (params.length() > 0) {
                    params.append(" and ");
                }
                params.append(MessageFormat.format(placeHolderTemplate, daoDescriptorProperty.getDbProperty()));
            }
        }
        String projection = "";
        if (daoProjectionBean == null) {
            projection = daoDescriptorBean.getAllDbProperties().stream().map(String::valueOf).collect(Collectors.joining(", "));
        } else {
            projection = daoProjectionBean.getProperties().stream().map(o -> daoDescriptorBean.getProperties().get(o).getDbProperty()).collect(Collectors.joining(", "));
        }

        return MessageFormat.format(selectSimpleTemplate, projection, daoDescriptorBean.getDbTable(), params.toString());
    }

    public String createDeleteStatement(DaoDescriptorBean daoDescriptorBean, Optional<String> tableName) throws Exception {
        return MessageFormat.format(deleteTemplate, tableName.isEmpty()?daoDescriptorBean.getDbTable():tableName.get(), MessageFormat.format(placeHolderTemplate, daoDescriptorBean.getPrimaryKey()));
    }

    public Map<String, DaoDescriptorBean> createBeanMap(String scanPackage) throws Exception {
        // Discover classes annotated with @DaoDescriptorClass without Spring
        Set<Class<?>> candidates = findAnnotatedClasses(scanPackage);
        for (Class<?> beanClass : candidates) {
            annotatedClasses.add(beanClass.getName());
            for (Field field : beanClass.getDeclaredFields()) {
                if (field.getAnnotation(DaoDescriptorElement.class) != null) {
                    String name = field.getName();
                    annotatedElements.computeIfAbsent(beanClass.getName(), k -> new ArrayList<>());
                    if (!annotatedElements.get(beanClass.getName()).contains(name)) {
                        annotatedElements.get(beanClass.getName()).add(name);
                    }
                }
            }
        }

        Map<String, DaoDescriptorBean> result = new HashMap<>();

        DaoDescriptorBean daoDescriptorBean;
        DaoDescriptorProperty daoDescriptorProperty;

        for (String annotatedClass : annotatedClasses) {
            Class<?> aClass = Class.forName(annotatedClass, false, Thread.currentThread().getContextClassLoader());

            daoDescriptorBean = new DaoDescriptorBean();
            daoDescriptorBean.setTransientBean(aClass.getAnnotation(DaoDescriptorClass.class).transientBean());
            daoDescriptorBean.setDbTable(aClass.getAnnotation(DaoDescriptorClass.class).dbTable());
            daoDescriptorBean.setPrimaryKey(aClass.getAnnotation(DaoDescriptorClass.class).primaryKey());
            daoDescriptorBean.setName(aClass.getName());
            if (daoDescriptorBean.getDbTable().equals(unknown)) {
                daoDescriptorBean.setDbTable(aClass.getSimpleName());
            }

            result.put(daoDescriptorBean.getName(), daoDescriptorBean);

            List<String> fields = annotatedElements.get(aClass.getName());
            for (String fieldName : fields) {
                Field aField = aClass.getDeclaredField(fieldName);
                daoDescriptorProperty = new DaoDescriptorProperty();

                if (ClassUtils.primitiveToWrapper(aField.getType()) != null) {
                    daoDescriptorProperty.setTypeClass(ClassUtils.primitiveToWrapper(aField.getType()));
                } else {
                    daoDescriptorProperty.setTypeClass(ClassUtils.getClass(aField.getType().getName()));
                }

                daoDescriptorProperty.setType(daoDescriptorProperty.getTypeClass().getName());
                daoDescriptorProperty.setDbProperty(aField.getAnnotation(DaoDescriptorElement.class).dbProperty());
                daoDescriptorProperty.setNotNull(aField.getAnnotation(DaoDescriptorElement.class).notNull());

                if (daoDescriptorProperty.getDbProperty().equals(unknown)) {
                    daoDescriptorProperty.setDbProperty(aField.getName());
                }

                daoDescriptorBean.getAllProperties().add(aField.getName());
                daoDescriptorBean.getProperties().put(aField.getName(), daoDescriptorProperty);
                daoDescriptorBean.getAllDbProperties().add(daoDescriptorProperty.getDbProperty());
                daoDescriptorBean.getDbPropertyMapping().put(daoDescriptorProperty.getDbProperty(), aField.getName());

            }
        }

        return result;
    }

    private Set<Class<?>> findAnnotatedClasses(String basePackage) throws IOException, ClassNotFoundException {
        Set<Class<?>> classes = new HashSet<>();
        String path = basePackage.replace('.', '/');
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = net.sberg.jdbcgenericdao.core.DaoDescriptorHelper.class.getClassLoader();
        }
        Enumeration<URL> resources = classLoader.getResources(path);
        while (resources.hasMoreElements()) {
            URL resource = resources.nextElement();
            String protocol = resource.getProtocol();
            if ("file".equals(protocol)) {
                File dir = new File(resource.getFile());
                scanDirectory(basePackage, dir, classes, (Class<?>) DaoDescriptorClass.class);
            } else if ("jar".equals(protocol)) {
                JarURLConnection conn = (JarURLConnection) resource.openConnection();
                try (JarFile jarFile = conn.getJarFile()) {
                    Enumeration<JarEntry> entries = jarFile.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        if (name.startsWith(path) && name.endsWith(".class") && !entry.isDirectory()) {
                            String className = name.replace('/', '.').substring(0, name.length() - 6);
                            Class<?> cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                            if (cls.getAnnotation((Class) DaoDescriptorClass.class) != null) {
                                classes.add(cls);
                            }
                        }
                    }
                }
            }
        }
        return classes;
    }

    private void scanDirectory(String basePackage, File dir, Set<Class<?>> out, Class<?> annotationClass) throws ClassNotFoundException {
        if (!dir.exists() || !dir.isDirectory()) return;
        File[] files = dir.listFiles();
        if (files == null) return;
        for (File file : files) {
            if (file.isDirectory()) {
                scanDirectory(basePackage + "." + file.getName(), file, out, annotationClass);
            } else if (file.getName().endsWith(".class")) {
                String className = basePackage + '.' + file.getName().substring(0, file.getName().length() - 6);
                Class<?> cls = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
                if (cls.getAnnotation((Class) annotationClass) != null) {
                    out.add(cls);
                }
            }
        }
    }

}