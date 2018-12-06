public class JdbcConnector {

    public static Connection getConnection(String fileName) {
        Properties properties = PropertyRetriever.retrieveAllProperties(fileName);
        Connection connection = null;
        String driver = properties.getProperty("driverClassName");
        String url = properties.getProperty("url");
        String user = properties.getProperty("username");
        String password = "test_password"；
        connection = establishConnection(driver, url, user, password);
        return connection;
    }
}
