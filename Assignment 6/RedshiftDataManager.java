import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties; // For connection options

public class RedshiftDataManager {

    private Connection dbConnection;
    private final String dbUrl;
    private final String dbUser;
    private final String dbPassword;

    public RedshiftDataManager(String url, String user, String password) {
        this.dbUrl = url;
        this.dbUser = user;
        this.dbPassword = password;
    }

    public static void main(String[] args) {
        // Redshift connection details, replace with yours!
        String redshiftJdbcUrl = "jdbc:redshift://redshift-cluster-1.cyng5xv3kiwd.eu-north-1.redshift.amazonaws.com:5439/dev";
        String redshiftUser = "admin";
        String redshiftPassword = "Mynameissourin00";

        RedshiftDataManager manager = new RedshiftDataManager(redshiftJdbcUrl, redshiftUser, redshiftPassword);

        try {
            manager.establishConnection();
            manager.dropAllTables();
            manager.createSchemaTables();
            manager.loadTpchData();

            System.out.println("\n--- Query 1: Recent Top 10 Orders in America ---");
            try (ResultSet results1 = manager.getRecentAmericanOrders()) {
                System.out.println(formatResultSet(results1, 10));
            }

            System.out.println("\n--- Query 2: Urgent, Non-Failed Orders from Largest Market Segment (Outside Europe) ---");
            try (ResultSet results2 = manager.getUrgentOrdersByLargestSegment()) {
                System.out.println(formatResultSet(results2, 10));
            }

            System.out.println("\n--- Query 3: Line Item Count by Order Priority (1997-2003) ---");
            try (ResultSet results3 = manager.getLineItemCountsByOrderPriority()) {
                System.out.println(formatResultSet(results3, 10));
            }

        } catch (SQLException e) {
            System.err.println("Something went wrong with the database: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Couldn't read a file: " + e.getMessage());
            e.printStackTrace();
        } finally {
            manager.closeConnection();
        }
    }

    public void establishConnection() throws SQLException {
        System.out.println("Trying to connect to the database...");
        try {
            Class.forName("com.amazon.redshift.jdbc.Driver"); // Load the Redshift driver

            Properties props = new Properties(); // Good for connection settings
            props.setProperty("user", dbUser);
            props.setProperty("password", dbPassword);

            dbConnection = DriverManager.getConnection(dbUrl, props);
            System.out.println("Connection successful!");

        } catch (ClassNotFoundException e) {
            System.err.println("Redshift driver not found. Check your classpath.");
            throw new SQLException("Driver not here", e);
        } catch (SQLException e) {
            System.err.println("Failed to connect: " + e.getMessage());
            throw e;
        }
    }

    public void closeConnection() {
        System.out.println("Closing database connection...");
        try {
            if (dbConnection != null && !dbConnection.isClosed()) {
                dbConnection.close();
                System.out.println("Connection closed.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }

    public void dropAllTables() {
        System.out.println("Dropping all the tables...");
        String[] dropStatements = {
                "DROP TABLE IF EXISTS LINEITEM CASCADE",
                "DROP TABLE IF EXISTS ORDERS CASCADE",
                "DROP TABLE IF EXISTS CUSTOMER CASCADE",
                "DROP TABLE IF EXISTS PARTSUPP CASCADE",
                "DROP TABLE IF EXISTS SUPPLIER CASCADE",
                "DROP TABLE IF EXISTS PART CASCADE",
                "DROP TABLE IF EXISTS NATION CASCADE",
                "DROP TABLE IF EXISTS REGION CASCADE"
        };

        try (Statement stmt = dbConnection.createStatement()) { // auto-close statement
            for (String sql : dropStatements) {
                try {
                    stmt.execute(sql);
                    System.out.println("Did: " + sql);
                } catch (SQLException e) {
                    System.out.println("Heads up: " + sql + " - " + e.getMessage());
                }
            }
            System.out.println("Tables dropped.");
        } catch (SQLException e) {
            System.err.println("Error dropping tables: " + e.getMessage());
        }
    }

    public void createSchemaTables() throws SQLException {
        System.out.println("Creating Tables now...");
        String[] createTableSqls = {
                "CREATE TABLE REGION (" +
                        "R_REGIONKEY INTEGER PRIMARY KEY, " +
                        "R_NAME CHAR(25), " +
                        "R_COMMENT VARCHAR(152))",

                "CREATE TABLE NATION (" +
                        "N_NATIONKEY INTEGER PRIMARY KEY, " +
                        "N_NAME CHAR(25), " +
                        "N_REGIONKEY BIGINT NOT NULL, " +
                        "N_COMMENT VARCHAR(152))",

                "CREATE TABLE PART (" +
                        "P_PARTKEY INTEGER PRIMARY KEY, " +
                        "P_NAME VARCHAR(55), " +
                        "P_MFGR CHAR(25), " +
                        "P_BRAND CHAR(10), " +
                        "P_TYPE VARCHAR(25), " +
                        "P_SIZE INTEGER, " +
                        "P_CONTAINER CHAR(10), " +
                        "P_RETAILPRICE DECIMAL, " +
                        "P_COMMENT VARCHAR(23))",

                "CREATE TABLE SUPPLIER (" +
                        "S_SUPPKEY INTEGER PRIMARY KEY, " +
                        "S_NAME CHAR(25), " +
                        "S_ADDRESS VARCHAR(40), " +
                        "S_NATIONKEY BIGINT NOT NULL, " +
                        "S_PHONE CHAR(15), " +
                        "S_ACCTBAL DECIMAL, " +
                        "S_COMMENT VARCHAR(101))",

                "CREATE TABLE PARTSUPP (" +
                        "PS_PARTKEY BIGINT NOT NULL, " +
                        "PS_SUPPKEY BIGINT NOT NULL, " +
                        "PS_AVAILQTY INTEGER, " +
                        "PS_SUPPLYCOST DECIMAL, " +
                        "PS_COMMENT VARCHAR(199), " +
                        "PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY))",

                "CREATE TABLE CUSTOMER (" +
                        "C_CUSTKEY INTEGER PRIMARY KEY, " +
                        "C_NAME VARCHAR(25), " +
                        "C_ADDRESS VARCHAR(40), " +
                        "C_NATIONKEY BIGINT NOT NULL, " +
                        "C_PHONE CHAR(15), " +
                        "C_ACCTBAL DECIMAL, " +
                        "C_MKTSEGMENT CHAR(10), " +
                        "C_COMMENT VARCHAR(117))",

                "CREATE TABLE ORDERS (" +
                        "O_ORDERKEY INTEGER PRIMARY KEY, " +
                        "O_CUSTKEY BIGINT NOT NULL, " +
                        "O_ORDERSTATUS CHAR(1), " +
                        "O_TOTALPRICE DECIMAL, " +
                        "O_ORDERDATE DATE, " +
                        "O_ORDERPRIORITY CHAR(15), " +
                        "O_CLERK CHAR(15), " +
                        "O_SHIPPRIORITY INTEGER, " +
                        "O_COMMENT VARCHAR(79))",

                "CREATE TABLE LINEITEM (" +
                        "L_ORDERKEY BIGINT NOT NULL, " +
                        "L_PARTKEY BIGINT NOT NULL, " +
                        "L_SUPPKEY BIGINT NOT NULL, " +
                        "L_LINENUMBER INTEGER, " +
                        "L_QUANTITY DECIMAL, " +
                        "L_EXTENDEDPRICE DECIMAL, " +
                        "L_DISCOUNT DECIMAL, " +
                        "L_TAX DECIMAL, " +
                        "L_RETURNFLAG CHAR(1), " +
                        "L_LINESTATUS CHAR(1), " +
                        "L_SHIPDATE DATE, " +
                        "L_COMMITDATE DATE, " +
                        "L_RECEIPTDATE DATE, " +
                        "L_SHIPINSTRUCT CHAR(25), " +
                        "L_SHIPMODE CHAR(10), " +
                        "L_COMMENT VARCHAR(44), " +
                        "PRIMARY KEY (L_ORDERKEY, L_LINENUMBER))"
        };

        try (Statement stmt = dbConnection.createStatement()) { // Auto-close statement
            for (String sql : createTableSqls) {
                stmt.execute(sql);
                System.out.println("Table created: " + sql.substring(0, Math.min(sql.length(), 50)) + "...");
            }
            System.out.println("All tables made.");
        } catch (SQLException e) {
            System.err.println("Problem making tables: " + e.getMessage());
            throw e;
        }
    }

    private String readSqlFileContent(String filename) throws IOException {
        // Reads the SQL file
        return new String(Files.readAllBytes(Paths.get("ddl", filename)));
    }

    public void loadTpchData() throws SQLException, IOException {
        System.out.println("Starting to load TPC-H data...");
        System.out.println("Looking for files in: " + System.getProperty("user.dir"));

        boolean originalAutoCommit = dbConnection.getAutoCommit(); // Save original setting
        try {
            dbConnection.setAutoCommit(false); // Make it faster

            String[] dataFiles = {
                    "region.sql", "nation.sql", "part.sql", "supplier.sql",
                    "partsupp.sql", "customer.sql", "orders.sql", "lineitem.sql"
            };

            for (String fileName : dataFiles) {
                System.out.println("Loading from: " + fileName);
                long startTime = System.currentTimeMillis();

                String sqlContent = readSqlFileContent(fileName);

                if (sqlContent.trim().isEmpty()) {
                    System.out.println("Warning: File '" + fileName + "' is empty.");
                    continue;
                }

                int recordsProcessed = executeOptimizedInserts(sqlContent);

                long endTime = System.currentTimeMillis();
                double durationSeconds = (endTime - startTime) / 1000.0;

                System.out.println("Loaded from '" + fileName + "'" +
                        " (" + recordsProcessed + " records in " +
                        String.format("%.2f", durationSeconds) + " seconds)");
            }

            dbConnection.commit(); // Save all changes
            System.out.println("Data loaded and saved.");

        } catch (SQLException | IOException e) {
            System.err.println("Error loading data. Trying to undo: " + e.getMessage());
            if (dbConnection != null) {
                try {
                    dbConnection.rollback(); // Undo if error
                    System.err.println("Changes undone.");
                } catch (SQLException rollbackEx) {
                    System.err.println("Error during undo: " + rollbackEx.getMessage());
                }
            }
            throw e; // Pass on the error
        } finally {
            dbConnection.setAutoCommit(originalAutoCommit); // Set auto-commit back
        }
    }

    private int executeOptimizedInserts(String sqlData) throws SQLException {
        String[] individualStatements = sqlData.split(";\n");

        List<String> rawValuesList = new ArrayList<>();
        String insertBaseTemplate = "";
        String targetTableName = "";

        // Try to combine inserts
        for (String stmt : individualStatements) {
            stmt = stmt.trim();
            if (!stmt.isEmpty() && stmt.toUpperCase().startsWith("INSERT INTO")) {
                if (targetTableName.isEmpty()) {
                    // Get table name for multi-insert
                    int intoKeywordIndex = stmt.toUpperCase().indexOf("INTO ") + 5;
                    int valuesKeywordIndex = stmt.toUpperCase().indexOf(" VALUES");
                    if (intoKeywordIndex > 4 && valuesKeywordIndex > intoKeywordIndex) {
                        targetTableName = stmt.substring(intoKeywordIndex, valuesKeywordIndex).trim();
                        insertBaseTemplate = "INSERT INTO " + targetTableName + " VALUES ";
                    }
                }

                int valuesPartIndex = stmt.toUpperCase().indexOf("VALUES");
                if (valuesPartIndex != -1) {
                    String valuesSegment = stmt.substring(valuesPartIndex + 6).trim();
                    rawValuesList.add(valuesSegment);
                }
            }
        }

        if (rawValuesList.isEmpty() || targetTableName.isEmpty()) {
            System.out.println("  Can't do multi-row insert. Using regular batch inserts.");
            return executeBatchInserts(sqlData);
        }

        System.out.println("  Making " + rawValuesList.size() + " inserts into multi-row statements for: " + targetTableName);

        int multiRowStatementSize = calculateMultiRowBatchSize(rawValuesList.size());

        System.out.println("  Using a multi-row batch size of: " + multiRowStatementSize + " rows per INSERT.");

        int totalInsertedRecords = 0;
        long methodStartTime = System.currentTimeMillis();

        try (Statement statementExecutor = dbConnection.createStatement()) { // Auto-close statement
            for (int i = 0; i < rawValuesList.size(); i += multiRowStatementSize) {
                StringBuilder multiValueInsertBuilder = new StringBuilder(insertBaseTemplate);
                int endIndex = Math.min(i + multiRowStatementSize, rawValuesList.size());

                // Put many VALUES together
                for (int j = i; j < endIndex; j++) {
                    if (j > i) {
                        multiValueInsertBuilder.append(", ");
                    }
                    multiValueInsertBuilder.append(rawValuesList.get(j));
                }

                statementExecutor.execute(multiValueInsertBuilder.toString());
                int currentBatchCount = endIndex - i;
                totalInsertedRecords += currentBatchCount;

                printProgressUpdate(totalInsertedRecords, rawValuesList.size(), methodStartTime);
            }
        }
        return totalInsertedRecords;
    }

    private static int calculateMultiRowBatchSize(int totalRecords) {
        // Decide how many rows per multi-row insert
        if (totalRecords > 500000) {
            return 10000;
        } else if (totalRecords > 100000) {
            return 5000;
        } else if (totalRecords > 10000) {
            return 2000;
        } else if (totalRecords > 1000) {
            return 1000;
        } else {
            return 500;
        }
    }

    private int executeBatchInserts(String sqlData) throws SQLException {
        String[] statementsArray = sqlData.split(";\n");

        List<String> actualInsertStatements = new ArrayList<>();
        for (String stmt : statementsArray) {
            stmt = stmt.trim();
            if (!stmt.isEmpty() && stmt.toUpperCase().startsWith("INSERT")) {
                actualInsertStatements.add(stmt);
            }
        }

        if (actualInsertStatements.isEmpty()) {
            return 0;
        }

        System.out.println("  Doing " + actualInsertStatements.size() + " INSERTs in batches.");

        int batchOperationSize = calculateStandardBatchSize(actualInsertStatements.size());
        System.out.println("  Batch size: " + batchOperationSize);

        int totalRecordsInserted = 0;
        long operationStartTime = System.currentTimeMillis();

        try (Statement batchStatement = dbConnection.createStatement()) { // Auto-close statement
            for (int i = 0; i < actualInsertStatements.size(); i += batchOperationSize) {
                int endIndex = Math.min(i + batchOperationSize, actualInsertStatements.size());

                for (int j = i; j < endIndex; j++) {
                    batchStatement.addBatch(actualInsertStatements.get(j));
                }

                int[] results = batchStatement.executeBatch(); // Run the batch
                batchStatement.clearBatch();

                for (int res : results) {
                    if (res >= 0) {
                        totalRecordsInserted += (res > 0 ? res : 1);
                    }
                }
                printProgressUpdate(totalRecordsInserted, actualInsertStatements.size(), operationStartTime);
            }
        }
        return totalRecordsInserted;
    }

    private static int calculateStandardBatchSize(int totalStatements) {
        // Decides how big each batch should be
        if (totalStatements > 100000) {
            return 5000;
        } else if (totalStatements > 10000) {
            return 2000;
        } else if (totalStatements > 1000) {
            return 1000;
        } else {
            return 500;
        }
    }

    private void printProgressUpdate(int currentCount, int totalCount, long startTimeMillis) {
        // Shows how much data has been loaded
        long elapsedTime = System.currentTimeMillis() - startTimeMillis;
        double elapsedSeconds = elapsedTime / 1000.0;
        double recordsPerSecond = elapsedSeconds > 0 ? currentCount / elapsedSeconds : 0.0;

        System.out.printf("  Progress: %d/%d (%.1f%%) | Speed: %.0f records/sec%n",
                currentCount, totalCount, (currentCount * 100.0 / totalCount), recordsPerSecond);
    }

    // --- Query Methods ---

    public ResultSet getRecentAmericanOrders() throws SQLException {
        System.out.println("Running Query 1: Top 10 latest orders from America...");
        String sqlQuery = "SELECT o.O_ORDERKEY, o.O_TOTALPRICE, o.O_ORDERDATE " +
                          "FROM ORDERS o " +
                          "JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY " +
                          "JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY " +
                          "JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY " +
                          "WHERE r.R_NAME = 'AMERICA' " +
                          "ORDER BY o.O_ORDERDATE DESC " +
                          "LIMIT 10";
        Statement stmt = dbConnection.createStatement();
        return stmt.executeQuery(sqlQuery);
    }

    public ResultSet getUrgentOrdersByLargestSegment() throws SQLException {
        System.out.println("Running Query 2: Finding urgent orders from the biggest market group (not Europe)...");
        String sqlQuery = "WITH TopMarketSegment AS (" +
                          "  SELECT C_MKTSEGMENT " +
                          "  FROM CUSTOMER " +
                          "  GROUP BY C_MKTSEGMENT " +
                          "  ORDER BY COUNT(*) DESC " +
                          "  LIMIT 1" +
                          ") " +
                          "SELECT c.C_CUSTKEY, SUM(o.O_TOTALPRICE) AS total_amount_spent " +
                          "FROM CUSTOMER c " +
                          "JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY " +
                          "JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY " +
                          "JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY " +
                          "WHERE o.O_ORDERPRIORITY = '1-URGENT' " +
                          "AND o.O_ORDERSTATUS <> 'F' " +
                          "AND r.R_NAME <> 'EUROPE' " +
                          "AND c.C_MKTSEGMENT = (SELECT C_MKTSEGMENT FROM TopMarketSegment) " +
                          "GROUP BY c.C_CUSTKEY " +
                          "ORDER BY total_amount_spent DESC";

        Statement stmt = dbConnection.createStatement();
        return stmt.executeQuery(sqlQuery);
    }

    public ResultSet getLineItemCountsByOrderPriority() throws SQLException {
        System.out.println("Running Query 3: Counting line items by order priority (April 1997 - March 2003)...");
        String sqlQuery = "SELECT o.O_ORDERPRIORITY, COUNT(l.L_LINENUMBER) AS line_item_count " +
                          "FROM LINEITEM l " +
                          "JOIN ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY " +
                          "WHERE o.O_ORDERDATE >= '1997-04-01' " +
                          "AND o.O_ORDERDATE < '2003-04-01' " +
                          "GROUP BY o.O_ORDERPRIORITY " +
                          "ORDER BY o.O_ORDERPRIORITY";

        Statement stmt = dbConnection.createStatement();
        return stmt.executeQuery(sqlQuery);
    }

    // --- ResultSet formatting methods (standard helpers) ---

    public static String formatResultSet(ResultSet resultSet, int displayMaxRows) throws SQLException {
        StringBuilder outputBuilder = new StringBuilder();
        int rowCounter = 0;

        ResultSetMetaData metaData = resultSet.getMetaData();
        outputBuilder.append("ResultSet Details:\n").append(formatResultSetMetaData(metaData)).append("\n\n");

        outputBuilder.append("Total Columns: ").append(metaData.getColumnCount()).append("\n");

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            outputBuilder.append(metaData.getColumnName(i));
            if (i < metaData.getColumnCount()) {
                outputBuilder.append(", ");
            }
        }
        outputBuilder.append("\n");

        while (resultSet.next()) {
            if (rowCounter < displayMaxRows) {
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    Object columnValue = resultSet.getObject(i);
                    outputBuilder.append(columnValue != null ? columnValue.toString() : "NULL");
                    if (i < metaData.getColumnCount()) {
                        outputBuilder.append(", ");
                    }
                }
                outputBuilder.append("\n");
            }
            rowCounter++;
        }
        outputBuilder.append("Total Results: ").append(rowCounter).append("\n");
        return outputBuilder.toString();
    }

    public static String formatResultSetMetaData(ResultSetMetaData metaData) throws SQLException {
        StringBuilder metaDataBuilder = new StringBuilder();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            metaDataBuilder.append(metaData.getColumnName(i))
                    .append(" (Label: ").append(metaData.getColumnLabel(i))
                    .append(", Type: ").append(metaData.getColumnType(i))
                    .append("-").append(metaData.getColumnTypeName(i))
                    .append(", DisplaySize: ").append(metaData.getColumnDisplaySize(i))
                    .append(", Precision: ").append(metaData.getPrecision(i))
                    .append(", Scale: ").append(metaData.getScale(i))
                    .append(")");
            if (i < metaData.getColumnCount()) {
                metaDataBuilder.append("\n");
            }
        }
        return metaDataBuilder.toString();
    }
}