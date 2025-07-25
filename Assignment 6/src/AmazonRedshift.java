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


public class AmazonRedshift {

    private Connection con;


    public static void main(String[] args) throws SQLException {
        AmazonRedshift q = new AmazonRedshift();
        q.connect();
        q.drop();
        q.create();
        q.insert();

        // Execute queries and display results
        ResultSet rs1 = q.query1();
        System.out.println("Query 1 Results:");
        System.out.println(resultSetToString(rs1, 10));

        ResultSet rs2 = q.query2();
        System.out.println("\nQuery 2 Results:");
        System.out.println(resultSetToString(rs2, 10));

        ResultSet rs3 = q.query3();
        System.out.println("\nQuery 3 Results:");
        System.out.println(resultSetToString(rs3, 10));

        q.close();
    }

    public void connect() throws SQLException {
        System.out.println("Connecting to database.");
        try {
            // Load the Redshift JDBC driver
            Class.forName("com.amazon.redshift.jdbc.Driver");

            // Establish connection
            // Replace these with your actual Redshift cluster details
            String url = "jdbc:redshift://redshift-cluster-1.cyng5xv3kiwd.eu-north-1.redshift.amazonaws.com:5439/dev";
            String uid = "admin";
            String pw = "Mynameissourin00";
            con = DriverManager.getConnection(url, uid, pw);
            System.out.println("Connection successful!");

        } catch (ClassNotFoundException e) {
            System.err.println("Redshift JDBC Driver not found. Add it to your classpath.");
            throw new SQLException("Driver not found", e);
        } catch (SQLException e) {
            System.err.println("Connection failed: " + e.getMessage());
            throw e;
        }
    }


    public void close() {
        System.out.println("Closing database connection.");
        try {
            if (con != null && !con.isClosed()) {
                con.close();
                System.out.println("Connection closed successfully.");
            }
        } catch (SQLException e) {
            System.err.println("Error closing connection: " + e.getMessage());
        }
    }


    public void drop() {
        System.out.println("Dropping all the tables");
        try {
            Statement stmt = con.createStatement();

            // Drop tables in reverse order of dependencies (matching the tpch_create.sql)
            String[] dropQueries = {
                    "DROP TABLE IF EXISTS LINEITEM CASCADE",
                    "DROP TABLE IF EXISTS ORDERS CASCADE",
                    "DROP TABLE IF EXISTS CUSTOMER CASCADE",
                    "DROP TABLE IF EXISTS PARTSUPP CASCADE",
                    "DROP TABLE IF EXISTS SUPPLIER CASCADE",
                    "DROP TABLE IF EXISTS PART CASCADE",
                    "DROP TABLE IF EXISTS NATION CASCADE",
                    "DROP TABLE IF EXISTS REGION CASCADE"
            };

            for (String query : dropQueries) {
                try {
                    stmt.execute(query);
                    System.out.println("Executed: " + query);
                } catch (SQLException e) {
                    System.out.println("Note: " + query + " - " + e.getMessage());
                }
            }

            stmt.close();
            System.out.println("All tables dropped successfully.");

        } catch (SQLException e) {
            System.err.println("Error dropping tables: " + e.getMessage());
        }
    }

    public void create() throws SQLException {
        System.out.println("Creating Tables");
        try {
            Statement stmt = con.createStatement();

            // Create TPC-H tables based on the provided schema
            String[] createQueries = {
                    // Region table
                    "CREATE TABLE REGION (" +
                            "R_REGIONKEY INTEGER PRIMARY KEY, " +
                            "R_NAME CHAR(25), " +
                            "R_COMMENT VARCHAR(152))",

                    // Nation table
                    "CREATE TABLE NATION (" +
                            "N_NATIONKEY INTEGER PRIMARY KEY, " +
                            "N_NAME CHAR(25), " +
                            "N_REGIONKEY BIGINT NOT NULL, " +
                            "N_COMMENT VARCHAR(152))",

                    // Part table
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

                    // Supplier table
                    "CREATE TABLE SUPPLIER (" +
                            "S_SUPPKEY INTEGER PRIMARY KEY, " +
                            "S_NAME CHAR(25), " +
                            "S_ADDRESS VARCHAR(40), " +
                            "S_NATIONKEY BIGINT NOT NULL, " +
                            "S_PHONE CHAR(15), " +
                            "S_ACCTBAL DECIMAL, " +
                            "S_COMMENT VARCHAR(101))",

                    // Partsupp table
                    "CREATE TABLE PARTSUPP (" +
                            "PS_PARTKEY BIGINT NOT NULL, " +
                            "PS_SUPPKEY BIGINT NOT NULL, " +
                            "PS_AVAILQTY INTEGER, " +
                            "PS_SUPPLYCOST DECIMAL, " +
                            "PS_COMMENT VARCHAR(199), " +
                            "PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY))",

                    // Customer table
                    "CREATE TABLE CUSTOMER (" +
                            "C_CUSTKEY INTEGER PRIMARY KEY, " +
                            "C_NAME VARCHAR(25), " +
                            "C_ADDRESS VARCHAR(40), " +
                            "C_NATIONKEY BIGINT NOT NULL, " +
                            "C_PHONE CHAR(15), " +
                            "C_ACCTBAL DECIMAL, " +
                            "C_MKTSEGMENT CHAR(10), " +
                            "C_COMMENT VARCHAR(117))",

                    // Orders table
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

                    // Lineitem table
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

            for (String query : createQueries) {
                stmt.execute(query);
                System.out.println("Table created successfully.");
            }

            stmt.close();
            System.out.println("All tables created successfully.");

        } catch (SQLException e) {
            System.err.println("Error creating tables: " + e.getMessage());
            throw e;
        }
    }


    private String readSQLFile(String filename) throws IOException {
        return new String(Files.readAllBytes(Paths.get("ddl", filename)));
    }


    public void insert() throws SQLException {
        System.out.println("Loading TPC-H Data");
        System.out.println("Current working directory: " + System.getProperty("user.dir"));

        try {
            // Disable auto-commit for better performance
            con.setAutoCommit(false);

            // List of DDL files to execute in order (matching dependency order)
            String[] ddlFiles = {
                    "region.sql",
                    "nation.sql",
                    "part.sql",
                    "supplier.sql",
                    "partsupp.sql",
                    "customer.sql",
                    "orders.sql",
                    "lineitem.sql"
            };

            for (String file : ddlFiles) {
                try {
                    System.out.println("Loading data from: " + file);
                    long startTime = System.currentTimeMillis();

                    String sql = readSQLFile(file);

                    if (sql.trim().isEmpty()) {
                        System.out.println("Warning: File " + file + " is empty or contains no data");
                        continue;
                    }


                    int recordCount = insertDataOptimized(sql);

                    long endTime = System.currentTimeMillis();
                    double seconds = (endTime - startTime) / 1000.0;

                    System.out.println("Data loaded successfully from: " + file +
                            " (" + recordCount + " records in " +
                            String.format("%.2f", seconds) + " seconds)");

                } catch (IOException e) {
                    System.err.println("Error reading file " + file + ": " + e.getMessage());
                    // Continue with the next file instead of failing completely
                } catch (SQLException e) {
                    System.err.println("Database error with " + file + ": " + e.getMessage());
                    // Roll back this file's changes and continue
                    con.rollback();
                }
            }

            // Final commit for all successful operations
            con.commit();
            con.setAutoCommit(true);

            System.out.println("Data loading process completed successfully.");

        } catch (SQLException e) {
            System.err.println("Error during data loading: " + e.getMessage());
            try {
                con.rollback();
                con.setAutoCommit(true);
            } catch (SQLException rollbackEx) {
                System.err.println("Error during rollback: " + rollbackEx.getMessage());
            }
            throw e;
        }
    }

    private int insertDataOptimized(String sql) throws SQLException {
        String[] statements = sql.split(";\n");

        List<String> valuesList = new ArrayList<>();
        String tableName = "";
        String insertTemplate = "";

        // Parse INSERT statements and extract VALUES
        for (String statement : statements) {
            statement = statement.trim();
            if (!statement.isEmpty() && statement.toUpperCase().startsWith("INSERT")) {
                if (tableName.isEmpty()) {
                    // Extract table name and INSERT template
                    // Example: "INSERT INTO customer VALUES" -> table = "customer"
                    String upperStmt = statement.toUpperCase();
                    int intoPos = upperStmt.indexOf("INTO ") + 5;
                    int valuesPos = upperStmt.indexOf(" VALUES");
                    if (intoPos > 4 && valuesPos > intoPos) {
                        tableName = statement.substring(intoPos, valuesPos).trim();
                        insertTemplate = "INSERT INTO " + tableName + " VALUES ";
                    }
                }

                // Extract just the VALUES part: (data1, data2, ...)
                int valuesIndex = statement.toUpperCase().indexOf("VALUES");
                if (valuesIndex != -1) {
                    String valuesSubstring = statement.substring(valuesIndex + 6).trim();
                    valuesList.add(valuesSubstring);
                }
            }
        }

        if (valuesList.isEmpty() || tableName.isEmpty()) {
            System.out.println("  No valid INSERT statements found, falling back to batch method");
            return insertDataInBatches(sql);
        }

        System.out.println("  Converting " + valuesList.size() + " INSERT to multi-row INSERT for table: " + tableName);

        // Use very large batch sizes for multi-row INSERT
        int multiRowBatchSize = getMultiRowBatchSize(valuesList);

        System.out.println("  Using multi-row batch size: " + multiRowBatchSize + " rows per INSERT");

        int totalInserted = 0;
        long startTime = System.currentTimeMillis();

        try (Statement stmt = con.createStatement()) {
            // Process in multi-row batches
            for (int i = 0; i < valuesList.size(); i += multiRowBatchSize) {
                StringBuilder multiInsert = new StringBuilder(insertTemplate);

                int endIndex = Math.min(i + multiRowBatchSize, valuesList.size());

                // Combine multiple VALUES into single INSERT
                for (int j = i; j < endIndex; j++) {
                    if (j > i) {
                        multiInsert.append(", ");
                    }
                    multiInsert.append(valuesList.get(j));
                }

                // Execute the multi-row INSERT
                stmt.execute(multiInsert.toString());

                int batchCount = endIndex - i;
                totalInserted += batchCount;

                // Show progress with performance metrics
                showProgress(totalInserted, valuesList, startTime);
            }

        }

        return totalInserted;
    }

    private static int getMultiRowBatchSize(List<String> valuesList) {
        int multiRowBatchSize;
        if (valuesList.size() > 50000) {
            multiRowBatchSize = 5000;  // 5K rows per statement for huge datasets
        } else if (valuesList.size() > 10000) {
            multiRowBatchSize = 2000;  // 2K rows per statement for large datasets
        } else if (valuesList.size() > 1000) {
            multiRowBatchSize = 1000;  // 1K rows per statement for medium datasets
        } else {
            multiRowBatchSize = 500;   // 500 rows per statement for smaller datasets
        }
        return multiRowBatchSize;
    }

    private int insertDataInBatches(String sql) throws SQLException {
        // Split into individual INSERT statements
        String[] statements = sql.split(";\n");

        int totalRecords = 0;

        // Count actual INSERT statements first
        List<String> insertStatements = new ArrayList<>();
        for (String statement : statements) {
            statement = statement.trim();
            if (!statement.isEmpty() && statement.toUpperCase().startsWith("INSERT")) {
                insertStatements.add(statement);
            }
        }

        if (insertStatements.isEmpty()) {
            return 0;
        }

        System.out.println("  Found " + insertStatements.size() + " INSERT statements to process");

        // Use much larger batch sizes for better performance
        int batchSize;
        if (insertStatements.size() > 10000) {
            batchSize = 2000;  // For very large files
        } else if (insertStatements.size() > 1000) {
            batchSize = 1000;  // For medium files
        } else {
            batchSize = 500;   // For smaller files
        }

        System.out.println("  Using batch size: " + batchSize);

        try (Statement stmt = con.createStatement()) {
            long startTime = System.currentTimeMillis();
            // Process in large batches
            for (int i = 0; i < insertStatements.size(); i += batchSize) {
                int endIndex = Math.min(i + batchSize, insertStatements.size());

                // Add statements to batch
                for (int j = i; j < endIndex; j++) {
                    stmt.addBatch(insertStatements.get(j));
                }

                // Execute batch
                stmt.executeBatch();
                stmt.clearBatch();

                totalRecords += (endIndex - i);

                // Show progress with timing
                showProgress(totalRecords, insertStatements, startTime);
            }

        }

        return totalRecords;
    }

    private void showProgress(int totalRecords, List<String> insertStatements, long startTime) {
        long currentTime = System.currentTimeMillis();
        double elapsed = (currentTime - startTime) / 1000.0;
        double rate = totalRecords / elapsed;

        System.out.println("    Progress: " + totalRecords + "/" + insertStatements.size() +
                " (" + String.format("%.1f", (totalRecords * 100.0 / insertStatements.size())) + "%) " +
                "Rate: " + String.format("%.0f", rate) + " records/sec");
    }

    /**
     * Query returns the most recent top 10 orders with the total sale and the date of the
     * order for customers in America.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query1() throws SQLException {
        System.out.println("Executing query #1.");

        String sql = "SELECT o.O_ORDERKEY, o.O_TOTALPRICE, o.O_ORDERDATE " +
                "FROM ORDERS o " +
                "JOIN CUSTOMER c ON o.O_CUSTKEY = c.C_CUSTKEY " +
                "JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY " +
                "JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY " +
                "WHERE r.R_NAME = 'AMERICA' " +
                "ORDER BY o.O_ORDERDATE DESC " +
                "LIMIT 10";

        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * Query returns the customer key and the total price a customer spent in descending
     * order, for all urgent orders that are not failed for all customers who are outside Europe
     * and belong to the largest market segment.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query2() throws SQLException {
        System.out.println("Executing query #2.");

        String sql = "WITH largest_segment AS (" +
                "    SELECT C_MKTSEGMENT " +
                "    FROM CUSTOMER " +
                "    GROUP BY C_MKTSEGMENT " +
                "    ORDER BY COUNT(*) DESC " +
                "    LIMIT 1" +
                ") " +
                "SELECT c.C_CUSTKEY, SUM(o.O_TOTALPRICE) as total_spent " +
                "FROM CUSTOMER c " +
                "JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY " +
                "JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY " +
                "JOIN REGION r ON n.N_REGIONKEY = r.R_REGIONKEY " +
                "WHERE o.O_ORDERPRIORITY = '1-URGENT' " +
                "AND o.O_ORDERSTATUS != 'F' " +
                "AND r.R_NAME != 'EUROPE' " +
                "AND c.C_MKTSEGMENT = (SELECT C_MKTSEGMENT FROM largest_segment) " +
                "GROUP BY c.C_CUSTKEY " +
                "ORDER BY total_spent DESC";

        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * Query returns a count of all the line items that were ordered within the six years
     * starting on April 1st, 1997 grouped by order priority.
     *
     * @return ResultSet
     * @throws SQLException if an error occurs
     */
    public ResultSet query3() throws SQLException {
        System.out.println("Executing query #3.");

        String sql = "SELECT o.O_ORDERPRIORITY, COUNT(l.L_LINENUMBER) as lineitem_count " +
                "FROM LINEITEM l " +
                "JOIN ORDERS o ON l.L_ORDERKEY = o.O_ORDERKEY " +
                "WHERE o.O_ORDERDATE >= '1997-04-01' " +
                "AND o.O_ORDERDATE < '2003-04-01' " +
                "GROUP BY o.O_ORDERPRIORITY " +
                "ORDER BY o.O_ORDERPRIORITY ";

        Statement stmt = con.createStatement();
        return stmt.executeQuery(sql);
    }

    /*
     * Do not change anything below here.
     */

    /**
     * Converts a ResultSet to a string with a given number of rows displayed.
     * Total rows are determined, but only the first few are put into a string.
     *
     * @param rst     ResultSet
     * @param maxRows maximum number of rows to display
     * @return String form of results
     * @throws SQLException if a database error occurs
     */
    public static String resultSetToString(ResultSet rst, int maxRows) throws SQLException {
        StringBuilder buf = new StringBuilder(5000);
        int rowCount = 0;

        ResultSetMetaData meta = rst.getMetaData();
        System.out.println("ResultSet metadata: \n" + resultSetMetaDataToString(meta));

        buf.append("Total columns: ").append(meta.getColumnCount());
        buf.append('\n');
        if (meta.getColumnCount() > 0)
            buf.append(meta.getColumnName(1));
        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", ").append(meta.getColumnName(j));
        buf.append('\n');

        while (rst.next()) {
            if (rowCount < maxRows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1)
                        buf.append(", ");
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: ").append(rowCount);
        return buf.toString();
    }

    /**
     * Converts ResultSetMetaData into a string.
     *
     * @param meta ResultSetMetaData
     * @return string form of metadata
     * @throws SQLException if a database error occurs
     */
    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuilder buf = new StringBuilder(5000);
        buf.append(meta.getColumnName(1)).append(" (").append(meta.getColumnLabel(1)).append(", ").append(meta.getColumnType(1)).append("-").append(meta.getColumnTypeName(1)).append(", ").append(meta.getColumnDisplaySize(1)).append(", ").append(meta.getPrecision(1)).append(", ").append(meta.getScale(1)).append(")");

        for (int j = 2; j <= meta.getColumnCount(); j++)
            buf.append(", ").append(meta.getColumnName(j)).append(" (").append(meta.getColumnLabel(j)).append(", ").append(meta.getColumnType(j)).append("-").append(meta.getColumnTypeName(j)).append(", ").append(meta.getColumnDisplaySize(j)).append(", ").append(meta.getPrecision(j)).append(", ").append(meta.getScale(j)).append(")");
        return buf.toString();
    }
}