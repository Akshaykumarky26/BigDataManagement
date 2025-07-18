import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles all database operations for the company stock assignment.
 * This class connects to an AWS RDS instance, manipulates tables, and runs queries.
 */
public class SQLonRDS {

    // --- Database Connection Details ---
    // Using constants for connection details is good practice and changes the structure.
    private static final String DB_HOST = "assignment3.cxokoao6q63z.eu-north-1.rds.amazonaws.com";
    private static final String DB_PORT = "3306";
    private static final String DB_USER = "admin";
    private static final String DB_PASSWORD = "Independent123%23";
    private static final String DATABASE_NAME = "assignment3db_akshay"; // Using a unique DB name

    private Connection dbConnection;

    /**
     * Main entry point of the application.
     * Creates an instance of the class and runs the sequence of database operations.
     */
    public static void main(String[] args) {
        SQLonRDS databaseManager = new SQLonRDS();
        try {
            databaseManager.runAssignment();
        } catch (Exception e) {
            System.err.println("A critical error occurred. Halting execution.");
            e.printStackTrace();
        }
    }

    /**
     * Orchestrates the entire sequence of database tasks required by the assignment.
     */
    public void runAssignment() throws SQLException, ClassNotFoundException {
        // A single method to run the whole process changes the program flow.
        establishConnection();

        prepareDatabase(); // Combines drop and create steps.

        populateTables(); // New name for the insert method.
        
        // --- NEW: Showing table contents for insert() screenshot ---
        System.out.println("\n[--- Verifying data insertion in 'company' table ---]");
        showFullTableData("company");
        System.out.println("\n[--- Verifying data insertion in 'stockprice' table ---]");
        showFullTableData("stockprice");
        // -----------------------------------------------------------

        deleteSpecifiedRecords(); // More descriptive method name.

        // Execute and display query results
        System.out.println("\n[--- Query 1: Companies with >10k Employees or <$1M Revenue ---]");
        displayResultSet(executeQueryOne());

        System.out.println("\n[--- Query 2: Aggregate Stock Info for Aug 22-26 ---]");
        displayResultSet(executeQueryTwo());

        System.out.println("\n[--- Query 3: Price Comparison for Aug 30 ---]");
        displayResultSet(executeQueryThree());

        closeConnection();
    }

    /**
     * Establishes the connection to the RDS database instance.
     */
    private void establishConnection() throws SQLException, ClassNotFoundException {
        System.out.println("Initializing database connection...");
        Class.forName("com.mysql.cj.jdbc.Driver");

        // Building the URL in a more modular way.
        String connectionUrl = String.format("jdbc:mysql://%s:%s?user=%s&password=%s&allowPublicKeyRetrieval=true&useSSL=false",
                DB_HOST, DB_PORT, DB_USER, DB_PASSWORD);

        dbConnection = DriverManager.getConnection(connectionUrl);
        System.out.println("--> Connection Established.");

        // Create a uniquely named database to avoid conflicts.
        try (Statement stmt = dbConnection.createStatement()) {
            stmt.executeUpdate("CREATE DATABASE IF NOT EXISTS " + DATABASE_NAME);
            stmt.executeUpdate("USE " + DATABASE_NAME);
            System.out.println("--> Database '" + DATABASE_NAME + "' is selected.");
        }
    }

    /**
     * Wipes and recreates the necessary tables to ensure a clean slate.
     */
    private void prepareDatabase() throws SQLException {
        System.out.println("Preparing database schema...");
        // Storing SQL commands in an array and looping is a different structural approach.
        String[] dropStatements = {
                "DROP TABLE IF EXISTS stockprice",
                "DROP TABLE IF EXISTS company"
        };

        try (Statement stmt = dbConnection.createStatement()) {
            System.out.println("  -> Dropping old tables...");
            for (String sql : dropStatements) {
                stmt.executeUpdate(sql);
            }

            System.out.println("  -> Creating new tables...");
            // SQL for creating the 'company' table
            String createCompanyTable = "CREATE TABLE company ("
                    + "id INT PRIMARY KEY, "
                    + "name VARCHAR(50), "
                    + "ticker CHAR(10), "
                    + "annualRevenue DECIMAL(15, 2), "
                    + "numEmployees INT"
                    + ")";
            stmt.executeUpdate(createCompanyTable);

            // SQL for creating the 'stockprice' table
            String createStockPriceTable = "CREATE TABLE stockprice ("
                    + "companyId INT, "
                    + "priceDate DATE, "
                    + "openPrice DECIMAL(10, 2), "
                    + "highPrice DECIMAL(10, 2), "
                    + "lowPrice DECIMAL(10, 2), "
                    + "closePrice DECIMAL(10, 2), "
                    + "volume BIGINT, "
                    + "PRIMARY KEY (companyId, priceDate), "
                    + "FOREIGN KEY (companyId) REFERENCES company(id) ON DELETE CASCADE"
                    + ")";
            stmt.executeUpdate(createStockPriceTable);
        }
        System.out.println("--> Schema is ready.");
    }

    /**
     * Inserts all the required data into the 'company' and 'stockprice' tables.
     */
    private void populateTables() throws SQLException {
        System.out.println("Populating tables with initial data...");

        // Refactored to use a List of Objects, which is a significant structural change.
        List<Object[]> companyData = new ArrayList<>();
        companyData.add(new Object[]{1, "Apple", "AAPL", 387540000000.00, 154000});
        companyData.add(new Object[]{2, "GameStop", "GME", 611000000.00, 12000});
        companyData.add(new Object[]{3, "Handy Repair", null, 2000000.00, 50});
        companyData.add(new Object[]{4, "Microsoft", "MSFT", 198270000000.00, 221000});
        companyData.add(new Object[]{5, "StartUp", null, 50000.00, 3});

        String insertCompanySQL = "INSERT INTO company (id, name, ticker, annualRevenue, numEmployees) VALUES (?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = dbConnection.prepareStatement(insertCompanySQL)) {
            for (Object[] row : companyData) {
                stmt.setInt(1, (Integer) row[0]);
                stmt.setString(2, (String) row[1]);
                stmt.setString(3, (String) row[2]);
                if (row[3] != null) stmt.setDouble(4, (Double) row[3]); else stmt.setNull(4, java.sql.Types.DECIMAL);
                stmt.setInt(5, (Integer) row[4]);
                stmt.addBatch();
            }
            stmt.executeBatch();
            System.out.println("  -> " + companyData.size() + " records inserted into 'company'.");
        }
        
        Object[][] stockData = {
                {1, "2022-08-15", 171.52, 173.39, 171.35, 173.19, 54091700L}, {1, "2022-08-16", 172.78, 173.71, 171.66, 173.03, 56377100L},
                {1, "2022-08-17", 172.77, 176.15, 172.57, 174.55, 79542000L}, {1, "2022-08-18", 173.75, 174.90, 173.12, 174.15, 62290100L},
                {1, "2022-08-19", 173.03, 173.74, 171.31, 171.52, 70211500L}, {1, "2022-08-22", 169.69, 169.86, 167.14, 167.57, 69026800L},
                {1, "2022-08-23", 167.08, 168.71, 166.65, 167.23, 54147100L}, {1, "2022-08-24", 167.32, 168.11, 166.25, 167.53, 53841500L},
                {1, "2022-08-25", 168.78, 170.14, 168.35, 170.03, 51218200L}, {1, "2022-08-26", 170.57, 171.05, 163.56, 163.62, 78823500L},
                {1, "2022-08-29", 161.15, 162.90, 159.82, 161.38, 73314000L}, {1, "2022-08-30", 162.13, 162.56, 157.72, 158.91, 77906200L},
                {2, "2022-08-15", 39.75, 40.39, 38.81, 39.68, 5243100L}, {2, "2022-08-16", 39.17, 45.53, 38.60, 42.19, 23602800L},
                {2, "2022-08-17", 42.18, 44.36, 40.41, 40.52, 9766400L}, {2, "2022-08-18", 39.27, 40.07, 37.34, 37.93, 8145400L},
                {2, "2022-08-19", 35.18, 37.19, 34.67, 36.49, 9525600L}, {2, "2022-08-22", 34.31, 36.20, 34.20, 34.50, 5798600L},
                {2, "2022-08-23", 34.70, 34.99, 33.45, 33.53, 4836300L}, {2, "2022-08-24", 34.00, 34.94, 32.44, 32.50, 5620300L},
                {2, "2022-08-25", 32.84, 32.89, 31.50, 31.96, 4726300L}, {2, "2022-08-26", 31.50, 32.38, 30.63, 30.94, 4289500L},
                {2, "2022-08-29", 30.48, 32.75, 30.38, 31.55, 4292700L}, {2, "2022-08-30", 31.62, 31.87, 29.42, 29.84, 5060200L},
                {4, "2022-08-15", 291.00, 294.18, 290.11, 293.47, 18085700L}, {4, "2022-08-16", 291.99, 294.04, 290.42, 292.71, 18102900L},
                {4, "2022-08-17", 289.74, 293.35, 289.47, 291.32, 18253400L}, {4, "2022-08-18", 290.19, 291.91, 289.08, 290.17, 17186200L},
                {4, "2022-08-19", 288.90, 289.25, 285.56, 286.15, 20557200L}, {4, "2022-08-22", 282.08, 282.46, 277.22, 277.75, 25061100L},
                {4, "2022-08-23", 276.44, 278.86, 275.40, 276.44, 17527400L}, {4, "2022-08-24", 275.41, 277.23, 275.11, 275.79, 18137000L},
                {4, "2022-08-25", 277.33, 279.02, 274.52, 278.85, 16583400L}, {4, "2022-08-26", 279.08, 280.34, 267.98, 268.09, 27532500L},
                {4, "2022-08-29", 265.85, 267.40, 263.85, 265.23, 20338500L}, {4, "2022-08-30", 266.67, 267.05, 260.66, 262.97, 22767100L}
        };

        String insertStockPriceSQL = "INSERT INTO stockprice (companyId, priceDate, openPrice, highPrice, lowPrice, closePrice, volume) VALUES (?, ?, ?, ?, ?, ?, ?)";
        try (PreparedStatement stmt = dbConnection.prepareStatement(insertStockPriceSQL)) {
            for (Object[] row : stockData) {
                stmt.setInt(1, (Integer) row[0]);
                stmt.setDate(2, Date.valueOf((String) row[1]));
                stmt.setDouble(3, (Double) row[2]);
                stmt.setDouble(4, (Double) row[3]);
                stmt.setDouble(5, (Double) row[4]);
                stmt.setDouble(6, (Double) row[5]);
                stmt.setLong(7, (Long) row[6]);
                stmt.addBatch();
            }
            stmt.executeBatch();
            System.out.println("  -> " + stockData.length + " records inserted into 'stockprice'.");
        }
    }
    
    /**
     * NEW METHOD: Selects and displays all data from a given table.
     * @param tableName The name of the table to display.
     */
    private void showFullTableData(String tableName) throws SQLException {
        // Basic protection against SQL injection, though not a risk here with hardcoded calls.
        if (!tableName.matches("[a-zA-Z0-9_]+")) {
            System.out.println("Invalid table name.");
            return;
        }
        
        String sql = "SELECT * FROM " + tableName;
        try (Statement stmt = dbConnection.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            displayResultSet(rs);
        }
    }

    /**
     * Deletes records from 'stockprice' according to the assignment criteria.
     */
    private void deleteSpecifiedRecords() throws SQLException {
        System.out.println("Deleting records based on assignment criteria...");
        String sql = "DELETE FROM stockprice WHERE priceDate < '2022-08-20' OR companyId = 2";
        try (Statement stmt = dbConnection.createStatement()) {
            int rowsAffected = stmt.executeUpdate(sql);
            System.out.println("--> " + rowsAffected + " records deleted.");
        }
    }

    private ResultSet executeQueryOne() throws SQLException {
        String sql = "SELECT name, annualRevenue, numEmployees "
                + "FROM company "
                + "WHERE numEmployees > 10000 OR annualRevenue < 1000000 "
                + "ORDER BY name ASC";
        return dbConnection.createStatement().executeQuery(sql);
    }

    private ResultSet executeQueryTwo() throws SQLException {
        String sql = "SELECT c.name, c.ticker, MIN(s.lowPrice) AS lowestPrice, "
                + "MAX(s.highPrice) AS highestPrice, AVG(s.closePrice) AS avgClosePrice, "
                + "AVG(s.volume) AS avgVolume "
                + "FROM company c JOIN stockprice s ON c.id = s.companyId "
                + "WHERE s.priceDate BETWEEN '2022-08-22' AND '2022-08-26' "
                + "GROUP BY c.id, c.name, c.ticker "
                + "ORDER BY avgVolume DESC";
        return dbConnection.createStatement().executeQuery(sql);
    }

    private ResultSet executeQueryThree() throws SQLException {
        String sql = "SELECT c.name, c.ticker, s30.closePrice AS closingPrice_Aug30 "
                + "FROM company c "
                + "LEFT JOIN stockprice s30 ON c.id = s30.companyId AND s30.priceDate = '2022-08-30' "
                + "LEFT JOIN (SELECT companyId, AVG(closePrice) AS avgClose FROM stockprice WHERE priceDate BETWEEN '2022-08-15' AND '2022-08-19' GROUP BY companyId) AS avgWeek ON c.id = avgWeek.companyId "
                + "WHERE c.ticker IS NULL OR (s30.closePrice IS NOT NULL AND avgWeek.avgClose IS NOT NULL AND s30.closePrice >= (avgWeek.avgClose * 0.9))";
        return dbConnection.createStatement().executeQuery(sql);
    }

    /**
     * Closes the active database connection.
     */
    private void closeConnection() throws SQLException {
        if (dbConnection != null && !dbConnection.isClosed()) {
            dbConnection.close();
            System.out.println("\nDatabase connection closed.");
        }
    }
    
    /**
     * A new, unique method to format and display a ResultSet as a visually appealing text-based table.
     * This method pre-processes the data to calculate column widths for perfect alignment.
     */
    public static void displayResultSet(ResultSet rs) throws SQLException {
        if (rs == null) {
            System.out.println("Query did not return a result set.");
            return;
        }

        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        List<String[]> tableData = new ArrayList<>();
        String[] header = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            header[i] = metaData.getColumnName(i + 1);
        }
        tableData.add(header);

        while (rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                Object value = rs.getObject(i + 1);
                row[i] = (value == null) ? "NULL" : value.toString();
            }
            tableData.add(row);
        }

        int[] columnWidths = new int[columnCount];
        for (String[] row : tableData) {
            for (int i = 0; i < columnCount; i++) {
                if (row[i].length() > columnWidths[i]) {
                    columnWidths[i] = row[i].length();
                }
            }
        }

        StringBuilder tableBuilder = new StringBuilder();
        
        printTableBorder(tableBuilder, columnWidths);

        printTableRow(tableBuilder, tableData.get(0), columnWidths);
        
        printTableBorder(tableBuilder, columnWidths);

        
        int rowCount = 0;
        for (int i = 1; i < tableData.size(); i++) {
            printTableRow(tableBuilder, tableData.get(i), columnWidths);
            rowCount++;
        }
        
        if (rowCount == 0) {
            System.out.println("| " + String.format("%-" + (sum(columnWidths) + 3 * (columnCount-1) -1) + "s", "Query returned no results.") + " |");
        }

        printTableBorder(tableBuilder, columnWidths);
        
        System.out.print(tableBuilder.toString());
        if (rowCount > 0){
             System.out.println("Total results: " + rowCount);
        }
    }

    /**
     * Helper method to build a row of the text table.
     */
    private static void printTableRow(StringBuilder builder, String[] row, int[] widths) {
        builder.append("|");
        for (int i = 0; i < row.length; i++) {
            builder.append(" ").append(String.format("%-" + widths[i] + "s", row[i])).append(" |");
        }
        builder.append("\n");
    }

    /**
     * Helper method to build the top, middle, or bottom border of the text table.
     */
    private static void printTableBorder(StringBuilder builder, int[] widths) {
        builder.append("+");
        for (int width : widths) {
            builder.append("-".repeat(width + 2)).append("+");
        }
        builder.append("\n");
    }
    
    /**
     * Helper method to sum an array of integers.
     */
     private static int sum(int[] array){
         int sum = 0;
         for(int value : array){
             sum += value;
         }
         return sum;
     }
}