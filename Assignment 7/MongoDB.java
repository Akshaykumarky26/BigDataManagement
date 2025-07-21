import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map; 
import java.util.HashMap; 
import static com.mongodb.client.model.Projections.fields;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Accumulators.first;
import static com.mongodb.client.model.Accumulators.first;

import java.io.BufferedReader;
// ... rest of your imports

import java.io.BufferedReader;
// ... rest of your imports

/**
 * Program to create a collection, insert JSON objects, and perform simple
 * queries on MongoDB.
 */
public class MongoDB {

    /**
     * MongoDB database name
     */
    public static final String DATABASE_NAME = "mydb";
    
    // Define specific collection names
    public static final String CUSTOMER_COLLECTION = "customer";
    public static final String ORDERS_COLLECTION = "orders";
    public static final String CUSTORDERS_COLLECTION = "custorders"; // For nested data
    
    // Define data file paths
    public static final String CUSTOMER_FILE = "data/customer.tbl";
    public static final String ORDER_FILE = "data/order.tbl";

    /**
     * Mongo client connection to server
     */
    public MongoClient mongoClient;
    /**
     * Mongo database
     */
    public MongoDatabase db;

    /**
     * Main method
     *
     * @param args
     * no arguments required
     */
    public static void main(String[] args) throws Exception {
        MongoDB qmongo = new MongoDB();
        qmongo.connect();
        System.out.println("? MongoDB connected to " + qmongo.DATABASE_NAME);
        
        qmongo.load();
        qmongo.loadNest();

        // Query calls (currently unimplemented, will print null/0)
        System.out.println(qmongo.query1(1000));
        System.out.println(qmongo.query2(32));
        System.out.println(qmongo.query2Nest(32));
        System.out.println(qmongo.query3());
        System.out.println(qmongo.query3Nest());
        System.out.println(MongoDB.toString(qmongo.query4()));
        System.out.println(MongoDB.toString(qmongo.query4Nest()));
    }

    /**
     * Connects to Mongo database and returns database object to manipulate for
     * connection.
     *
     * @return Mongo database
     */
    public MongoDatabase connect() {
        try {
            // Provide connection information to MongoDB server
            // IMPORTANT: Replace with your actual working password
            String url = "mongodb+srv://g24ai1033:MyPass123@cosc516.tr9u09h.mongodb.net/?retryWrites=true&w=majority&appName=COSC516";
            mongoClient = MongoClients.create(url);
        } catch (Exception ex) {
            System.out.println("Exception: " + ex);
            ex.printStackTrace();
        }
        // Provide database information to connect to
        // Note: If the database does not already exist, it will be created automatically.
        db = mongoClient.getDatabase(DATABASE_NAME);
        return db;
    }

    /**
     * Loads TPC-H data into MongoDB.
     * Loads customer and orders data into separate collections (customer and orders).
     *
     * @throws Exception if a file I/O or database error occurs
     */
    public void load() throws Exception {
        System.out.println("\nLoading customer data into '" + CUSTOMER_COLLECTION + "' collection...");
        MongoCollection<Document> customerCol = db.getCollection(CUSTOMER_COLLECTION);
        customerCol.drop(); // Clear existing data for a clean load
        
        List<Document> customerDocuments = new ArrayList<>();
        File customerFile = new File(CUSTOMER_FILE);

        try (BufferedReader br = new BufferedReader(new FileReader(customerFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                String[] fields = line.split("\\|"); // Split by pipe
                
                // TPC-H customer table has 8 fields + a potential trailing empty field
                if (fields.length < 8) { 
                    System.err.println("Skipping malformed customer line (too few fields): " + line);
                    continue;
                }

                Document customerDoc = new Document();
                // TPC-H Customer Schema:
                // C_CUSTKEY (INTEGER), C_NAME (VARCHAR), C_ADDRESS (VARCHAR),
                // C_NATIONKEY (INTEGER), C_PHONE (VARCHAR), C_ACCTBAL (DECIMAL),
                // C_MKTSEGMENT (VARCHAR), C_COMMENT (VARCHAR)

                customerDoc.append("C_CUSTKEY", Integer.parseInt(fields[0]));
                customerDoc.append("C_NAME", fields[1]);
                customerDoc.append("C_ADDRESS", fields[2]);
                customerDoc.append("C_NATIONKEY", Integer.parseInt(fields[3]));
                customerDoc.append("C_PHONE", fields[4]);
                customerDoc.append("C_ACCTBAL", new BigDecimal(fields[5]));
                customerDoc.append("C_MKTSEGMENT", fields[6]);
                customerDoc.append("C_COMMENT", fields[7]); 
                
                customerDocuments.add(customerDoc);
            }
        } catch (java.io.FileNotFoundException e) {
            System.err.println("ERROR: Customer file not found at " + customerFile.getAbsolutePath());
            e.printStackTrace();
            throw e;
        } catch (java.io.IOException e) {
            System.err.println("ERROR: Error reading customer file: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
        
        if (!customerDocuments.isEmpty()) {
            customerCol.insertMany(customerDocuments);
            System.out.println("Inserted " + customerDocuments.size() + " customer documents.");
        } else {
            System.out.println("No customer documents to insert.");
        }

        // --- START OF ORDERS LOADING LOGIC ---
        System.out.println("\nLoading orders data into '" + ORDERS_COLLECTION + "' collection...");
        MongoCollection<Document> orderCol = db.getCollection(ORDERS_COLLECTION);
        orderCol.drop(); // Clear existing data for a clean load

        List<Document> orderDocuments = new ArrayList<>();
        File orderFile = new File(ORDER_FILE);

        try (BufferedReader br = new BufferedReader(new FileReader(orderFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                String[] fields = line.split("\\|"); // Split by pipe

                // TPC-H order table has 9 fields + a potential trailing empty field
                if (fields.length < 9) {
                    System.err.println("Skipping malformed order line (too few fields): " + line);
                    continue;
                }

                Document orderDoc = new Document();
                // O_ORDERKEY (INTEGER), O_CUSTKEY (INTEGER), O_ORDERSTATUS (CHAR),
                // O_TOTALPRICE (DECIMAL), O_ORDERDATE (DATE), O_ORDERPRIORITY (CHAR),
                // O_CLERK (VARCHAR), O_SHIPPRIORITY (INTEGER), O_COMMENT (VARCHAR)
                
                orderDoc.append("O_ORDERKEY", Integer.parseInt(fields[0]));
                orderDoc.append("O_CUSTKEY", Integer.parseInt(fields[1]));
                orderDoc.append("O_ORDERSTATUS", fields[2]);
                orderDoc.append("O_TOTALPRICE", new BigDecimal(fields[3]));
                orderDoc.append("O_ORDERDATE", fields[4]); 
                orderDoc.append("O_ORDERPRIORITY", fields[5]);
                orderDoc.append("O_CLERK", fields[6]);
                orderDoc.append("O_SHIPPRIORITY", Integer.parseInt(fields[7]));
                orderDoc.append("O_COMMENT", fields[8]);

                orderDocuments.add(orderDoc);
            }
        } catch (java.io.FileNotFoundException e) {
            System.err.println("ERROR: Order file not found at " + orderFile.getAbsolutePath());
            e.printStackTrace();
            throw e;
        } catch (java.io.IOException e) {
            System.err.println("ERROR: Error reading order file: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        if (!orderDocuments.isEmpty()) {
            orderCol.insertMany(orderDocuments);
            System.out.println("Inserted " + orderDocuments.size() + " order documents.");
        } else {
            System.out.println("No order documents to insert.");
        }
        // --- END OF ORDERS LOADING LOGIC ---
    }

    /**
     * Loads customer and orders TPC-H data into a single collection.
     *
     * @throws Exception if a file I/O or database error occurs
     */
    public void loadNest() throws Exception {
        System.out.println("\nLoading customer and orders data into '" + CUSTORDERS_COLLECTION + "' collection...");
        MongoCollection<Document> custordersCol = db.getCollection(CUSTORDERS_COLLECTION);
        custordersCol.drop(); // Clear existing data for a clean load

        // Step 1: Get all customer documents
        MongoCollection<Document> customerCol = db.getCollection(CUSTOMER_COLLECTION);
        List<Document> customers = customerCol.find().into(new ArrayList<>());
        System.out.println("Retrieved " + customers.size() + " customer documents for nesting.");

        // Step 2: Get all order documents
        MongoCollection<Document> orderCol = db.getCollection(ORDERS_COLLECTION);
        List<Document> orders = orderCol.find().into(new ArrayList<>());
        System.out.println("Retrieved " + orders.size() + " order documents for nesting.");
        
        // --- START OF NESTING LOGIC ---
        // Step 3: Group orders by customer key
        // A Map to store customer ID as key and a list of their orders as value
        java.util.Map<Integer, List<Document>> ordersByCustomer = new java.util.HashMap<>();
        for (Document order : orders) {
            Integer customerKey = order.getInteger("O_CUSTKEY");
            if (customerKey != null) {
                ordersByCustomer.computeIfAbsent(customerKey, k -> new ArrayList<>()).add(order);
            }
        }
        System.out.println("Grouped " + ordersByCustomer.size() + " unique customers with orders.");


        // Step 4: Create nested documents
        List<Document> nestedCustOrderDocuments = new ArrayList<>();
        for (Document customer : customers) {
            Integer customerKey = customer.getInteger("C_CUSTKEY");
            // Create a new document that will contain customer info and their orders
            Document nestedDoc = new Document();
            
            // Copy all fields from the original customer document to the new nested document
            for (String key : customer.keySet()) {
                // Exclude the MongoDB "_id" field if you want a cleaner copy, or keep it if desired
                if (!key.equals("_id")) {
                    nestedDoc.append(key, customer.get(key));
                }
            }
            
            // Add the list of orders as a nested array
            List<Document> customerOrders = ordersByCustomer.getOrDefault(customerKey, new ArrayList<>());
            nestedDoc.append("ORDERS", customerOrders); // Naming the nested array "ORDERS"

            nestedCustOrderDocuments.add(nestedDoc);
        }
        System.out.println("Created " + nestedCustOrderDocuments.size() + " nested customer-order documents.");

        // Step 5: Insert the new nested documents into the custorders collection
        if (!nestedCustOrderDocuments.isEmpty()) {
            custordersCol.insertMany(nestedCustOrderDocuments);
            System.out.println("Inserted " + nestedCustOrderDocuments.size() + " nested customer-order documents into '" + CUSTORDERS_COLLECTION + "' collection.");
        } else {
            System.out.println("No nested customer-order documents to insert.");
        }
        // --- END OF NESTING LOGIC ---
    }
    /**
     * Performs a MongoDB query that prints out all data (except for the_id).
     * Returns the customer name given a customer id using the customer collection.
     */
    public String query1 (int custkey) {
        System.out.println("\nExecuting query 1: Get customer name for C_CUSTKEY = " + custkey);
        MongoCollection<Document> col = db.getCollection(CUSTOMER_COLLECTION);
        
        // Build the query: find a document where C_CUSTKEY equals custkey
        // Project: include only C_NAME, exclude _id
        Document result = col.find(eq("C_CUSTKEY", custkey))
                             .projection(fields(include("C_NAME"), excludeId()))
                             .first(); // Get the first matching document

        if (result != null) {
            return result.toJson(); // Return the document as JSON string
        } else {
            return "Customer with C_CUSTKEY " + custkey + " not found.";
        }
    }

    /**
     * Performs a MongoDB query that returns order date for a given order id using
     * the orders collection.
     */
    public String query2(int orderId) {
        System.out.println("\nExecuting query 2: Get O_ORDERDATE for O_ORDERKEY = " + orderId);
        MongoCollection<Document> col = db.getCollection(ORDERS_COLLECTION); // Use ORDERS_COLLECTION

        // Find the document where O_ORDERKEY equals orderId
        // Project to include only O_ORDERDATE and exclude _id
        Document result = col.find(eq("O_ORDERKEY", orderId))
                             .projection(fields(include("O_ORDERDATE"), excludeId()))
                             .first(); // Get the first matching document

        if (result != null) {
            return result.toJson(); // Return the document as JSON string
        } else {
            return "Order with O_ORDERKEY " + orderId + " not found.";
        }
    }


    /**
     * Performs a MongoDB query that returns order date for a given order id using
     * the custorders collection.
     */
    public String query2Nest(int orderId) {
        System.out.println("\nExecuting query 2 nested: Get O_ORDERDATE for O_ORDERKEY = " + orderId + " from nested collection.");
        MongoCollection<Document> col = db.getCollection(CUSTORDERS_COLLECTION);

        // Find a document where any element in the 'ORDERS' array has O_ORDERKEY matching orderId
        // Project to include only the matched order's O_ORDERDATE and exclude _id
        
        // Using $unwind to deconstruct the ORDERS array, then match, then project
        // This is a common pattern for querying nested arrays
        List<Bson> pipeline = Arrays.asList(
            match(eq("ORDERS.O_ORDERKEY", orderId)), // Match documents where the nested array contains the order
            unwind("$ORDERS"),                       // Deconstruct the ORDERS array
            match(eq("ORDERS.O_ORDERKEY", orderId)), // Match the specific order after unwind
            project(fields(include("ORDERS.O_ORDERDATE"), excludeId())) // Project only the order date
        );

        AggregateIterable<Document> result = col.aggregate(pipeline);
        MongoCursor<Document> cursor = result.iterator();

        if (cursor.hasNext()) {
            Document doc = cursor.next();
            cursor.close();
            // The result will be something like {"ORDERS.O_ORDERDATE": "1995-07-16"}
            // We might want to reformat it to just the date string if desired
            return doc.toJson();
        } else {
            return "Order with O_ORDERKEY " + orderId + " not found in nested collection.";
        }
    }

    /**
     * Performs a MongoDB query that returns the total number of orders using the
     * orders collection.
     */

    public long query3() {
        System.out.println("\nExecuting query 3: Get total number of orders from orders collection.");
        MongoCollection<Document> col = db.getCollection(ORDERS_COLLECTION);
        
        long count = col.countDocuments(); // Use countDocuments() to get the total number of documents
        return count;
    }

/**
     * Performs a MongoDB query that returns the total number of orders using the
     * custorders collection.
     */
    public long query3Nest() {
        System.out.println("\nExecuting query 3 nested: Get total number of orders from custorders collection.");
        MongoCollection<Document> col = db.getCollection(CUSTORDERS_COLLECTION);
        
        // Aggregation pipeline to count total nested orders
        List<Bson> pipeline = Arrays.asList(
            // Stage 1: Unwind the 'ORDERS' array to get one document per order
            unwind("$ORDERS"),
            // Stage 2: Group all documents and count them using Accumulators.sum
            group(new Document("_id", null), sum("totalOrders", 1))
        );

        AggregateIterable<Document> result = col.aggregate(pipeline);
        MongoCursor<Document> cursor = result.iterator();

        long totalOrders = 0;
        if (cursor.hasNext()) {
            Document doc = cursor.next();
            // CORRECTED LINE: Safely get as Integer and convert to long
            totalOrders = doc.getInteger("totalOrders").longValue(); 
        }
        cursor.close();
        
        return totalOrders;
    }

    /**
     * Performs a MongoDB query that returns the top 5 customers based on total
     * order amount using the customer and orders collections.
     * Returns an iterator of Documents.
     */
    /**
     * Performs a MongoDB query that returns the top 5 customers based on total
     * order amount using the customer and orders collections.
     * Returns an iterator of Documents.
     */
    public MongoCursor<Document> query4() {
        System.out.println("\nExecuting query 4: Get top 5 customers based on total order amount (customer & orders collections).");
        MongoCollection<Document> customerCol = db.getCollection(CUSTOMER_COLLECTION);

        List<Bson> pipeline = Arrays.asList(
            // Stage 1: Join 'customer' collection with 'orders' collection
            // Perform a left outer join to the orders collection using 'C_CUSTKEY' and 'O_CUSTKEY'
            lookup(ORDERS_COLLECTION, "C_CUSTKEY", "O_CUSTKEY", "customerOrders"),

            // Stage 2: Unwind the 'customerOrders' array
            // This creates a separate document for each order associated with the customer
            unwind("$customerOrders"),

            // Stage 3: Group by customer and calculate the sum of O_TOTALPRICE
            group("$C_CUSTKEY",
                  sum("totalOrderAmount", "$customerOrders.O_TOTALPRICE"), // Sum the total price from each order
                  first("C_NAME", "$C_NAME"), // Keep the customer name
                  first("C_ACCTBAL", "$C_ACCTBAL") // Keep the account balance
            ),

            // Stage 4: Sort by totalOrderAmount in descending order
            // CORRECTED LINE: Use Sorts.descending explicitly
            sort(com.mongodb.client.model.Sorts.descending("totalOrderAmount")),

            // Stage 5: Limit to the top 5 results
            limit(5),

            // Stage 6: Project the desired fields
            project(fields(
                include("C_NAME", "totalOrderAmount"), // Include customer name and calculated total amount
                excludeId() // Exclude the default _id
            ))
        );

        AggregateIterable<Document> result = customerCol.aggregate(pipeline);
        return result.iterator();
    }
    /**
     * Performs a MongoDB query that returns the top 5 customers based on total
     * order amount using the custorders collection.
     * Returns an iterator of Documents.
     */
    public MongoCursor<Document> query4Nest() {
        System.out.println("\nExecuting query 4 nested: Get top 5 customers based on total order amount (custorders collection).");
        MongoCollection<Document> col = db.getCollection(CUSTORDERS_COLLECTION);

        List<Bson> pipeline = Arrays.asList(
            // Stage 1: Unwind the 'ORDERS' array within each customer document
            unwind("$ORDERS"),

            // Stage 2: Group by customer and calculate the sum of O_TOTALPRICE from the unwound orders
            group("$C_CUSTKEY", // Group by the customer key
                  sum("totalOrderAmount", "$ORDERS.O_TOTALPRICE"), // Sum the total price from each nested order
                  first("C_NAME", "$C_NAME") // Keep the customer name from the original document
            ),

            // Stage 3: Sort by totalOrderAmount in descending order
            sort(com.mongodb.client.model.Sorts.descending("totalOrderAmount")),

            // Stage 4: Limit to the top 5 results
            limit(5),

            // Stage 5: Project the desired fields
            project(fields(
                include("C_NAME", "totalOrderAmount"), // Include customer name and calculated total amount
                excludeId() // Exclude the default _id
            ))
        );

        AggregateIterable<Document> result = col.aggregate(pipeline);
        return result.iterator();
    }
    /**
     * Returns the Mongo database being used.
     *
     * @return Mongo database
     */
    public MongoDatabase getDb() {
        return db;
    }

    /**
     * Outputs a cursor of MongoDB results in string form.
     *
     * @param cursor Mongo cursor
     * @return results as a string
     */
    public static String toString(MongoCursor<Document> cursor) {
        StringBuilder buf = new StringBuilder();
        int count = 0;
        buf.append("Rows:\n");
        if (cursor != null) {
            while (cursor.hasNext()) {
                Document obj = cursor.next();
                buf.append(obj.toJson());
                buf.append("\n");
                count++;
            }
            cursor.close();
        }
        buf.append("Number of rows: " + count);
        return buf.toString();
    }
}