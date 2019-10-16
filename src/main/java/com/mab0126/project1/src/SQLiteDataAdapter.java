package com.mab0126.project1.src;
import com.mab0126.project1.src.IDataAdapter;
import com.mab0126.project1.src.ProductModel;
import com.mab0126.project1.src.PurchaseModel;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

import java.sql.DriverManager;
import java.sql.SQLException;

public class SQLiteDataAdapter implements IDataAdapter {

    Connection conn = null;

    public int connect(String dbfile) {
        try {
            // db parameters
            String url = "jdbc:sqlite:" + dbfile;
            // create a connection to the database
            conn = DriverManager.getConnection(url);
            initialize(conn);
            System.out.println("Connection to SQLite has been established.");

        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return CONNECTION_OPEN_FAILED;
        }
        return CONNECTION_OPEN_OK;
    }

    public static void initialize(Connection conn) {
        try {
            Statement statement = conn.createStatement();
            //Create db if not exists
            //Create tables (if not exists)
            String createCustomers = "CREATE TABLE IF NOT EXISTS \"Customers\" (\n" +
                    "\"CustomerId\" INTEGER NOT NULL,\n" +
                    "\"name\"TEXT,\n" +
                    "\"address\" TEXT,\n" +
                    "\"phone\" TEXT DEFAULT '334-444-4444',\n" +
                    "PRIMARY KEY(\"CustomerId\")\n" +
                    ");";
            statement.execute(createCustomers);
            String createProducts = "CREATE TABLE IF NOT EXISTS \"Products\" (\n" +
                    "\"ProductId\" INTEGER NOT NULL,\n" +
                    "\"Name\"TEXT,\n" +
                    "\"Price\" NUMERIC,\n" +
                    "\"Quantity\" INTEGER,\n" +
                    "PRIMARY KEY(\"ProductId\")\n" +
                    ");";
            statement.execute(createProducts);
            String createPurchases = "CREATE TABLE IF NOT EXISTS \"Purchases\" (\n" +
                    "\"purchase_id\" INTEGER NOT NULL,\n" +
                    "\"customer_id\" INTEGER NOT NULL,\n" +
                    "\"product_id\" INTEGER NOT NULL,\n" +
                    "\"price\" TEXT NUMERIC,\n" +
                    "\"quantity\" INTEGER,\n" +
                    "\"cost\" NUMERIC,\n" +
                    "\"tax\" NUMERIC, \n" +
                    "\"total\" NUMERIC, \n" +
                    "PRIMARY KEY(\"purchase_id\"),\n" +
                    "FOREIGN KEY(\"customer_id\") REFERENCES Customers(\"CustomerId\"),\n" +
                    "FOREIGN KEY(\"product_id\") REFERENCES Products(\"ProductId\"))";
            statement.execute(createPurchases);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int disconnect() {
        try {
            conn.close();
        } catch (SQLException e) {
            System.out.println(e.getMessage());
            return CONNECTION_CLOSE_FAILED;
        }
        return CONNECTION_CLOSE_OK;
    }

    @Override
    public ProductModel loadProduct(int productID) {
        ProductModel product = null;

        try {
            String sql = "SELECT ProductId, Name, Price, Quantity FROM Products WHERE ProductId = " + productID;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                product = new ProductModel();
                product.mProductID = rs.getInt("ProductId");
                product.mName = rs.getString("Name");
                product.mPrice = rs.getDouble("Price");
                product.mQuantity = rs.getDouble("Quantity");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return product;
    }

    @Override
    public int saveProduct(ProductModel product) {
        try {
            String sql = "INSERT INTO Products(ProductId, Name, Price, Quantity) VALUES " + product;
            System.out.println(sql);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);

        } catch (Exception e) {
            String msg = e.getMessage();
            System.out.println(msg);
            if (msg.contains("UNIQUE constraint failed"))
                return PRODUCT_DUPLICATE_ERROR;
        }

        return PRODUCT_SAVED_OK;
    }

    @Override
    public int savePurchase(PurchaseModel purchase) {
        try {
            String sql = "INSERT INTO Purchases VALUES " + purchase;
            System.out.println(sql);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);

        } catch (Exception e) {
            String msg = e.getMessage();
            System.out.println(msg);
            if (msg.contains("UNIQUE constraint failed"))
                return PURCHASE_DUPLICATE_ERROR;
        }

        return PURCHASE_SAVED_OK;

    }

    @Override
    public CustomerModel loadCustomer(int id) {
        CustomerModel customer = null;

        try {
            String sql = "SELECT * FROM Customers WHERE CustomerId = " + id;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                customer = new CustomerModel();
                customer.mCustomerID = id;
                customer.mName = rs.getString("Name");
                customer.mPhone = rs.getString("Phone");
                customer.mAddress = rs.getString("Address");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return customer;
    }

    @Override
    public int saveCustomer(CustomerModel model) {
        try {
            String sql = "INSERT INTO Customers VALUES" + model;
            System.out.println(sql);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        }
        catch(Exception e) {
            String msg = e.getMessage();
            System.out.println(msg);
            if (msg.contains("UNIQUE constraint failed"))
                return CUSTOMER_DUPLICATE_ERROR;
        }

        return CUSTOMER_SAVED_OK;
    }

    @Override
    public PurchaseModel loadPurchase(int id) {
        PurchaseModel purchase = null;

        try {
            String sql = "SELECT * FROM Purchases WHERE PurchaseId = " + id;
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
                purchase = new PurchaseModel();
                purchase.mPurchaseID = id;
                purchase.mProductID = rs.getInt("ProductID");
                purchase.mCustomerID = rs.getInt("CustomerID");
                purchase.mPrice = rs.getDouble("Price");
                purchase.mQuantity = rs.getDouble("Quantity");
                purchase.mCost = rs.getDouble("Cost");
                purchase.mTax = rs.getDouble("Tax");
                purchase.mTotal = rs.getDouble("Total");
            }

        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return purchase;

    }
}
