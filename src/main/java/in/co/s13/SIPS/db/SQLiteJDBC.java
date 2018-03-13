/* 
 * Copyright (C) 2017 Navdeep Singh Sidhu
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package in.co.s13.SIPS.db;

/**
 *
 * @author Nika
 */
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class SQLiteJDBC {

    /**
     * Database Connection
     */
    private Connection connection = null;
    /**
     * Database statement to hold SQL queries/operations
     */
    private Statement statement = null;

    /**
     * Verbosity of SQL queries/statements/operations and errors. if set to true
     * will display all messages and errors default is false to prevent clutter
     * on screen
     */
    private boolean verbose = false;

    /**
     * Modern implementation of OLDSqliteJDBC, in which you can set database
     * file with every operation
     * <b> Safe practice: always call closeConnection() after every operation to
     * clean resources and unlock DB file</b>
     * Constructor Loads the sqlite JDBC library
     */
    public SQLiteJDBC() {
        try {
            Class.forName("org.sqlite.JDBC");
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Verbosity of SQL queries/statements/operations and errors. if set to true
     * will display all messages and errors default is false to prevent clutter
     * on screen
     *
     * @return Verbosity
     */
    public boolean isVerbose() {
        return verbose;
    }

    /**
     * Set verbosity Verbosity of SQL queries/statements/operations and errors.
     * if set to true will display all messages and errors default is false to
     * prevent clutter on screen
     *
     * @param verbose
     */
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    /**
     * *
     * closes connection and statement for last DB file and performed operation
     * Good practice: perform this operation after every db operation, so that
     * single object can be used to handle multiple db files
     */
    public void closeConnection() {
        try {
            statement.close();
            connection.close();
        } catch (SQLException ex) {
            try {
                connection.close();
            } catch (SQLException ex1) {
                Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex1);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Helps in performing create table statement
     *
     * @param db database location
     * @param sql SQL statement to create table
     */
    public boolean createtable(String db, String sql) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            //System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            // stmt.close();
            // c.close();
            if (isVerbose()) {
                System.out.println(sql);
                System.out.println("Table created successfully on DB " + db);
            }
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.out.println(sql + " did not executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
            return false;
        }
        return true;
    }

    /**
     * Helps in performing insert operation
     *
     * @param db database location
     * @param sql SQL statement to insert data
     */
    public void insert(String db, String sql) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            connection.setAutoCommit(false);
            //  System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);

            //    stmt.close();
            connection.commit();
            //    c.close();
            if (isVerbose()) {
                System.out.println(sql);
                System.out.println("Records created successfully on DB " + db);
            }
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);

        }
    }

    /**
     * Helps in performing select operation
     *
     * @param db database location
     * @param sql SQL statement to select data
     * @return rows in result set
     */
    public ResultSet select(String db, String sql) {

        ResultSet rs2 = null;
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            connection.setAutoCommit(false);
            //  System.out.println("Opened database successfully");

            statement = connection.createStatement();
            rs2 = statement.executeQuery(sql);
            if (isVerbose()) {
                System.out.println(sql);
                System.out.println("Select Operation done successfully on DB " + db);
            }
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);

        }

        return rs2;

    }

    /**
     * Helps in performing update operation
     *
     * @param db database location
     * @param sql SQL statement to update data
     */
    public void update(String db, String sql) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            connection.setAutoCommit(false);

            statement = connection.createStatement();
            int r = statement.executeUpdate(sql);
            connection.commit();

            //      stmt.close();
            //    c.close();
            if (isVerbose()) {
                System.out.println(sql);
                System.out.println(r + " Rows effected Update Operation done successfully on DB " + db);
            }
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.err.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);

        }

    }

    /**
     * Helps in performing update operation
     *
     * @param db database location
     * @param sql SQL statement to update object
     * @param obj new object value
     */
    public void update(String db, String sql, Object obj) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            connection.setAutoCommit(false);
            PreparedStatement ps = null;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);

            oos.writeObject(obj);
            oos.flush();
            oos.close();
            bos.close();

            byte[] data = bos.toByteArray();

//            sql = "insert into javaobject (javaObject) values(?)";
            ps = connection.prepareStatement(sql);
            ps.setObject(1, data);
            ps.executeUpdate();

            connection.commit();

            if (isVerbose()) {
                System.out.println(sql);
                System.out.println("Update Operation done successfully on DB " + db);
            }
        } catch (SQLException e) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, e);

        } catch (IOException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Helps in performing delete operation
     *
     * @param db database location
     * @param sql SQL statement to delete data
     */
    public void delete(String db, String sql) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            connection.setAutoCommit(false);
            // System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();

            // stmt.close();
            // c.close();
            if (isVerbose()) {
                System.out.println(sql);
                System.out.println("Delete Operation done successfully on DB " + db);
            }
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);

        }
    }

    /**
     * Helps in performing SQL operation
     *
     * @param db database location
     * @param sql SQL statement to be executed
     */
    public void execute(String db, String sql) {
        try {
            connection = DriverManager.getConnection("jdbc:sqlite:" + db);
            connection.setAutoCommit(false);
            // System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();

            // stmt.close();
            // c.close();
            if (isVerbose()) {
                System.out.println(sql);
                System.out.println("Query Executed Operation done successfully on DB " + db);
            }
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);

        }

    }

    /**
     * Helps in saving database file to text file, where rows are separated by
     * new lines and columns by tabs
     *
     * @param db database file
     * @param sql SQL Select query
     * @param file to save the results of select operation
     */
    public void toFile(String db, String sql, String file) {
        try {
            ResultSet result = this.select(db, sql);
            ResultSetMetaData rsm = result.getMetaData();
            int columncount = rsm.getColumnCount();
            PrintStream out = new PrintStream(file); //new AppendFileStream
            for (int i = 1; i <= columncount; i++) {
                out.print(rsm.getColumnName(i) + "\t");
            }
            out.print("\n");
            while (result.next()) {
                for (int i = 1; i <= columncount; i++) {
                    out.print(result.getString(i) + "\t");
                }
                out.print("\n");

            }
            out.close();
        } catch (SQLException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            if (isVerbose()) {
                System.out.println(sql + " didnot executed on " + db);
            }
            Logger.getLogger(SQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
