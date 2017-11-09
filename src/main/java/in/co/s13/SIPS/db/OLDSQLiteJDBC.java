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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.sql.*;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class OLDSQLiteJDBC {

    private Connection connection = null;
    private Statement statement = null;
    private ResultSet resultSet = null;
    private String databaseLocation;

    /**
     * OLDSQLiteJDBC class supports simple functions on single database file
     * <b>Note:</b> No need to close connection
     *
     * @param dbloc specifies the location of database file
     */
    public OLDSQLiteJDBC(String dbloc) {
        databaseLocation = dbloc;
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + databaseLocation);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SQLException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * *
     * Closes connection will remove file lock on database file If you perform
     * this operation and want to perform any other operation, you have to open
     * connection to database file again.
     */
    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Close the statement and free up resource
     */
    public void closeStatement() {
        try {
            statement.close();
        } catch (SQLException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * *
     * Helps in performing create table statement
     *
     * @param sql : SQL statement to create table
     */
    public void createtable(String sql) {
        try {
            //System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            // statement.close();
            // connection.close();
            System.out.println(sql);
            System.out.println("Table created successfully on DB " + databaseLocation);

        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());

        }

    }

    /**
     * *
     * Helps to execute INSERT SQL statement on database file
     *
     * @param sql : SQL statement to perform insert operation
     */
    public void insert(String sql) {
        try {
            connection.setAutoCommit(false);
            //  System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);

            //    statement.close();
            connection.commit();
            //    connection.close();
            System.out.println(sql);
            System.out.println("Records created successfully on DB " + databaseLocation);

        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());

        }
    }

    /**
     * **
     * Helps performing select operation
     *
     * @param sql : SQL select query to be performed on database file
     * @return contains the rows of requested data
     * @throws SQLException
     */
    public ResultSet select(String sql) throws SQLException {

        ResultSet rs2 = null;
        try {
            connection.setAutoCommit(false);
            //  System.out.println("Opened database successfully");

            statement = connection.createStatement();
            rs2 = statement.executeQuery(sql);
            System.out.println(sql);
            System.out.println("Select Operation done successfully on DB " + databaseLocation);
        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());
            System.out.println("Select Operation was not done successfully on DB " + databaseLocation);
            return null;
        }

        return rs2;

    }

    /**
     * Helps in performing update statement on database file
     *
     * @param sql: SQL statement to update data in database file
     */
    public void Update(String sql) {
        try {
            connection.setAutoCommit(false);

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();

            //      statement.close();
            //    connection.close();
            System.out.println(sql);
            System.out.println("Update Operation done successfully on DB " + databaseLocation);
        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());

        }

    }

    /**
     * Updates the Java Object in the database file
     *
     * @param sql: SQL query to update Java Object
     * @param obj: Value to inserted
     */
    public void UpdateObj(String sql, Object obj) {
        try {
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

            System.out.println(sql);
            System.out.println("Update Operation done successfully on DB " + databaseLocation);
        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());

        } catch (IOException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    /**
     * Helps retriving the Java Object from database file
     *
     * @param sql : SQL Select operation
     * @return Java Object
     */
    public Object getObject(String sql) {
        Object rmObj = null;
        try {
            connection.setAutoCommit(false);
            PreparedStatement ps = null;
            ResultSet rs = null;
            //String sql=null;

            //sql="select * from javaobject where id=1";
            ps = connection.prepareStatement(sql);

            rs = ps.executeQuery();

            if (rs.next()) {
                ByteArrayInputStream bais;

                ObjectInputStream ins;

                try {

                    bais = new ByteArrayInputStream(rs.getBytes("VALUE"));

                    ins = new ObjectInputStream(bais);

                    ArrayList mc = (ArrayList) ins.readObject();

                    System.out.println("Object in value ::" + mc);
                    ins.close();

                    rmObj = mc;
                } catch (Exception e) {

                    e.printStackTrace();
                }

            }

            return rmObj;
        } catch (SQLException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }
        return rmObj;
    }

    /**
     * *
     * Helps in performing delete operation on database file
     *
     * @param sql : SQL DELETE query to be performed on database file
     */
    public void delete(String sql) {
        try {
            connection.setAutoCommit(false);
            // System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();

            // statement.close();
            // connection.close();
            System.out.println(sql);
            System.out.println("Delete Operation done successfully on DB " + databaseLocation);
        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());

        }
    }

    /**
     * *
     * Helps in executing SQL operation
     *
     * @param sql : SQL operation
     */
    public void execute(String sql) {
        try {
//            Class.forName("org.sqlite.JDBC");
//            connection = DriverManager.getConnection("jdbc:sqlite:" + databaseLocation);
            connection.setAutoCommit(false);
            // System.out.println("Opened database successfully");

            statement = connection.createStatement();
            statement.executeUpdate(sql);
            connection.commit();

            // statement.close();
            // connection.close();
            System.out.println(sql);
            System.out.println("Query Executed Operation done successfully on DB " + databaseLocation);
        } catch (SQLException e) {
            System.out.println(e.getClass().getName() + ": " + e.getMessage());

        }

    }

    /**
     * Helps in saving database file to text file, where rows are separated by
     * new lines and columns by tabs
     *
     * @param sql: SQL Select query
     * @param file: to save the results of select operation
     */
    public void toFile(String sql, String file) {
        try {
            ResultSet result = this.select(sql);
            ResultSetMetaData rsm = result.getMetaData();
            int columncount = rsm.getColumnCount();
            try (PrintStream out = new PrintStream(file)) {
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
            }
        } catch (SQLException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(OLDSQLiteJDBC.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
