package com.jessespalding;

import java.sql.*;
import java.util.Scanner;

public class Main {
    private static String driver = "org.apache.derby.jdbc.EmbeddedDriver";
    private static String protocol = "jdbc:derby:";
    private static String dbName = "hivesDB";

    private static final String USER = "username";
    private static final String PASS = "password";

    public static void main(String[] args) {
        Statement statement = null;
        Connection conn = null;
        ResultSet rs = null;
        PreparedStatement psInsert = null;

        Scanner scanner = new Scanner(System.in);

        int hiveId = 1;

        try {
            Class.forName(driver);
            conn = DriverManager.getConnection(protocol + dbName + ";create=true", USER, PASS);
            statement = conn.createStatement();

            try {
                String createTableSQL = "CREATE TABLE Hives (HiveID int, Honey int, HoneyDate date)";
                statement.executeUpdate(createTableSQL);
                System.out.println("Created hives table");

            } catch (SQLException e) {
                System.out.println("Hives database already exists");
            }

            while (true) {
                System.out.println("Do you wish to add a harvest year?");
                String harvestInput = scanner.next();
                if (harvestInput.equalsIgnoreCase("n")) {
                    break;
                } else {
                    String prepareInsert = "INSERT INTO Hives VALUES ( ? , ? , ?)";
                    psInsert = conn.prepareStatement(prepareInsert);

                    System.out.println("Please enter the year of harvest (yyyy):");
                    int harvestYear = scanner.nextInt();

                    for (int i = 1; i < 4; i++) {
                        psInsert.setInt(1, i);
                        System.out.println("Hive " + i + ":\nPlease enter number of pounds harvested:");
                        int honeyInput = scanner.nextInt();
                        psInsert.setInt(2, honeyInput);
                        System.out.println("Please enter harvest month and day (mm-dd):");
                        String harvestMonthDay = scanner.next();
                        String dateInput = harvestYear + "-" + harvestMonthDay;
                        Date dateValue = Date.valueOf(dateInput);
                        psInsert.setDate(3, dateValue);
                        psInsert.executeUpdate();
                    }
                    System.out.println("Data added to table\n");
                }
            }

//            AND sales.Date BETWEEN '" + sdate + "' AND '" + edate + "' "

            System.out.println("Hives in the table:");
            String fetchHives = "SELECT * FROM Hives";
            rs = statement.executeQuery(fetchHives);

            while (rs.next()) {
                String hivesId = rs.getString("HiveID");
                String honey = rs.getString("Honey");
                String honeyDate = rs.getString("HoneyDate");
                System.out.println("Hive ID: " + hivesId + "\nHoney(lbs): " + honey + "\nHarvest Date: " + honeyDate + "\n");
            }

            System.out.println("Enter a year to view total honey harvested:");
            Integer yearQuery = scanner.nextInt();
            String yearString = yearQuery.toString();
            String yearStringBegin = yearString + "-01-01";
            String yearStringEnd = yearString + "-12-31";
            String fetchYearly = "SELECT * FROM Hives " +
                    "WHERE HoneyDate BETWEEN '" + yearStringBegin + "' AND '" + yearStringEnd + "' ";
            rs = statement.executeQuery(fetchYearly);
            int yearlyHoney = 0;
            while (rs.next()) {
                int honey = rs.getInt("Honey");
                yearlyHoney = yearlyHoney + honey;
            }
            System.out.println("Total honey harvested in " + yearQuery + ": " + yearlyHoney);

            System.out.println("Enter a Hive ID for hive stats:");
            Integer hiveQuery = scanner.nextInt();
            String hiveString = hiveQuery.toString();
            String fetchHiveId = "SELECT * FROM Hives " +
                    "WHERE HiveID =" + hiveQuery;
            rs = statement.executeQuery(fetchHiveId);
            int hiveHoney = 0;
            int bestHoney = 0;
            String bestYear = "";
            while (rs.next()) {
                int honey = rs.getInt("Honey");
                hiveHoney = hiveHoney + honey;
                if (honey > bestHoney) {
                    bestYear = rs.getString("HoneyDate");
                    bestHoney = honey;
                }
            }
            System.out.println("Total honey harvested by Hive " + hiveString + ": " + hiveHoney);
            System.out.println("Best harvest: " + bestYear + " with " + bestHoney + " lbs harvested");


        } catch (SQLException se) {
            se.printStackTrace();
            System.out.println("SQL Exception error");
        } catch (ClassNotFoundException cnf) {
            cnf.printStackTrace();
            System.out.println("Class was not found");
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            try {
                if (rs != null) {
                    rs.close();
                    System.out.println("Result set is closed");
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

            try {
                if (statement != null) {
                    statement.close();
                    System.out.println("Statement closed");
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

            try {
                if (psInsert != null) {
                    psInsert.close();
                    System.out.println("Prepared statement closed");
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

            try {
                // If connection is null and finished, gives message
                if (conn != null) {
                    conn.close();
                    System.out.println("Database connection is closed");
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }

        }
        System.out.println("Program done");
    }
}
