import com.github.javafaker.Faker;
import graph.Graph;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.concurrent.TimeUnit;

public class DataGenerator
{

    static final String JDBC_DRIVER = "org.mariadb.jdbc.Driver";
    static final String DB_URL = "jdbc:mariadb://127.0.0.1/";

    //  Database credentials
    static final String USERNAME = "root";
    static final String PASSWORD = "root";

    public ResultSet executeSQLInsert(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);


        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        return resultSet;
    }


    public ResultSet executeSQLQuery(String sqlQuery) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet resultSet = null;

        try {

            Class.forName(JDBC_DRIVER);

            conn = DriverManager.getConnection(DB_URL, USERNAME, PASSWORD);
            stmt = conn.createStatement();

            resultSet = stmt.executeQuery(sqlQuery);

        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            try {
                if (stmt != null) {
                    conn.close();
                }
            } catch (SQLException se) {
            }
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException se) {
                se.printStackTrace();
            }
        }

        return resultSet;
    }



    public void insertData()
    {

        Faker faker = new Faker();

        String name = faker.name().fullName(); // Miss Samanta Schmidt
        String streetAddress = faker.address().streetAddress(); // 60018 Sawayn Brooks Suite 449

        String sqlInsert = "INSERT INTO varasto.asiakas (nimi, osoite) VALUES (" + name + "," + streetAddress + ")";

        executeSQLInsert(sqlInsert);

        ResultSet resultSet = executeSQLQuery("SELECT id FROM varasto.asiakas WHERE nimi=" + name + " AND osoite=" + streetAddress + ";");

        int asiakasId = 0;

        try {

            asiakasId = resultSet.getInt("id");


        } catch (Exception e) {
            e.printStackTrace();
        }

        Date dueDate = (Date) faker.date().past(360, TimeUnit.DAYS);

        DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");

        String dueDateAsString = dateFormat.format(dueDate);

        sqlInsert = "INSERT INTO varasto.lasku (asiakasId, tila, erapaiva, ykkososa, edellinenlasku) VALUES (" + asiakasId + ",1,STR_TO_DATE('" + dueDateAsString + "','%d-%m-%Y'),1,0)";

        executeSQLInsert(sqlInsert);

        resultSet = executeSQLQuery("SELECT id FROM varasto.lasku WHERE asiakasId=" + asiakasId + " AND erapaiva=" + streetAddress + ";");

        int laskuId = 0;

        try {

            laskuId = resultSet.getInt("id");


        } catch (Exception e) {
            e.printStackTrace();
        }


        name = faker.name().fullName();
        streetAddress = faker.address().streetAddress();

        sqlInsert = "INSERT INTO varasto.tyokohde (nimi, osoite, asiakasid) VALUES (" + name + ",1,STR_TO_DATE('" + dateFormat.format(dueDate) + "','%d-%m-%Y'),1,0)";

        executeSQLInsert(sqlInsert);

        resultSet = executeSQLQuery("SELECT id FROM varasto.tyokohde WHERE asiakasId=" + asiakasId + " AND erapaiva=" + streetAddress + ";");

        int tyokohdeId = 0;

        try {

            tyokohdeId = resultSet.getInt("id");

        } catch (Exception e) {
            e.printStackTrace();
        }

        int hinta = faker.random().nextInt(10,10000);

        sqlInsert = "INSERT INTO varasto.suoritus (tyyppi, urakkahinta, laskuId, kohdeId) VALUES (2," + hinta + "," + laskuId + "," + tyokohdeId + ")";

        executeSQLInsert(sqlInsert);

        resultSet = executeSQLQuery("SELECT id FROM varasto.suoritus WHERE laskuId=" + laskuId + " AND tyokohdeId=" + tyokohdeId + ";");

        int suoritusId = 0;

        try {

            suoritusId = resultSet.getInt("id");

        } catch (Exception e) {
            e.printStackTrace();
        }

        int pituus = faker.random().nextInt(1,100);

        int varastoSaldo = faker.random().nextInt(10,10000);

        int sisaanOstoHinta = faker.random().nextInt(1,100);

        sqlInsert = "INSERT INTO varasto.varasto (nimi, varastosaldo, yksikko, sisaanostohinta, alv, poistettu) VALUES('" + pituus + " m KAAPELI', " + varastoSaldo + ", 'm'," + sisaanOstoHinta + ", 24, false)";

        executeSQLInsert(sqlInsert);

        sqlInsert = "INSERT INTO varasto.tarvike (lukumaara, alennus, suoritusId, varastotarvikeId) VALUES(1, 0," + suoritusId + ", 1)";

        executeSQLInsert(sqlInsert);


    }


}